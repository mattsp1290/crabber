use super::*;
use crate::{CallbackInvocationOwner, ToolResourceOwner, with_callback_invocations};
use crabber_core::SessionId;
use futures::StreamExt;
fn selection() -> Selection {
    Selection {
        provider_id: "value".into(),
        model_id: "text".into(),
    }
}
fn source(text: &str) -> LibraryScriptedSource {
    LibraryScriptedSource::new(selection(), vec![text.into()]).unwrap()
}
fn session(id: &str) -> SessionContext {
    SessionContext::new(SessionId(id.into()), "workspace", "directory")
}
async fn drain(resources: &ToolResourceOwner) {
    let report = resources.drain(Duration::from_secs(5)).await;
    assert!(report.sealed);
    assert_eq!(report.pending, 0);
    assert_eq!(report.failures, [] as [crate::ResourceFailure; 0]);
}

#[tokio::test]
async fn missing_owner_refuses_pristine_activation() {
    assert!(
        source("unused")
            .activate(session("no-owner"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn routing_errors_preserve_recipe_and_semantic_exhaustion() {
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        let active = source("original").activate(session("route")).await.unwrap();
        let mut wrong = selection();
        wrong.model_id = "other".into();
        let mismatch = active.stream(wrong).await;
        let mut stream = active.stream(selection()).await.unwrap();
        let text = stream.next().await;
        let completed = stream.next().await;
        drop(stream);
        let exhausted = active.stream(selection()).await;
        active
            .observation()
            .close(Duration::from_secs(5))
            .await
            .unwrap();
        assert!(matches!(
            mismatch,
            Err(LibraryScriptedError::Script(ScriptedError::RouteMismatch))
        ));
        assert!(matches!(text, Some(StreamDelta::TextDelta(value)) if value == "original"));
        assert!(matches!(completed, Some(StreamDelta::Completed)));
        assert!(matches!(
            exhausted,
            Err(LibraryScriptedError::Script(ScriptedError::Exhausted))
        ));
    })
    .await;
    drop(owner);
    drain(&resources).await;
}

#[tokio::test]
async fn held_stream_reference_blocks_same_original_close_and_sibling_survives() {
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        let first = source("A")
            .activate(session("shared-session"))
            .await
            .unwrap();
        let second = source("B")
            .activate(session("shared-session"))
            .await
            .unwrap();
        let mut held = first.stream(selection()).await.unwrap();
        let observation = first.observation();
        let incomplete = observation.close(Duration::from_millis(20)).await;
        let late = first.stream(selection()).await;
        let closed = held.next().await;
        let fused = held.next().await;
        drop(held);
        observation.close(Duration::from_secs(5)).await.unwrap();
        let mut other = second.stream(selection()).await.unwrap();
        let text = other.next().await;
        drop(other);
        second
            .observation()
            .close(Duration::from_secs(5))
            .await
            .unwrap();
        assert!(incomplete.is_err());
        assert!(late.is_err());
        assert!(matches!(closed, Some(StreamDelta::Error(_))));
        assert!(fused.is_none());
        assert!(matches!(text, Some(StreamDelta::TextDelta(value)) if value == "B"));
    })
    .await;
    drop(owner);
    drain(&resources).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_drop_outside_runtime_starts_same_retained_close() {
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let active = with_callback_invocations(
        owner.capability(),
        source("never requested").activate(session("outside")),
    )
    .await
    .unwrap();
    let observation = active.observation();
    std::thread::spawn(move || drop(active)).join().unwrap();
    observation.close(Duration::from_secs(5)).await.unwrap();
    drop(owner);
    drain(&resources).await;
}

#[tokio::test]
async fn actual_caller_close_disposes_source_while_active_handle_survives() {
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let cap = owner.capability();
    let active = with_callback_invocations(
        cap.clone(),
        source("not consumed").activate(session("caller")),
    )
    .await
    .unwrap();
    drop(owner);
    drain(&resources).await;
    let result = with_callback_invocations(cap, active.stream(selection())).await;
    active
        .observation()
        .close(Duration::from_secs(5))
        .await
        .unwrap();
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_ready_future_disposal_retains_both_original_joins() {
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let cap = owner.capability();
    let active =
        with_callback_invocations(cap.clone(), source("ready").activate(session("ready-drop")))
            .await
            .unwrap();
    let observation = active.observation();
    let (creation, entered) = probe::Gate::new(true);
    let watchdog = creation.watchdog();
    let (stream_drop, stream_seen) = probe::Gate::new(false);
    *active.state.probe.lock().unwrap() = Some(Arc::new(probe::Probe {
        creation: Some(creation.clone()),
        stream: stream_drop,
        stream_panics: false,
        source: Arc::downgrade(&active.state),
        frames: Mutex::new(Vec::new()),
        initializer: Mutex::new(None),
    }));
    let request = tokio::spawn(with_callback_invocations(cap, async move {
        active.stream(selection()).await
    }));
    let entered = entered.recv_timeout(Duration::from_secs(5));
    let mut close = Box::pin(observation.close(Duration::from_secs(5)));
    let source_pending = futures::poll!(&mut close).is_pending();
    // The returned stream was installed before its original creation future's
    // Drop blocked, so real child disposal can run concurrently and unblock it.
    let disposed = stream_seen.recv_timeout(Duration::from_secs(5));
    drop(owner);
    let mut parent = Box::pin(resources.drain(Duration::from_secs(5)));
    let caller_pending = futures::poll!(&mut parent).is_pending();
    creation.release();
    let output = request.await.unwrap();
    drop(output);
    close.await.unwrap();
    let report = parent.await;
    let watchdog_ok = watchdog.finish();
    assert!(entered.is_ok());
    assert!(disposed.is_ok());
    assert!(source_pending && caller_pending);
    assert!(watchdog_ok);
    assert_eq!(report.pending, 0);
    assert_eq!(report.failures, [] as [crate::ResourceFailure; 0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_stream_disposal_retains_source_and_caller_until_same_join() {
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let cap = owner.capability();
    let active = with_callback_invocations(
        cap.clone(),
        source("stream").activate(session("stream-drop")),
    )
    .await
    .unwrap();
    let observation = active.observation();
    let (stream_drop, entered) = probe::Gate::new(true);
    let watchdog = stream_drop.watchdog();
    *active.state.probe.lock().unwrap() = Some(Arc::new(probe::Probe {
        creation: None,
        stream: stream_drop.clone(),
        stream_panics: false,
        source: Arc::downgrade(&active.state),
        frames: Mutex::new(Vec::new()),
        initializer: Mutex::new(None),
    }));
    let stream = with_callback_invocations(cap, active.stream(selection()))
        .await
        .unwrap();
    drop(stream);
    let entered = entered.recv_timeout(Duration::from_secs(5));
    let mut close = Box::pin(observation.close(Duration::from_secs(5)));
    let source_pending = futures::poll!(&mut close).is_pending();
    drop(owner);
    let mut parent = Box::pin(resources.drain(Duration::from_secs(5)));
    let caller_pending = futures::poll!(&mut parent).is_pending();
    stream_drop.release();
    close.await.unwrap();
    let report = parent.await;
    let watchdog_ok = watchdog.finish();
    assert!(entered.is_ok());
    assert!(source_pending && caller_pending);
    assert!(watchdog_ok);
    assert_eq!(report.pending, 0);
    assert_eq!(report.failures, [] as [crate::ResourceFailure; 0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_stream_disposal_panic_keeps_observed_failure_sticky() {
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let cap = owner.capability();
    let active =
        with_callback_invocations(cap.clone(), source("panic").activate(session("panic-drop")))
            .await
            .unwrap();
    let observation = active.observation();
    let (stream_drop, entered) = probe::Gate::new(false);
    let probe = Arc::new(probe::Probe {
        creation: None,
        stream: stream_drop,
        stream_panics: true,
        source: Arc::downgrade(&active.state),
        frames: Mutex::new(Vec::new()),
        initializer: Mutex::new(None),
    });
    *active.state.probe.lock().unwrap() = Some(probe.clone());
    let stream = with_callback_invocations(cap, active.stream(selection()))
        .await
        .unwrap();
    let original_child = active.state.streams.lock().unwrap()[0].clone();
    drop(stream);
    let entered = entered.recv_timeout(Duration::from_secs(5));
    let failed = original_child.join().await;
    let repeated = original_child.join().await;
    let close = observation.close(Duration::from_millis(20)).await;
    drop(owner);
    let report = resources.drain(Duration::from_millis(20)).await;
    assert!(entered.is_ok());
    assert!(failed.is_err() && repeated.is_err());
    assert!(close.is_err());
    assert_ne!(report.failures, [] as [crate::ResourceFailure; 0]);
    assert_eq!(*probe.frames.lock().unwrap(), [(true, true)]);
    // Failed original disposal deliberately retains the uncertain mount member;
    // neither this timeout nor a finished child creates a successful close.
    assert!(report.pending > 0);
}

#[tokio::test]
async fn caller_close_before_installation_return_retains_later_original_mount() {
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let cap = owner.capability();
    let installation = probe::Installation::new();
    let mut pristine = source("unentered");
    pristine.installation = Some(installation.clone());
    let work = tokio::spawn(with_callback_invocations(
        cap,
        pristine.activate(session("install")),
    ));
    let entered =
        tokio::time::timeout(Duration::from_secs(5), installation.entered.notified()).await;
    let original = installation
        .state
        .lock()
        .unwrap()
        .as_ref()
        .and_then(std::sync::Weak::upgrade);
    drop(owner);
    let mut parent = Box::pin(resources.drain(Duration::from_secs(5)));
    let pending = futures::poll!(&mut parent).is_pending();
    installation.release.notify_one();
    let result = work.await.unwrap();
    drop(result);
    let report = parent.await;
    assert!(entered.is_ok());
    assert!(pending);
    assert_eq!(report.pending, 0);
    assert_eq!(report.failures, [] as [crate::ResourceFailure; 0]);
    let original = original.unwrap();
    original.join().await.unwrap();
    assert!(original.mount.lock().unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_contextual_initializer_survives_scripted_request_and_stream_drop() {
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let outer = Owner::Workspace("outer-original".into());
    let frame = INITIALIZING
        .scope(
            outer.clone(),
            with_callback_invocations(owner.capability(), async {
                let active = source("context")
                    .activate(session("initializer"))
                    .await
                    .unwrap();
                let (stream_drop, entered) = probe::Gate::new(false);
                let probe = Arc::new(probe::Probe {
                    creation: None,
                    stream: stream_drop,
                    stream_panics: false,
                    source: Arc::downgrade(&active.state),
                    frames: Mutex::new(Vec::new()),
                    initializer: Mutex::new(None),
                });
                *active.state.probe.lock().unwrap() = Some(probe.clone());
                let stream = active.stream(selection()).await.unwrap();
                drop(stream);
                let entered = entered.recv_timeout(Duration::from_secs(5));
                active
                    .observation()
                    .close(Duration::from_secs(5))
                    .await
                    .unwrap();
                assert!(entered.is_ok());
                probe.initializer.lock().unwrap().clone()
            }),
        )
        .await;
    drop(owner);
    drain(&resources).await;
    assert_eq!(frame, Some(outer));
}

#[tokio::test]
async fn sequential_semantic_refusals_reap_only_real_joins_beyond_active_capacity() {
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        let active = source("still original")
            .activate(session("sequential"))
            .await
            .unwrap();
        let mut other = selection();
        other.model_id = "wrong".into();
        for _ in 0..65 {
            assert!(matches!(
                active.stream(other.clone()).await,
                Err(LibraryScriptedError::Script(ScriptedError::RouteMismatch))
            ));
        }
        let mut stream = active.stream(selection()).await.unwrap();
        let text = stream.next().await;
        drop(stream);
        active
            .observation()
            .close(Duration::from_secs(5))
            .await
            .unwrap();
        assert!(matches!(text, Some(StreamDelta::TextDelta(value)) if value == "still original"));
    })
    .await;
    drop(owner);
    drain(&resources).await;
}

struct NestedResult {
    closed: Result<(), ExtensionError>,
    observation: LibraryScriptedSourceObservation,
}
struct NestedSource {
    result: Arc<Mutex<Option<NestedResult>>>,
}
#[async_trait::async_trait]
impl crate::Extension for NestedSource {
    fn id(&self) -> &'static str {
        "scripted-parent"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut crate::Registrar) -> Result<(), ExtensionError> {
        use crate::Point;
        let result = self.result.clone();
        registrar.on_hook(
            crate::TurnPrepare::ID,
            0,
            "nested-source",
            Arc::new(move |_| {
                let result = result.clone();
                Box::pin(async move {
                    let active = source("inner").activate(session("nested")).await?;
                    let observation = active.observation();
                    let closed = observation.close(Duration::from_secs(5)).await;
                    *result.lock().unwrap() = Some(NestedResult {
                        closed,
                        observation,
                    });
                    Ok(serde_json::Value::Null)
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test]
async fn unrelated_outer_mount_with_same_local_id_may_close_scripted_source() {
    let registry = crate::Registry::new();
    let result = Arc::new(Mutex::new(None));
    let outer = registry
        .mount_owned(
            Arc::new(NestedSource {
                result: result.clone(),
            }),
            crate::Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId("nested".into()));
    let identical_registry = crate::Registry::new();
    let identical = identical_registry
        .mount_owned(
            Arc::new(NestedSource {
                result: result.clone(),
            }),
            crate::Scope::Global,
        )
        .await
        .unwrap();
    let identical_plan = identical_registry.acquire(&SessionId("nested".into()));
    let same_fingerprint = plan.fingerprint == identical_plan.fingerprint;
    drop(identical_plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let hook = with_callback_invocations(
        owner.capability(),
        plan.dispatcher
            .hook::<crate::TurnPrepare>(serde_json::Value::Null),
    )
    .await;
    drop(plan);
    let NestedResult {
        closed: inside,
        observation,
    } = result.lock().unwrap().take().unwrap();
    let same_local_id = outer.identity()
        == observation
            .state
            .mount
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .identity();
    observation.close(Duration::from_secs(5)).await.unwrap();
    drop(owner);
    drain(&resources).await;
    outer.close(Duration::from_secs(5)).await.unwrap();
    identical.close(Duration::from_secs(5)).await.unwrap();
    assert!(hook.is_ok());
    assert!(
        same_fingerprint,
        "private lifetime frames do not change semantic fingerprints"
    );
    assert!(same_local_id, "semantic registration IDs remain local");
    assert_eq!(inside, Ok(()));
}
