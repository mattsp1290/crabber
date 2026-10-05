use super::*;
use crate::RuntimeError;
use crabber_extension::{
    CleanupTracker, MAX_PROMPT_CONTRIBUTION_BYTES, MAX_PROMPT_CONTRIBUTIONS_TOTAL_BYTES,
    PROMPT_CONTRIBUTION_DEADLINE,
};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn interrupt_during_contribution_settles_interrupted() {
    let registry = Registry::new();
    let entered = Arc::new(Notify::new());
    let captured = Arc::new(Mutex::new(None));
    mount(&registry, "cancel", Scope::Global, {
        let entered = entered.clone();
        let captured = captured.clone();
        move |r| {
            let entered = entered.clone();
            let captured = captured.clone();
            r.prompt_contributor(
                0,
                "wait",
                Arc::new(move |context| {
                    *captured.lock().unwrap() = Some(context.cancellation().clone());
                    entered.notify_one();
                    Box::pin(async move {
                        context.cancellation().cancelled().await;
                        Ok(None)
                    })
                }),
            );
        }
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![text_script("unused")]);
    let handle = runtime(store.clone(), &fake, registry)
        .start(request())
        .await
        .unwrap();
    let run_id = handle.run_id().clone();
    entered.notified().await;
    handle.interrupt();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Interrupted);
    assert!(captured.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().error,
        Some(RuntimeError::Interrupted.to_string())
    );
    assert_eq!(fake.requests().len(), 0);
}
#[tokio::test]
async fn oversize_contribution_fails_before_provider() {
    for (count, size, last, succeeds) in [
        (1, MAX_PROMPT_CONTRIBUTION_BYTES, 0, true),
        (1, MAX_PROMPT_CONTRIBUTION_BYTES + 1, 0, false),
        (
            MAX_PROMPT_CONTRIBUTIONS_TOTAL_BYTES / MAX_PROMPT_CONTRIBUTION_BYTES,
            MAX_PROMPT_CONTRIBUTION_BYTES,
            0,
            true,
        ),
        (4, MAX_PROMPT_CONTRIBUTION_BYTES, 1, false),
    ] {
        let registry = Registry::new();
        mount(&registry, "bytes", Scope::Global, move |r| {
            for i in 0..count {
                r.prompt_contributor(0, i.to_string(), constant(&"x".repeat(size)));
            }
            if last > 0 {
                r.prompt_contributor(1, "last", constant("x"));
            }
        })
        .await;
        let store = Arc::new(MemoryStore::new());
        let fake = FakeProvider::scripted(vec![text_script("done")]);
        let handle = runtime(store.clone(), &fake, registry)
            .start(request())
            .await
            .unwrap();
        let id = handle.run_id().clone();
        let result = handle.done().await;
        if succeeds {
            assert_eq!(result.unwrap().status, RunStatus::Completed);
        } else {
            assert!(matches!(result, Err(RuntimeError::Extension(_))));
        }
        assert_eq!(
            store.get_run(&id).await.unwrap().unwrap().status,
            if succeeds {
                RunStatus::Completed
            } else {
                RunStatus::Failed
            }
        );
        assert_eq!(fake.requests().len(), usize::from(succeeds));
        if !succeeds {
            assert_eq!(
                store.get_run(&id).await.unwrap().unwrap().error,
                Some(format!(
                    "extension: prompt contribution failed: {}",
                    if last > 0 { "last" } else { "0" }
                ))
            );
        }
    }
}
#[tokio::test(start_paused = true)]
async fn deadline_failure_keeps_spawned_work_tracked() {
    let registry = Registry::new();
    let entered = Arc::new(Notify::new());
    let captured: Arc<Mutex<Option<(CancellationToken, CleanupTracker)>>> =
        Arc::new(Mutex::new(None));
    let release = CancellationToken::new();
    let capacity = Arc::new(Semaphore::new(1));
    mount(&registry, "slow", Scope::Global, {
        let entered = entered.clone();
        let captured = captured.clone();
        let release = release.clone();
        let capacity = capacity.clone();
        move |r| {
            let entered = entered.clone();
            let captured = captured.clone();
            let release = release.clone();
            let capacity = capacity.clone();
            r.prompt_contributor(
                0,
                "slow",
                Arc::new(move |context| {
                    let release = release.clone();
                    let capacity = capacity.clone();
                    let entered = entered.clone();
                    let captured = captured.clone();
                    Box::pin(async move {
                        let permit = capacity.acquire_owned().await.unwrap();
                        *captured.lock().unwrap() =
                            Some((context.cancellation().clone(), context.cleanup().clone()));
                        context.cleanup().spawn(async move {
                            release.cancelled().await;
                            drop(permit);
                        });
                        entered.notify_one();
                        futures::future::pending().await
                    })
                }),
            );
        }
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![text_script("unused")]);
    let handle = runtime(store.clone(), &fake, registry)
        .start(request())
        .await
        .unwrap();
    entered.notified().await;
    tokio::time::advance(PROMPT_CONTRIBUTION_DEADLINE).await;
    let id = handle.run_id().clone();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::Extension(_))
    ));
    assert_eq!(
        store.get_run(&id).await.unwrap().unwrap().status,
        RunStatus::Failed
    );
    let (token, tracker) = captured.lock().unwrap().clone().unwrap();
    assert!(token.is_cancelled());
    assert_eq!(tracker.pending(), 1);
    assert_eq!(capacity.available_permits(), 0);
    assert_eq!(fake.requests().len(), 0);
    release.cancel();
    tokio::task::yield_now().await;
    assert_eq!(tracker.pending(), 0);
    assert_eq!(capacity.available_permits(), 1);
}
#[tokio::test]
async fn callback_error_and_panic_are_sanitized() {
    for panic in [false, true] {
        let registry = Registry::new();
        mount(&registry, "bad", Scope::Global, move |r| {
            r.prompt_contributor(
                0,
                "bad",
                Arc::new(move |_| {
                    Box::pin(async move {
                        assert!(!panic, "secret-token");
                        Err(ExtensionError::Tool("secret-token".into()))
                    })
                }),
            );
        })
        .await;
        let store = Arc::new(MemoryStore::new());
        let fake = FakeProvider::scripted(vec![text_script("unused")]);
        let handle = runtime(store.clone(), &fake, registry)
            .start(request())
            .await
            .unwrap();
        let id = handle.run_id().clone();
        let session = handle.session_id().clone();
        assert!(matches!(
            handle.done().await,
            Err(RuntimeError::Extension(_))
        ));
        assert_eq!(
            store.get_run(&id).await.unwrap().unwrap().status,
            RunStatus::Failed
        );
        let run = store.get_run(&id).await.unwrap().unwrap();
        assert_eq!(
            run.error.as_deref(),
            Some("extension: prompt contribution failed: bad")
        );
        assert!(
            !serde_json::to_string(&run)
                .unwrap()
                .contains("secret-token")
        );
        assert!(
            !serde_json::to_string(&store.list_messages(&session, None).await.unwrap())
                .unwrap()
                .contains("secret-token")
        );
        let events = store.list_events(&session, None, 100).await.unwrap();
        assert!(
            !serde_json::to_string(&events)
                .unwrap()
                .contains("secret-token")
        );
        assert!(
            events
                .iter()
                .all(|event| event.kind != crabber_core::EventKind::MessageStarted)
        );
        assert_eq!(fake.requests().len(), 0);
    }
}
#[tokio::test]
async fn failure_on_retry_makes_no_second_provider_call() {
    let registry = Registry::new();
    mount(&registry, "retry", Scope::Global, |r| {
        r.prompt_contributor(
            0,
            "retry",
            Arc::new(|context| {
                Box::pin(async move {
                    if context.attempt() == 1 {
                        Ok(Some("first".into()))
                    } else {
                        Err(ExtensionError::Tool("secret-token".into()))
                    }
                })
            }),
        );
    })
    .await;
    let fake = FakeProvider::scripted(vec![
        error_script(ProviderErrorKind::RateLimited),
        text_script("unused"),
    ]);
    let store = Arc::new(MemoryStore::new());
    let handle = runtime(store.clone(), &fake, registry)
        .start(request())
        .await
        .unwrap();
    let id = handle.run_id().clone();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::Extension(_))
    ));
    assert_eq!(
        store.get_run(&id).await.unwrap().unwrap().status,
        RunStatus::Failed
    );
    assert_eq!(fake.requests().len(), 1);
    assert_eq!(fake.requests()[0].system.as_deref(), Some("base\nfirst"));
}

struct PanicPayload;
impl Drop for PanicPayload {
    fn drop(&mut self) {
        panic!("secret payload destructor");
    }
}
fn panic_with_payload() -> Result<Option<String>, ExtensionError> {
    std::panic::panic_any(PanicPayload)
}
#[tokio::test]
async fn panic_payload_destructor_settles_failed_without_provider_call() {
    let registry = Registry::new();
    mount(&registry, "payload", Scope::Global, |r| {
        r.prompt_contributor(
            0,
            "payload",
            Arc::new(|_| Box::pin(async { panic_with_payload() })),
        );
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![text_script("unused")]);
    let handle = runtime(store.clone(), &fake, registry)
        .start(request())
        .await
        .unwrap();
    let id = handle.run_id().clone();
    let error = handle.done().await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "extension: prompt contribution failed: payload"
    );
    let run = store.get_run(&id).await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(
        run.error.as_deref(),
        Some("extension: prompt contribution failed: payload")
    );
    assert_eq!(fake.requests().len(), 0);
}
