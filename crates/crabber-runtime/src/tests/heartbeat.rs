//! Existing private fixture/suite, preserving actual native cuts and joins.
use super::*;

#[tokio::test]
async fn heartbeat_keeps_gated_tool_run_owned_past_initial_lease() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("finished"),
        text_script("another run"),
    ]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(GatedTool {
                entered: Mutex::new(Some(entered_tx)),
                release: Arc::clone(&release),
            }))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(25));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if store.list_unfinished_runs().await.unwrap()[0].lease_until
                > now + time::Duration::seconds(50)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    clock.set(now + time::Duration::seconds(35));
    release.notify_one();
    assert_eq!(
        handle.done().await.unwrap().status,
        crabber_core::RunStatus::Completed
    );
    let mut next = request();
    next.session_id = Some(session);
    assert_eq!(
        runtime
            .start(next)
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
            .status,
        crabber_core::RunStatus::Completed
    );
}

#[tokio::test]
async fn heartbeat_covers_model_stream_and_ends_at_durable_approval() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("done"),
    ]);
    let (model_tx, model_rx) = oneshot::channel();
    let model_release = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .model_stream(Arc::new(GatedModelStream {
            entered: Mutex::new(Some(model_tx)),
            release: Arc::clone(&model_release),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), model_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(25));
    wait_for_lease(&store, now + time::Duration::seconds(50)).await;
    model_release.notify_one();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let lease = store.get_run(&run).await.unwrap().unwrap().lease_until;
    clock.set(now + time::Duration::seconds(90));
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().lease_until,
        lease
    );
    assert_eq!(
        store
            .latest_execution_attempt(&run)
            .await
            .unwrap()
            .unwrap()
            .attempt
            .state,
        crabber_session::AttemptState::Suspended
    );
}

async fn wait_for_lease(store: &MemoryStore, threshold: time::OffsetDateTime) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if store.list_unfinished_runs().await.unwrap()[0].lease_until > threshold {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn heartbeat_interval_requires_margin_inside_lease() {
    for interval in [
        std::time::Duration::ZERO,
        std::time::Duration::from_secs(15),
        std::time::Duration::from_secs(30),
    ] {
        assert!(matches!(
            Orchestrator::builder()
                .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
                .heartbeat_interval(interval)
                .build(),
            Err(RuntimeError::InvalidConfiguration(_))
        ));
    }
}

#[tokio::test]
async fn reclaimed_lease_does_not_authorize_pending_approval() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("must not run"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::clone(&executed))))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run_id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    clock.set(now + time::Duration::seconds(31));
    let new_fence = store
        .claim_expired_run(&run_id, "replacement owner")
        .await
        .unwrap();
    assert!(matches!(
        runtime.resume(&run_id).await,
        Err(RuntimeError::DurableContinuationRequired { .. })
    ));
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 1);
    let kinds = store
        .list_events(&session, None, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(kinds.contains(&EventKind::RunPaused));
    assert!(!kinds.contains(&EventKind::PermissionDecided));
    assert!(!kinds.contains(&EventKind::ToolCallSettled));
    assert!(!kinds.contains(&EventKind::RunSettled));
    let unfinished = store.list_unfinished_runs().await.unwrap();
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].owner, "replacement owner");
    assert_eq!(unfinished[0].claim_token, new_fence.claim_token);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_settlement_wins_heartbeat_tick_during_observer_callback() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![text_script("done")]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let released = Arc::new((Mutex::new(false), Condvar::new()));
    let observer = Arc::new(BlockingSettledObserver {
        entered: Mutex::new(Some(entered_tx)),
        released: Arc::clone(&released),
    });
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .observer(observer)
        .heartbeat_interval(std::time::Duration::from_millis(5))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().status,
        crabber_core::RunStatus::Completed
    );
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    {
        let (lock, ready) = &*released;
        *lock.lock().unwrap() = true;
        ready.notify_all();
    }
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        crabber_core::RunStatus::Completed
    );
}

#[tokio::test]
async fn reclaimed_lease_cancels_pending_tool_body() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("must not run"),
    ]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(CancellableTool {
                entered: Mutex::new(Some(entered_tx)),
                release: Arc::clone(&release),
                completed: Arc::clone(&completed),
            }))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(31));
    let new_fence = store
        .claim_expired_run(&run_id, "replacement owner")
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
            .await
            .unwrap(),
        Err(RuntimeError::LeaseLost)
    ));
    release.notify_one();
    tokio::task::yield_now().await;
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 1);
    let kinds = store
        .list_events(&session, None, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(!kinds.contains(&EventKind::ToolCallSettled));
    assert!(!kinds.contains(&EventKind::RunSettled));
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().claim_token,
        new_fence.claim_token
    );
}
