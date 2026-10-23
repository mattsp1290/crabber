use super::*;

#[cfg(feature = "sqlite")]
#[path = "transaction_diagnostics.rs"]
mod transaction_diagnostics;

async fn contract(store: Arc<dyn Store>, inverted: bool) {
    for mode in 0..4 {
        case(store.clone(), inverted, mode).await;
    }
}
async fn start_session(
    runtime: &Orchestrator,
    inverted: bool,
    mode: usize,
    phase: &str,
) -> crate::RunHandle {
    let started = std::time::Instant::now();
    runtime.start(request()).await.unwrap_or_else(|error| {
        panic!(
            "{phase} admission: inverted={inverted} mode={mode} elapsed={:?} error={error:?}",
            started.elapsed()
        )
    })
}

async fn case(store: Arc<dyn Store>, inverted: bool, mode: usize) {
    let seen = Seen::new(mode);
    let release = Release(seen.gate.clone());
    let (runtime, plans, host) = build(store.clone(), inverted, &seen);
    let a = start_session(&runtime, inverted, mode, "first").await;
    let first = a.session_id().clone();
    let arun = a.run_id().clone();
    tokio::time::timeout(BOUND, seen.gate.entered.notified())
        .await
        .unwrap();
    if mode >= 2 {
        a.interrupt();
        let held = runtime
            .drain_execution_tasks(Duration::from_millis(20))
            .await;
        assert!(held.pending.contains(&arun));
        let attempt = store
            .latest_execution_attempt(&arun)
            .await
            .unwrap()
            .unwrap()
            .attempt;
        assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
        assert_eq!(seen.effects.load(Ordering::SeqCst), 0);
        assert!(seen.dropped.iter().all(|d| d.load(Ordering::SeqCst) == 0));
    }
    let b = start_session(&runtime, inverted, mode, "second (first held)").await;
    let second = b.session_id().clone();
    let brun = b.run_id().clone();
    assert_eq!(
        tokio::time::timeout(BOUND, b.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
    drop(release);
    let aresult = tokio::time::timeout(BOUND, a.done()).await.unwrap();
    match mode {
        0 | 1 => assert_eq!(aresult.unwrap().status, RunStatus::Completed),
        2 => assert_eq!(aresult.unwrap().status, RunStatus::Interrupted),
        _ => assert!(aresult.is_err()),
    }
    let drain = runtime.drain_execution_tasks(BOUND).await;
    assert_eq!(drain.pending, [] as [RunId; 0]);
    let acontext = SessionContext::new(first.clone(), "test", "/tmp");
    let bcontext = SessionContext::new(second.clone(), "test", "/tmp");
    let _aclose = plans.close_session(&acontext, BOUND).await;
    // Session close never claims Workspace object destruction.
    for kind in [Kind::Tool, Kind::Provider, Kind::Fs, Kind::Guard] {
        let workspace_kind = matches!(kind, Kind::Tool | Kind::Guard) == inverted;
        if workspace_kind {
            assert_eq!(seen.dropped[kind as usize].load(Ordering::SeqCst), 0);
        }
    }
    plans.close_session(&bcontext, BOUND).await.unwrap();
    let closed = plans.close_workspace(&acontext, BOUND).await;
    assert_eq!(closed.is_err(), mode == 3);
    let _all = plans.close_all(BOUND).await;
    assert_eq!(
        seen.effects.load(Ordering::SeqCst),
        if mode == 0 { 2 } else { 1 }
    );
    assert_eq!(
        seen.reads.load(Ordering::SeqCst),
        if mode == 0 { 2 } else { 1 }
    );
    assert!(seen.binds.load(Ordering::SeqCst) >= 2);
    assert!(seen.streams.load(Ordering::SeqCst) >= 3);
    assert_eq!(
        seen.streams.load(Ordering::SeqCst),
        seen.stream_drops.load(Ordering::SeqCst)
    );
    assert_eq!(
        seen.binds.load(Ordering::SeqCst),
        seen.streamer_drops.load(Ordering::SeqCst)
    );
    for kind in [Kind::Tool, Kind::Provider, Kind::Fs, Kind::Guard] {
        let expected = if matches!(kind, Kind::Tool | Kind::Guard) == inverted {
            1
        } else {
            2
        };
        assert_eq!(seen.made[kind as usize].load(Ordering::SeqCst), expected);
        assert_eq!(seen.dropped[kind as usize].load(Ordering::SeqCst), expected);
    }
    let observed = seen.observed.lock().unwrap().clone();
    assert_eq!(observed.len(), 2);
    assert!(observed.iter().any(|(s, r, _)| s == &first && r == &arun));
    assert!(observed.iter().any(|(s, r, _)| s == &second && r == &brun));
    // Each original per-scope graph joins or retains the genuine panic failure.
    let graphs = host.0.lock().unwrap().clone();
    for graph in graphs {
        let result = graph.registered_graph_join(BOUND).await;
        if mode != 3 {
            result.unwrap();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_mixed_products_both_scope_orders() {
    for inverted in [false, true] {
        contract(Arc::new(MemoryStore::new()), inverted).await;
    }
}
#[cfg(feature = "sqlite")]
#[test]
fn sqlite_mixed_products_both_scope_orders() {
    transaction_diagnostics::run(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed.sqlite");
        crabber_session::SqliteStore::migrate(&path).await.unwrap();
        for inverted in [false, true] {
            contract(
                Arc::new(crabber_session::SqliteStore::connect(&path).await.unwrap()),
                inverted,
            )
            .await;
        }
    });
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_mixed_products_both_scope_orders() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        for inverted in [false, true] {
            contract(
                Arc::new(crabber_session::PostgresStore::connect(&url).await.unwrap()),
                inverted,
            )
            .await;
        }
    })
    .await
    .unwrap();
}

// The original panic-payload destructor stays blocked throughout this check.
// A pending control observation must make progress on its independently owned
// task; another worker/connection alone cannot poll a sibling future for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_control_progresses_while_original_guard_panic_payload_is_held() {
    use crate::tests::{
        DelayedTerminalStore, SettlementGate, suspension_observation::ObservationGate,
    };
    struct ReleaseObserver(Arc<ObservationGate>);
    impl Drop for ReleaseObserver {
        fn drop(&mut self) {
            self.0.release_observer.notify_one();
        }
    }
    let observation = Arc::new(ObservationGate::default());
    let release_observer = ReleaseObserver(observation.clone());
    let store = Arc::new(DelayedTerminalStore {
        inner: Arc::new(MemoryStore::new()),
        gate: Arc::new(SettlementGate {
            observation: Some(observation.clone()),
            ..Default::default()
        }),
    });
    let seen = Seen::new(3);
    let release_payload = Release(seen.gate.clone());
    let (runtime, plans, _) = build(store, false, &seen);
    let first = runtime.start(request()).await.unwrap();
    tokio::time::timeout(BOUND, observation.observer_entered.notified())
        .await
        .expect("original control observation entered");
    tokio::time::timeout(BOUND, seen.gate.entered.notified())
        .await
        .expect("original guard panic payload is held");
    drop(release_observer);
    tokio::time::timeout(BOUND, observation.observer_returned.notified())
        .await
        .expect("original control observation must return while panic payload remains held");
    assert!(!*seen.gate.open.lock().unwrap());
    let second = tokio::time::timeout(BOUND, runtime.start(request()))
        .await
        .expect("independent admission must progress while original payload remains held")
        .unwrap();
    assert_eq!(
        tokio::time::timeout(BOUND, second.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
    assert!(!*seen.gate.open.lock().unwrap());
    drop(release_payload);
    assert!(
        tokio::time::timeout(BOUND, first.done())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(
        runtime.drain_execution_tasks(BOUND).await.pending,
        [] as [RunId; 0]
    );
    let _closed = plans.close_all(BOUND).await;
}
