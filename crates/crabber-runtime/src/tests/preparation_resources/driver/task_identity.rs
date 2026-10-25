//! Actual task IDs diagnose handoff; they never grant execution authority.
use super::*;

async fn contract(store: Arc<dyn Store>, backend: &str) {
    let mut observations = Vec::new();
    for unkeyed in [false, true] {
        let gate = Arc::new(SettlementGate::default());
        gate.release.notify_one();
        gate.initial_task_diagnostics.store(true, Ordering::SeqCst);
        let wrapped = Arc::new(DelayedTerminalStore {
            inner: store.clone(),
            gate: gate.clone(),
        });
        let h = Harness::on_store(wrapped, store.clone(), gate);
        let handle = start_initial(&h, unkeyed).await;
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();
        // Release real cleanup BEFORE recording any diagnostic assertion.
        // Even the expected identity failure must not strand actual work.
        h.provider.cleanup_release.notify_one();
        let done = handle.done().await;
        let drained = h
            .runtime
            .drain_session_execution_tasks(&session, Duration::from_secs(3))
            .await;
        let cleaned = tokio::time::timeout(Duration::from_secs(3), async {
            while h.provider.cleanup_completed.load(Ordering::SeqCst) != 1
                || !h.runtime.retained_preparation_status(&session).is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let ids = h.gate.initial_task_ids.lock().unwrap().clone();
        eprintln!(
            "initial task diagnostics backend={backend} unkeyed={unkeyed} run={run:?} ids={ids:?}"
        );
        observations.push((unkeyed, ids, done, drained, cleaned));
    }
    for (unkeyed, ids, done, drained, cleaned) in observations {
        assert!(
            done.is_ok(),
            "backend={backend} unkeyed={unkeyed} result={done:?}"
        );
        assert!(
            cleaned.is_ok(),
            "backend={backend} unkeyed={unkeyed} cleanup={cleaned:?}"
        );
        assert!(
            drained.pending.is_empty() && drained.pending_preparations.is_empty(),
            "{drained:?}"
        );
        assert_eq!(
            ids.len(),
            3,
            "backend={backend} unkeyed={unkeyed} ids={ids:?}"
        );
        assert_eq!(
            ids.iter().map(|(stage, _)| *stage).collect::<Vec<_>>(),
            vec![
                InitialTaskStage::CompletePreparation,
                InitialTaskStage::RegisterDriver,
                InitialTaskStage::BeginAttempt
            ]
        );
        assert!(
            ids[0].1.is_some(),
            "actual retained main must be a Tokio task: {ids:?}"
        );
        assert!(
            ids.iter().all(|(_, task)| *task == ids[0].1),
            "initial Complete, Register, and Begin must execute in the SAME actual task: backend={backend} unkeyed={unkeyed} ids={ids:?}"
        );
    }
}

#[tokio::test]
async fn memory_initial_completion_register_begin_share_actual_task() {
    contract(Arc::new(MemoryStore::new()), "memory").await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_initial_completion_register_begin_share_actual_task() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("initial-task.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let store = crabber_session::SqliteStore::connect(&path).await.unwrap();
    contract(Arc::new(store), "sqlite").await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_initial_completion_register_begin_share_actual_task() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        let store = crabber_session::PostgresStore::connect(&url).await.unwrap();
        contract(Arc::new(store), "postgres").await;
    })
    .await
    .unwrap();
}
