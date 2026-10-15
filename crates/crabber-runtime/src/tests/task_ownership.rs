use super::*;
use std::time::Duration;

#[tokio::test]
async fn dropped_run_handle_keeps_worker_owned_until_real_join() {
    let store = Arc::new(MemoryStore::new());
    let (entered, entering) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let runtime = orchestrator(
        store.clone(),
        FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"owned"}"#),
            text_script("done"),
        ]),
        Arc::new(CancellableTool {
            entered: Mutex::new(Some(entered)),
            release: release.clone(),
            completed: completed.clone(),
        }),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(Duration::from_secs(2), entering)
        .await
        .unwrap()
        .unwrap();
    drop(handle);
    let report = runtime
        .drain_execution_tasks(Duration::from_millis(1))
        .await;
    assert_eq!(report.joined, [] as [crate::JoinedExecutionTask; 0]);
    assert_eq!(report.pending, vec![run_id.clone()]);
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().status,
        RunStatus::Running
    );
    release.notify_one();
    let report = runtime.drain_execution_tasks(Duration::from_secs(2)).await;
    assert_eq!(report.pending, [] as [RunId; 0]);
    assert_eq!(
        report.joined,
        vec![crate::JoinedExecutionTask {
            run_id: run_id.clone(),
            termination: crate::TaskTermination::Returned
        }]
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(
        runtime.drain_execution_tasks(Duration::ZERO).await,
        crate::ExecutionTaskDrain::default()
    );
}

#[tokio::test]
async fn done_consumes_actual_join_and_host_drain_can_join_before_result_is_taken() {
    for host_first in [false, true] {
        let runtime = orchestrator(
            Arc::new(MemoryStore::new()),
            FakeProvider::scripted(vec![text_script("done")]),
            Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
            Arc::new(RecordingObserver::default()),
        );
        let handle = runtime.start(request()).await.unwrap();
        if host_first {
            let report = runtime.drain_execution_tasks(Duration::from_secs(2)).await;
            assert_eq!(report.pending, [] as [RunId; 0]);
            assert_eq!(report.joined.len(), 1);
        }
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
        assert_eq!(
            runtime.drain_execution_tasks(Duration::ZERO).await.joined,
            [] as [crate::JoinedExecutionTask; 0]
        );
    }
}

#[tokio::test]
async fn abandoned_resume_waiter_leaves_claim_and_work_owned() {
    let store = Arc::new(MemoryStore::new());
    let (entered, entering) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"resume"}"#),
            text_script("done"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(CancellableTool {
                entered: Mutex::new(Some(entered)),
                release: release.clone(),
                completed: completed.clone(),
            }))],
            vec![],
        )))
        .policy(Arc::new(PausePolicy))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let waiter = tokio::spawn({
        let runtime = runtime.clone();
        let run_id = run_id.clone();
        async move { runtime.resume(&run_id).await }
    });
    tokio::time::timeout(Duration::from_secs(2), entering)
        .await
        .unwrap()
        .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let report = runtime
        .drain_execution_tasks(Duration::from_millis(1))
        .await;
    assert_eq!(report.pending, vec![run_id.clone()]);
    release.notify_one();
    let report = runtime.drain_execution_tasks(Duration::from_secs(2)).await;
    assert_eq!(report.pending, [] as [RunId; 0]);
    assert_eq!(report.joined.len(), 1);
    assert_eq!(
        report.joined[0].termination,
        crate::TaskTermination::Returned
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
}

#[tokio::test]
async fn panicked_worker_join_does_not_invent_durable_terminal_outcome() {
    struct PanickingObserver;
    impl Observer for PanickingObserver {
        fn emit(&self, _: &EventRecord) {
            panic!("observer failure")
        }
    }
    let store = Arc::new(MemoryStore::new());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![text_script(
            "unused",
        )])))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .observer(Arc::new(PanickingObserver))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    let report = runtime.drain_execution_tasks(Duration::from_secs(2)).await;
    assert_eq!(report.pending, [] as [RunId; 0]);
    assert_eq!(
        report.joined,
        vec![crate::JoinedExecutionTask {
            run_id: run_id.clone(),
            termination: crate::TaskTermination::Panicked
        }]
    );
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::TaskStopped)
    ));
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().status,
        RunStatus::Running
    );
}
