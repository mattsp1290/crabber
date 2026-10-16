//! Resource owners for the same genuine Run remain separate local lifetimes.
use super::*;

#[tokio::test]
async fn older_worker_cleanup_cannot_close_another_workers_same_run_resources() {
    let h = Harness::new();
    h.gate.driver_register_fault.store(1, Ordering::SeqCst);
    let handle = start_initial(&h, false).await;
    let run = handle.run_id().clone();
    let session = handle.session_id().clone();
    let old = handle.worker_resources_for_test().clone();
    assert!(handle.done().await.is_err());
    assert!(h.memory.get_run(&run).await.unwrap().is_some());
    let tasks = h.runtime.execution_tasks_for_test();
    let later = tasks.worker_resources(session, run);
    let old_scope = old.tool_resources(crabber_core::ToolCallId::new(), None);
    let later_scope = later.tool_resources(crabber_core::ToolCallId::new(), None);
    let later_closing = later_scope.resources().closing().unwrap();
    let later_closed = Arc::new(AtomicUsize::new(0));
    let observed = later_closed.clone();
    later_scope
        .resources()
        .spawn(async move {
            later_closing.await;
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    old.finish().await.unwrap();
    assert!(old_scope.snapshot().sealed);
    assert!(old.join(Duration::ZERO).await);
    assert!(!later_scope.snapshot().sealed);
    assert_eq!(later_closed.load(Ordering::SeqCst), 0);
    later.finish().await.unwrap();
    assert_eq!(later_closed.load(Ordering::SeqCst), 1);
    h.provider.cleanup_release.notify_one();
}
