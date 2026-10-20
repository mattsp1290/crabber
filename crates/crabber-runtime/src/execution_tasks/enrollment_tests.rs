use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::oneshot;

#[tokio::test]
async fn closing_retains_real_descendants_after_original_result_and_ignores_handle_clones() {
    let tasks = ExecutionTasks::default();
    let session = SessionId::new();
    let (parent_send, parent_receive) = oneshot::channel();
    let (release, held) = oneshot::channel();
    let original_tasks = tasks.clone();
    let child_session = session.clone();
    let root = tasks
        .admit(Some(session.clone()), move |scope| async move {
            let child = original_tasks.spawn_reporting_in(
                &scope,
                child_session,
                None,
                move |child| async move {
                    parent_send.send(child).ok();
                    held.await.unwrap();
                    TaskTermination::Returned
                },
            )?;
            Ok(child)
        })
        .unwrap();
    let child = root.result.await.unwrap().unwrap();
    let actual_child_scope = parent_receive.await.unwrap();
    assert_eq!(tasks.join(&root.task).await, TaskTermination::Returned);
    assert!(!tasks.begin_close());
    assert!(actual_child_scope.cancellation().is_cancelled());
    let cleanup = tasks
        .spawn_reporting_in(
            &actual_child_scope,
            session.clone(),
            None,
            |scope| async move {
                assert!(scope.cancellation().is_cancelled());
                TaskTermination::Returned
            },
        )
        .unwrap();
    release.send(()).unwrap();
    assert_eq!(tasks.join(&child).await, TaskTermination::Returned);
    assert_eq!(tasks.join(&cleanup).await, TaskTermination::Returned);
    assert_eq!(tasks.pending_enrollments(), []);
    assert!(matches!(
        tasks.spawn_reporting_in(&actual_child_scope, session, None, |_| async {
            TaskTermination::Returned
        }),
        Err(crate::RuntimeError::HostClosed)
    ));
    // The retained original scope and task Arcs above do not count as pending.
    assert_eq!(tasks.snapshot_close().pending_preparations, []);
}

#[tokio::test]
async fn fresh_reentrant_admission_refuses_and_original_self_close_is_detected() {
    let tasks = ExecutionTasks::default();
    let original = tasks.clone();
    let dispatched = Arc::new(AtomicBool::new(false));
    let body_dispatched = dispatched.clone();
    let admitted = tasks
        .admit(None, move |_scope| async move {
            assert!(original.begin_close());
            let nested = original.admit(None, move |_| async move {
                body_dispatched.store(true, Ordering::SeqCst);
                Ok(())
            });
            assert!(matches!(nested, Err(crate::RuntimeError::HostClosed)));
            Ok(())
        })
        .unwrap();
    admitted.result.await.unwrap().unwrap();
    assert_eq!(tasks.join(&admitted.task).await, TaskTermination::Returned);
    assert!(!dispatched.load(Ordering::SeqCst));
    assert_eq!(tasks.pending_enrollments(), []);
}

#[tokio::test]
async fn panic_reaped_before_close_remains_in_actual_join_failure_report() {
    let tasks = ExecutionTasks::default();
    let admitted = tasks
        .admit::<(), _, _>(None, |_| async { panic!("actual original panic") })
        .unwrap();
    assert!(admitted.result.await.is_err());
    assert_eq!(tasks.join(&admitted.task).await, TaskTermination::Panicked);
    assert_eq!(tasks.snapshot_close().pending_preparations, []);
    tasks.begin_close();
    let failures = tasks.failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].termination, TaskTermination::Panicked);
}
