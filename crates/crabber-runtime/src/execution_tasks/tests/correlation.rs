use super::*;

#[tokio::test]
async fn original_supervisor_binds_run_while_drain_already_awaits_its_join() {
    let tasks = ExecutionTasks::default();
    let session = SessionId::new();
    let run = RunId::new();
    let (setup, after_admission) = oneshot::channel::<Arc<ExecutionTask>>();
    let (bound, observed) = oneshot::channel();
    let (release, finish) = oneshot::channel();
    let own_tasks = tasks.clone();
    let own_session = session.clone();
    let own_run = run.clone();
    let task = tasks.spawn_reporting(session.clone(), None, async move {
        let original = after_admission.await.unwrap();
        let result = own_tasks.correlate_run(&original, &own_session, &own_run);
        bound.send(result).unwrap();
        finish.await.unwrap();
        TaskTermination::Returned
    });
    let identity = task.local_id.clone();
    assert_eq!(tasks.pending(None).1.len(), 1);
    let mut drain = Box::pin(tasks.drain_session(&session, Duration::from_secs(5)));
    assert!(futures::poll!(drain.as_mut()).is_pending());
    // A drain owns the async join mutex while the actual supervisor binds its
    // newly admitted run. Waiting for that mutex here would deadlock the task.
    assert!(task.join.try_lock().is_err());
    setup
        .send(task.clone())
        .unwrap_or_else(|_| panic!("setup receiver lost"));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), observed)
            .await
            .unwrap()
            .unwrap(),
        Ok(())
    );
    assert_eq!(task.local_id, identity);
    let pending = tasks.pending(None);
    assert_eq!(pending.0, vec![run.clone()]);
    assert_eq!(pending.1, [] as [PreparationTaskId; 0]);
    release.send(()).unwrap();
    let report = drain.await;
    assert_eq!(report.joined.len(), 1);
    assert_eq!(report.joined[0].run_id, run);
    assert_eq!(report.joined[0].session_id, session);
    assert_eq!(report.joined[0].termination, TaskTermination::Returned);
    assert_eq!(report.joined_preparations, [] as [JoinedPreparationTask; 0]);
    assert_eq!(report.pending, [] as [RunId; 0]);
    assert_eq!(report.pending_preparations, [] as [PreparationTaskId; 0]);
}

#[tokio::test]
async fn correlation_refuses_wrong_owner_rebinding_and_observed_join() {
    let tasks = ExecutionTasks::default();
    let session = SessionId::new();
    let run = RunId::new();
    let (release, finish) = oneshot::channel();
    let task = tasks.spawn_preparation(session.clone(), async move {
        finish.await.unwrap();
    });
    let identity = task.local_id.clone();
    assert_eq!(tasks.correlate_run(&task, &SessionId::new(), &run), Err(()));
    assert_eq!(
        ExecutionTasks::default().correlate_run(&task, &session, &run),
        Err(())
    );
    assert_eq!(tasks.correlate_run(&task, &session, &run), Ok(()));
    assert_eq!(tasks.correlate_run(&task, &session, &run), Ok(()));
    assert_eq!(tasks.correlate_run(&task, &session, &RunId::new()), Err(()));
    assert_eq!(task.local_id, identity);
    release.send(()).unwrap();
    // Observe without removing the original registration, distinguishing closed
    // metadata from a missing owner. Neither can reopen a joined supervisor.
    assert_eq!(task.join().await, TaskTermination::Returned);
    assert_eq!(tasks.correlate_run(&task, &session, &run), Err(()));
    assert_eq!(tasks.join(&task).await, TaskTermination::Returned);
    assert_eq!(tasks.correlate_run(&task, &session, &run), Err(()));
}
