use super::*;

#[tokio::test(start_paused = true)]
async fn wait_times_out_with_current_statuses() {
    let mut host = PendingHost::new(false);
    let runner = host.pending_after(1);
    let mut j = Journey::new(
        runner,
        Limits {
            max_in_flight: 2,
            max_background_duration: Duration::from_secs(60),
            ..limits()
        },
    );
    let a = j.start().await;
    j.wait(std::slice::from_ref(&a)).await;
    let b = j.start().await;
    host.entered(1).await;
    let before = tokio::time::Instant::now();
    let value = result_value(
        &j.call(WAIT_TOOL, json!({"task_ids":[a,b],"timeout_seconds":1}))
            .await,
    );
    assert_eq!(before.elapsed(), Duration::from_secs(1));
    assert_eq!(value["status"], "timed_out");
    assert_eq!(value["tasks"][0]["status"], "completed");
    assert_eq!(value["tasks"][1]["status"], "running");
    assert!(!host.seen.lock().unwrap()[0].cancellation().is_cancelled());
    host.release.notify_one();
    j.wait(&[b]).await;
    j.close().await;
}

#[tokio::test(start_paused = true)]
async fn wait_wakes_on_settlement_without_timeout() {
    for early in [false, true] {
        let mut host = PendingHost::new(false);
        let mut j = Journey::new(
            host.runner.clone(),
            Limits {
                max_wait: Duration::from_secs(60),
                max_background_duration: Duration::from_secs(120),
                ..limits()
            },
        );
        let id = j.start().await;
        host.entered(1).await;
        let (_, handle, tools) = mounted(j.ext.clone()).await;
        let wait = tools[WAIT_TOOL].execute_json_with_context(
            context_for(j.session.clone().unwrap(), "workspace"),
            json!({"task_ids":[id],"timeout_seconds":60}),
        );
        tokio::pin!(wait);
        let before = tokio::time::Instant::now();
        if !early {
            tokio::select! {biased; _=&mut wait=>panic!("completed too early"), _=tokio::task::yield_now()=>{}}
        }
        host.release.notify_one();
        assert_eq!(wait.await.unwrap()["status"], "done");
        assert_eq!(before.elapsed(), Duration::ZERO);
        handle.close().await.unwrap();
        j.close().await;
    }
}

#[tokio::test]
async fn parent_cancellation_during_wait_leaves_task_running() {
    let mut host = PendingHost::new(false);
    let mut j = Journey::new(host.runner.clone(), limits());
    let id = j.start().await;
    host.entered(1).await;
    let (_, handle, tools) = mounted(j.ext.clone()).await;
    let cancel = CancellationToken::new();
    let mut wait_context = context_for(j.session.clone().unwrap(), "workspace");
    wait_context.cancel = cancel.clone();
    let wait = tools[WAIT_TOOL].execute_json_with_context(wait_context, json!({"task_ids":[id]}));
    tokio::pin!(wait);
    tokio::select! {biased; _=&mut wait=>panic!("completed too early"), _=tokio::task::yield_now()=>{}}
    cancel.cancel();
    assert!(
        wait.await
            .unwrap_err()
            .to_string()
            .contains("failed: cancelled")
    );
    assert!(!host.seen.lock().unwrap()[0].cancellation().is_cancelled());
    assert_eq!(
        tools[STATUS_TOOL]
            .execute_json_with_context(
                context_for(j.session.clone().unwrap(), "workspace"),
                json!({"task_id":id})
            )
            .await
            .unwrap()["status"],
        "running"
    );
    host.release.notify_one();
    host.exited(1).await;
    handle.close().await.unwrap();
    j.close().await;
}

#[tokio::test]
async fn run_interrupt_during_wait_retains_uncertainty_and_task_continues() {
    let mut host = PendingHost::new(false);
    let mut j = Journey::new(host.runner.clone(), limits());
    let id = j.start().await;
    host.entered(1).await;
    let provider = Arc::new(FakeProvider::scripted(vec![
        wait_call(std::slice::from_ref(&id), None),
        done(),
    ]));
    let a = agent(
        j.ext.clone(),
        j.store.clone(),
        provider,
        PermissionDecision::Allow,
    );
    let mut run = a.prompt(j.session.clone(), "wait").await.unwrap();
    // ToolCallStarted is emitted once execution has been admitted.
    let mut events = run.events();
    loop {
        let event = events.recv().await.unwrap().unwrap();
        if matches!(event.kind, crabber::core::EventKind::ToolCallStarted) {
            break;
        }
    }
    run.interrupt();
    assert!(matches!(
        run.done().await,
        Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
    ));
    let records = snapshot_tools(&j.store, j.session.as_ref().unwrap()).await;
    assert_eq!(
        records.last().unwrap().status,
        crabber::core::ToolCallStatus::Running
    );
    assert!(!host.seen.lock().unwrap()[0].cancellation().is_cancelled());
    assert!(matches!(
        a.prompt(j.session.clone(), "new run").await,
        Err(crabber::RuntimeError::SessionBusy)
    ));
    let (_registry, handle, tools) = mounted(j.ext.clone()).await;
    assert_eq!(
        tools[STATUS_TOOL]
            .execute_json_with_context(
                context_for(j.session.clone().unwrap(), "workspace"),
                json!({"task_id":id})
            )
            .await
            .unwrap()["status"],
        "running"
    );
    host.release.notify_one();
    assert_eq!(
        tools[WAIT_TOOL]
            .execute_json_with_context(
                context_for(j.session.clone().unwrap(), "workspace"),
                json!({"task_ids":[id]})
            )
            .await
            .unwrap()["status"],
        "done"
    );
    handle.close().await.unwrap();
    j.close().await;
}

#[tokio::test(start_paused = true)]
async fn wait_at_deadline_tie_reports_done() {
    let release = Arc::new(tokio::sync::Notify::new());
    let finished = Arc::new(tokio::sync::Notify::new());
    let host: Runner = {
        let release = release.clone();
        let finished = finished.clone();
        Arc::new(move |_| {
            let release = release.clone();
            let finished = finished.clone();
            Box::pin(async move {
                release.notified().await;
                tokio::time::sleep(Duration::from_secs(1)).await;
                finished.notify_one();
                Ok(Response::Completed("ok".into()))
            })
        })
    };
    let mut j = Journey::new(host, limits());
    let id = j.start().await;
    let (_, handle, tools) = mounted(j.ext.clone()).await;
    release.notify_one();
    tokio::task::yield_now().await;
    let wait = tools[WAIT_TOOL].execute_json_with_context(
        context_for(j.session.clone().unwrap(), "workspace"),
        json!({"task_ids":[id],"timeout_seconds":1}),
    );
    tokio::pin!(wait);
    tokio::select! {biased;_=&mut wait=>panic!("early"),_=tokio::task::yield_now()=>{}}
    tokio::time::advance(Duration::from_secs(1)).await;
    finished.notified().await;
    // Once the runner has actually settled, terminal rows win over the ready deadline.
    assert_eq!(wait.await.unwrap()["status"], "done");
    handle.close().await.unwrap();
    j.close().await;
}

#[tokio::test]
async fn background_token_survives_run_end_and_run_interrupt() {
    let mut host = PendingHost::new(false);
    let mut j = Journey::new(
        host.runner.clone(),
        Limits {
            max_in_flight: 2,
            ..limits()
        },
    );
    let id = j.start().await;
    host.entered(1).await;
    assert!(!host.seen.lock().unwrap()[0].cancellation().is_cancelled());
    let provider = Arc::new(FakeProvider::scripted(vec![call(), done()]));
    let a = agent(
        j.ext.clone(),
        j.store.clone(),
        provider,
        PermissionDecision::Allow,
    );
    let run = a.prompt(j.session.clone(), "inline").await.unwrap();
    host.entered(2).await;
    run.interrupt();
    assert!(matches!(
        run.done().await,
        Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
    ));
    assert!(!host.seen.lock().unwrap()[0].cancellation().is_cancelled());
    assert!(matches!(
        a.prompt(j.session.clone(), "new run").await,
        Err(crabber::RuntimeError::SessionBusy)
    ));
    let (_registry, handle, tools) = mounted(j.ext.clone()).await;
    assert_eq!(
        tools[WAIT_TOOL]
            .execute_json_with_context(
                context_for(j.session.clone().unwrap(), "workspace"),
                json!({"task_ids":[id],"timeout_seconds":1})
            )
            .await
            .unwrap()["status"],
        "timed_out"
    );
    host.release.notify_waiters();
    assert_eq!(
        tools[WAIT_TOOL]
            .execute_json_with_context(
                context_for(j.session.clone().unwrap(), "workspace"),
                json!({"task_ids":[id]})
            )
            .await
            .unwrap()["status"],
        "done"
    );
    host.exited(2).await;
    handle.close().await.unwrap();
    j.close().await;
}

#[tokio::test]
async fn wait_holds_records_across_eviction() {
    let mut host = PendingHost::new(false);
    let runner = host.pending_on(1, "retained");
    let mut j = Journey::new(
        runner,
        Limits {
            max_tracked: 2,
            max_in_flight: 2,
            ..limits()
        },
    );
    let first = j.start().await;
    j.wait(std::slice::from_ref(&first)).await;
    let second = j.start().await;
    host.entered(1).await;
    let (_, handle, tools) = mounted(j.ext.clone()).await;
    let make_context = || context_for(j.session.clone().unwrap(), "workspace");
    let wait = tools[WAIT_TOOL]
        .execute_json_with_context(make_context(), json!({"task_ids":[first,second]}));
    tokio::pin!(wait);
    tokio::select! {biased;_=&mut wait=>panic!("early"),_=tokio::task::yield_now()=>{}}
    let mut other = j.other_owner();
    other.start().await;
    host.release.notify_one();
    let value = wait.await.unwrap();
    assert_eq!(value["status"], "done");
    assert_eq!(value["tasks"][0]["output"], "retained");
    assert!(
        tools[STATUS_TOOL]
            .execute_json_with_context(make_context(), json!({"task_id":first}))
            .await
            .unwrap_err()
            .to_string()
            .contains("invalid: task_id")
    );
    handle.close().await.unwrap();
    j.close().await;
    other.close().await;
}
