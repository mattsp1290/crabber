use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn mount_close_terminates_idle_and_term_ignoring_runners() {
    if !python_available() {
        return;
    }
    for ignore in [false, true] {
        let h = Harness::new(|_| {}).await;
        h.execute(EXECUTE_TOOL, json!({"code":record_code("x=1")}))
            .await
            .unwrap();
        if ignore {
            let ready = h.dir.path().join("ready");
            let code = record_code(&format!(
                "import signal,time,pathlib; signal.signal(signal.SIGTERM,signal.SIG_IGN); pathlib.Path({}).touch(); time.sleep(60)",
                serde_json::to_string(ready.to_str().unwrap()).unwrap()
            ));
            let tool = h.tools[EXECUTE_TOOL].clone();
            let root = h.dir.path().to_path_buf();
            let call = tokio::spawn(async move {
                tool.execute_json_with_context(
                    context(&root, CancellationToken::new()),
                    json!({"code":code}),
                )
                .await
            });
            wait_file(&ready).await;
            let now = Instant::now();
            h.close().await;
            assert!(now.elapsed() < Duration::from_secs(5));
            assert!(
                call.await
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .ends_with("manager-closing")
            );
        } else {
            h.close().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn close_during_in_flight_call_interrupts_and_cleans_up() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let ready = h.dir.path().join("ready");
    let code = record_code(&format!(
        "import pathlib,time; pathlib.Path({}).touch(); time.sleep(60)",
        serde_json::to_string(ready.to_str().unwrap()).unwrap()
    ));
    let tool = h.tools[EXECUTE_TOOL].clone();
    let root = h.dir.path().to_path_buf();
    let call = tokio::spawn(async move {
        tool.execute_json_with_context(
            context(&root, CancellationToken::new()),
            json!({"code":code}),
        )
        .await
    });
    wait_file(&ready).await;
    let now = Instant::now();
    h.close().await;
    assert!(now.elapsed() < Duration::from_secs(5));
    assert!(
        call.await
            .unwrap()
            .unwrap_err()
            .to_string()
            .ends_with("manager-closing")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_run_resets_existing_state_and_next_call_reports_canceled() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let ready = h.dir.path().join("ready");
    let sleeping = record_code(&format!(
        "import pathlib,time; pathlib.Path({}).touch(); time.sleep(60)",
        serde_json::to_string(ready.to_str().unwrap()).unwrap()
    ));
    let provider = Arc::new(FakeProvider::scripted(vec![
        call(EXECUTE_TOOL, json!({"code":record_code("x=1")})),
        done(),
        call(EXECUTE_TOOL, json!({"code":sleeping})),
        call(
            EXECUTE_TOOL,
            json!({"code":record_code("'x' in globals()")}),
        ),
        done(),
    ]));
    let a = agent(
        h.ext.clone(),
        store.clone(),
        provider,
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let seed = a.prompt(None, "assign").await.unwrap();
    let session = seed.session_id().clone();
    seed.done().await.unwrap();
    let run = a.prompt(Some(session.clone()), "wait").await.unwrap();
    wait_file(&ready).await;
    run.interrupt();
    assert!(matches!(
        run.done().await,
        Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
    ));
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records.last().unwrap().status, ToolCallStatus::Running);
    assert!(records.last().unwrap().result.is_none());
    groups_gone(h.dir.path()).await;
    assert!(matches!(
        a.prompt(Some(session.clone()), "read").await,
        Err(crabber::RuntimeError::SessionBusy)
    ));
    // Resource reset can be verified independently of durable outcome recovery.
    // The uncertain run cannot authorize another run in this session.
    let next = h.tools[EXECUTE_TOOL]
        .execute_json_with_context(
            context_for(
                h.dir.path(),
                CancellationToken::new(),
                session.clone(),
                "workspace",
            ),
            json!({"code":record_code("'x' in globals()")}),
        )
        .await
        .unwrap();
    assert_eq!(next["result"]["text"], "False");
    assert_eq!(next["state_reset_reason"], "canceled");
    a.close_extensions().await.unwrap();
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remount_starts_with_empty_state() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let a = agent(
        h.ext.clone(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            call(EXECUTE_TOOL, json!({"code":record_code("x=1")})),
            done(),
        ])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = a.prompt(None, "assign").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    a.close_extensions().await.unwrap();
    h.close().await;
    let fresh = Arc::new(PythonRepl::new(options(h.dir.path())).unwrap());
    let b = agent(
        fresh.clone(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            call(EXECUTE_TOOL, json!({"code":record_code("x")})),
            done(),
        ])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    b.prompt(Some(session.clone()), "read")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    let value = result_value(records.last().unwrap());
    assert_eq!(value["status"], "python_error");
    assert_eq!(value["generation"], 0);
    b.close_extensions().await.unwrap();
    assert_eq!(fresh.live_runners(), 0);
    groups_gone(h.dir.path()).await;
    assert_eq!(
        std::fs::read_dir(h.dir.path().join("temporary"))
            .unwrap()
            .count(),
        0
    );
}
