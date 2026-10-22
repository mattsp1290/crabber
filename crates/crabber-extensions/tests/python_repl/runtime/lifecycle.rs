use super::*;
use crabber::runtime::ExecutionMode;

#[tokio::test(flavor = "multi_thread")]
async fn timeout_resets_state_and_next_call_reports_timed_out() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        call(EXECUTE_TOOL, json!({"code":record_code("x=1")})),
        done(),
        call(
            EXECUTE_TOOL,
            json!({"code":record_code("import time; time.sleep(30)"),"timeout_seconds":1}),
        ),
        done(),
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
    let run = a.prompt(None, "assign").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    a.prompt(Some(session.clone()), "timeout")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records[1].status, ToolCallStatus::Failed);
    assert!(
        result_value(&records[1])
            .as_str()
            .unwrap()
            .ends_with("timed-out")
    );
    groups_gone(h.dir.path()).await;
    a.prompt(Some(session.clone()), "read")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    let value = result_value(&records[2]);
    assert_eq!(value["state_reset_reason"], "timed_out");
    assert_eq!(value["result"]["text"], "False");
    a.close_extensions().await.unwrap();
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn capacity_bound_is_a_fixed_durable_failure() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let mut agents = Vec::new();
    for n in 0..3 {
        let a = agent(
            h.ext.clone(),
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![
                call(EXECUTE_TOOL, json!({"code":record_code("1")})),
                done(),
            ])),
            PermissionDecision::Allow,
            h.dir.path(),
        );
        let run = a.prompt(None, "capacity").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let records = snapshot_tools(&store, &session).await;
        if n == 2 {
            assert_eq!(records[0].status, ToolCallStatus::Failed);
            assert!(
                result_value(&records[0])
                    .as_str()
                    .unwrap()
                    .ends_with("capacity-exhausted")
            );
        } else {
            assert_eq!(records[0].status, ToolCallStatus::Completed);
        }
        agents.push(a);
    }
    assert_eq!(h.ext.live_runners(), 2);
    for a in agents {
        a.close_extensions().await.unwrap();
    }
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn workspace_root_drift_preserves_state() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let a = agent(
        h.ext.clone(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            call(EXECUTE_TOOL, json!({"code":record_code("x=42")})),
            done(),
            call(EXECUTE_TOOL, json!({"code":record_code("x")})),
            done(),
        ])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = a.prompt(None, "assign").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let other = tempfile::tempdir().unwrap();
    for tool in [EXECUTE_TOOL, CLEAR_TOOL] {
        let args = if tool == EXECUTE_TOOL {
            json!({"code":"x=7"})
        } else {
            json!({})
        };
        let error = h.tools[tool]
            .execute_json_with_context(
                context_for(
                    other.path(),
                    CancellationToken::new(),
                    session.clone(),
                    "workspace",
                ),
                args,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().ends_with("workspace-root-mismatch"));
    }
    a.prompt(Some(session.clone()), "read")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(result_value(&records[1])["result"]["text"], "42");
    a.close_extensions().await.unwrap();
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn idle_runner_death_is_reported_on_next_turn() {
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
            call(EXECUTE_TOOL, json!({"code":"1"})),
            done(),
            call(EXECUTE_TOOL, json!({"code":record_code("1")})),
            done(),
        ])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = a.prompt(None, "assign").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let id = pgids(h.dir.path())[0];
    rustix::process::kill_process_group(
        rustix::process::Pid::from_raw(id).unwrap(),
        rustix::process::Signal::KILL,
    )
    .unwrap();
    sleep(Duration::from_millis(100)).await;
    a.prompt(Some(session.clone()), "dead")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert!(
        result_value(&records[1])
            .as_str()
            .unwrap()
            .ends_with("runner-failed")
    );
    a.prompt(Some(session.clone()), "reset")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(
        result_value(&records[2])["state_reset_reason"],
        "runner_failed"
    );
    a.close_extensions().await.unwrap();
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn queue_bound_is_a_fixed_durable_failure() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let seed = agent(
        h.ext.clone(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![done()])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = seed.prompt(None, "seed").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let ready = h.dir.path().join("ready");
    let release = h.dir.path().join("release");
    let code = record_code(&format!(
        "import time,pathlib\npathlib.Path({}).touch()\nwhile not pathlib.Path({}).exists(): time.sleep(.01)",
        serde_json::to_string(ready.to_str().unwrap()).unwrap(),
        serde_json::to_string(release.to_str().unwrap()).unwrap()
    ));
    let tool = h.tools[EXECUTE_TOOL].clone();
    let root = h.dir.path().to_path_buf();
    let owner = session.clone();
    let holder = tokio::spawn(async move {
        tool.execute_json_with_context(
            context_for(&root, CancellationToken::new(), owner, "workspace"),
            json!({"code":code}),
        )
        .await
    });
    wait_file(&ready).await;
    let provider = Arc::new(FakeProvider::scripted(vec![
        calls(&[
            (EXECUTE_TOOL, json!({"code":"1"})),
            (EXECUTE_TOOL, json!({"code":"2"})),
            (EXECUTE_TOOL, json!({"code":"3"})),
        ]),
        done(),
    ]));
    let probe = Arc::new(ResultProbe::default());
    let a = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store.clone())
        .provider(provider)
        .config(agent_config(h.dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(ExecutionMode::Parallel { max: 3 })
        .shared_extension(h.ext.clone(), Scope::Global)
        .shared_extension(Arc::new(ProbeMount(probe.clone())), Scope::Global)
        .build()
        .unwrap();
    let run = a.prompt(Some(session.clone()), "queue").await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if probe
                .final_results
                .lock()
                .unwrap()
                .iter()
                .any(|(_, v)| v.as_str().is_some_and(|v| v.ends_with("queue-full")))
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    std::fs::write(release, "").unwrap();
    holder.await.unwrap().unwrap();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(
        records
            .iter()
            .filter(|r| r.status == ToolCallStatus::Failed)
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|r| r.status == ToolCallStatus::Completed)
            .count(),
        2
    );
    assert!(
        result_value(
            records
                .iter()
                .find(|r| r.status == ToolCallStatus::Failed)
                .unwrap()
        )
        .as_str()
        .unwrap()
        .ends_with("queue-full")
    );
    a.close_extensions().await.unwrap();
    seed.close_extensions().await.unwrap();
    h.close().await;
}
