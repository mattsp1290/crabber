use super::observe::{
    ProbeMount, ResultProbe, assert_provider_record, assert_settled, terminal_for,
};
use super::*;
use crabber::extension::ToolOutcomeClass;
#[tokio::test(flavor = "multi_thread")]
async fn start_then_status_are_durable_and_reenter_the_model() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        call(
            START_TOOL,
            json!({"command":command("printf out; printf err >&2; exit 3")}),
        ),
        done(),
    ]));
    let a = agent(
        h.ext.clone(),
        store.clone(),
        provider.clone(),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = a.prompt(None, "start").await.unwrap();
    let session = run.session_id().clone();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
    let records = snapshot_tools(&store, &session).await;
    let start = result_value(&records[0]);
    assert_eq!(start["state"], "running");
    let id = start["id"].as_str().unwrap();
    assert_eq!(id.len(), 53);
    assert_eq!(start["timeout_seconds"], 0);
    assert_eq!(start["started_at"].as_str().unwrap().len(), 30);
    let status = terminal_for(&h, &session, id).await;
    assert_eq!(status["state"], "failed");
    assert_eq!(status["exit_code"], 3);
    assert_eq!(status["stdout"], json!({"text":"out","truncated":false}));
    assert_eq!(status["stderr"], json!({"text":"err","truncated":false}));
    let provider2 = Arc::new(FakeProvider::scripted(vec![
        call(STATUS_TOOL, json!({"id":id})),
        done(),
    ]));
    let a2 = agent(
        h.ext.clone(),
        store.clone(),
        provider2.clone(),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = a2.prompt(Some(session.clone()), "status").await.unwrap();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
    let records = snapshot_tools(&store, &session).await;
    let record = records.iter().find(|r| r.name == STATUS_TOOL).unwrap();
    assert_eq!(result_value(record), status);
    assert_provider_record(&provider2, 1, record);
    assert_settled(&store, &session, &records).await;
    assert_eq!(status["completed_at"].as_str().unwrap().len(), 30);
    a.close_extensions().await.unwrap();
    a2.close_extensions().await.unwrap();
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn owner_isolation_and_list_order() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let first = h.start("sleep 30").await;
    let second = h.start("sleep 30").await;
    let ids = [
        first["id"].as_str().unwrap(),
        second["id"].as_str().unwrap(),
    ];
    for (session, workspace) in [
        (SessionId::from("other"), "workspace"),
        (SessionId::from("session"), "other"),
    ] {
        let ctx = || {
            context_for(
                h.dir.path(),
                CancellationToken::new(),
                session.clone(),
                workspace,
            )
        };
        assert_eq!(
            h.tools[LIST_TOOL]
                .execute_json_with_context(ctx(), json!({}))
                .await
                .unwrap(),
            json!({"jobs":[]})
        );
        for tool in [STATUS_TOOL, KILL_TOOL] {
            assert!(
                h.tools[tool]
                    .execute_json_with_context(ctx(), json!({"id":ids[0]}))
                    .await
                    .unwrap_err()
                    .to_string()
                    .ends_with("job-not-found")
            );
        }
    }
    let list = h.execute(LIST_TOOL, json!({})).await.unwrap();
    assert_eq!(list["jobs"][0]["id"], ids[0]);
    assert_eq!(list["jobs"][1]["id"], ids[1]);
    for id in ids {
        assert_eq!(h.kill(id).await["state"], "killed");
    }
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn permission_deny_and_ask_never_spawn() {
    if !shell_available() {
        return;
    }
    for decision in [PermissionDecision::Deny, PermissionDecision::Ask] {
        let h = Harness::new(|_| {}).await;
        let store = Arc::new(MemoryStore::new());
        let provider = Arc::new(FakeProvider::scripted(vec![
            call(START_TOOL, json!({"command":command(": > \"$CANARY\"")})),
            done(),
        ]));
        let probe = Arc::new(ResultProbe::default());
        let a = Agent::builder()
            .behavior_version("crabber-extensions-tests.v1")
            .store(store.clone())
            .provider(provider)
            .config(agent_config(h.dir.path()))
            .policy(Arc::new(StaticPolicy::new(decision)))
            .shared_extension(h.ext.clone(), Scope::Global)
            .shared_extension(Arc::new(ProbeMount(probe.clone())), Scope::Global)
            .build()
            .unwrap();
        let run = a.prompt(None, "deny").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let records = snapshot_tools(&store, &session).await;
        if matches!(decision, PermissionDecision::Ask) {
            assert_eq!(records[0].status, ToolCallStatus::Pending);
            assert!(records[0].result.is_none());
            assert!(probe.classes.lock().unwrap().is_empty());
        } else {
            assert_eq!(records[0].status, ToolCallStatus::Failed);
            assert_eq!(
                *probe.classes.lock().unwrap(),
                vec![(records[0].id.clone(), ToolOutcomeClass::PermissionDenied)]
            );
            assert_eq!(result_value(&records[0]), json!("permission denied"));
        }
        assert_eq!(h.ext.live_jobs(), 0);
        assert!(!h.dir.path().join("canary").exists());
        assert!(pgids(h.dir.path()).is_empty());
        assert_eq!(
            h.execute(LIST_TOOL, json!({})).await.unwrap(),
            json!({"jobs":[]})
        );
        a.close_extensions().await.unwrap();
        h.close().await;
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn status_result_at_capacity_survives_json_escaping() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|o| o.limits.max_output_bytes_per_stream = 32).await;
    let start=h.start("i=0; while [ \"$i\" -lt 16 ]; do printf '\"\\001'; printf '\"\\001' >&2; i=$((i+1)); done").await;
    let status = h.terminal(start["id"].as_str().unwrap()).await;
    let store = Arc::new(MemoryStore::new());
    // Create a durable session before starting the owner-scoped command.
    let provider = Arc::new(FakeProvider::scripted(vec![done()]));
    let seed = agent(
        h.ext.clone(),
        store.clone(),
        provider,
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = seed.prompt(None, "seed").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let owner_start = h.tools[START_TOOL].execute_json_with_context(context_for(h.dir.path(),CancellationToken::new(),session.clone(),"workspace"),json!({"command":command("i=0; while [ \"$i\" -lt 16 ]; do printf '\"\\001'; printf '\"\\001' >&2; i=$((i+1)); done")})).await.unwrap();
    let expected = terminal_for(&h, &session, owner_start["id"].as_str().unwrap()).await;
    let provider = Arc::new(FakeProvider::scripted(vec![
        call(STATUS_TOOL, json!({"id":owner_start["id"]})),
        done(),
    ]));
    let a = agent(
        h.ext.clone(),
        store.clone(),
        provider.clone(),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    a.prompt(Some(session.clone()), "status")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    let record = &records[0];
    let text = result_text(record);
    assert_eq!(serde_json::from_str::<Value>(text).unwrap(), expected);
    assert_eq!(expected["stdout"], status["stdout"]);
    assert_eq!(expected["stderr"], status["stderr"]);
    assert_provider_record(&provider, 1, record);
    assert_settled(&store, &session, &records).await;
    assert_eq!(expected["stdout"]["text"].as_str().unwrap().len(), 32);
    assert_eq!(expected["stdout"]["truncated"], false);
    assert!(
        text.len()
            <= Limits {
                max_output_bytes_per_stream: 32,
                ..limits()
            }
            .worst_case_status_bytes()
    );
    a.close_extensions().await.unwrap();
    seed.close_extensions().await.unwrap();
    h.close().await;
}

#[path = "runtime/input.rs"]
mod input;
