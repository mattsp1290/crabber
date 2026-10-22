use super::observe::{ProbeMount, ResultProbe, assert_provider_record, assert_settled};
use super::*;
use crabber::extension::ToolOutcomeClass;

#[tokio::test(flavor = "multi_thread")]
async fn stateful_durable_journey() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        call(EXECUTE_TOOL, json!({"code":record_code("x=41")})),
        done(),
        call(EXECUTE_TOOL, json!({"code":record_code("x+1")})),
        done(),
        call(CLEAR_TOOL, json!({})),
        done(),
        call(EXECUTE_TOOL, json!({"code":record_code("x")})),
        done(),
    ]));
    let a = agent(
        h.ext.clone(),
        store.clone(),
        provider.clone(),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let mut session = None;
    for turn in 0..4 {
        let run = a.prompt(session.clone(), "continue").await.unwrap();
        session = Some(run.session_id().clone());
        assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
        let records = snapshot_tools(&store, session.as_ref().unwrap()).await;
        assert_eq!(records.len(), turn + 1);
        let record = records.last().unwrap();
        assert_eq!(record.status, ToolCallStatus::Completed);
        let value = result_value(record);
        match turn {
            0 => {
                assert_eq!(value["result"]["text"], "");
                assert_eq!(value["generation"], 0);
            }
            1 => {
                assert_eq!(value["result"]["text"], "42");
                assert_eq!(value["state_reset"], false);
            }
            2 => assert_eq!(value, json!({"had_state":true,"generation":1})),
            _ => {
                assert_eq!(value["status"], "python_error");
                assert!(
                    value["exception"]["text"]
                        .as_str()
                        .unwrap()
                        .contains("NameError")
                );
                assert_eq!(value["state_reset_reason"], "cleared");
                assert_eq!(value["state_reset"], true);
            }
        }
        assert_provider_record(&provider, 2 * turn + 1, record);
        assert_settled(&store, session.as_ref().unwrap(), &records).await;
    }
    a.close_extensions().await.unwrap();
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn global_mount_isolates_durable_owners() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|o| o.limits.max_sessions = 4).await;
    let store = Arc::new(MemoryStore::new());
    let first = agent(
        h.ext.clone(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            call(EXECUTE_TOOL, json!({"code":record_code("x=7")})),
            done(),
        ])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = first.prompt(None, "assign").await.unwrap();
    let first_session = run.session_id().clone();
    run.done().await.unwrap();
    for other_workspace in [false, true] {
        let provider = Arc::new(FakeProvider::scripted(vec![
            call(EXECUTE_TOOL, json!({"code":record_code("x")})),
            done(),
        ]));
        let mut config = agent_config(h.dir.path());
        if other_workspace {
            config.workspace_id = "other".into();
        }
        let a = Agent::builder()
            .behavior_version("crabber-extensions-tests.v1")
            .store(store.clone())
            .provider(provider)
            .config(config)
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .shared_extension(h.ext.clone(), Scope::Global)
            .build()
            .unwrap();
        let run = a.prompt(None, "read").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let records = snapshot_tools(&store, &session).await;
        assert_eq!(
            result_value(records.last().unwrap())["status"],
            "python_error"
        );
        // Closing is shared, so retain mounts until both isolation checks finish.
        drop(a);
    }
    let isolated = h.tools[EXECUTE_TOOL]
        .execute_json_with_context(
            context_for(
                h.dir.path(),
                CancellationToken::new(),
                first_session,
                "alternate-workspace",
            ),
            json!({"code":record_code("x")}),
        )
        .await
        .unwrap();
    assert_eq!(isolated["status"], "python_error");
    first.close_extensions().await.unwrap();
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn permission_denial_and_ask_have_no_python_side_effect() {
    if !python_available() {
        return;
    }
    for decision in [PermissionDecision::Deny, PermissionDecision::Ask] {
        let h = Harness::new(|_| {}).await;
        let store = Arc::new(MemoryStore::new());
        let probe = Arc::new(ResultProbe::default());
        let provider = Arc::new(FakeProvider::scripted(vec![
            call(EXECUTE_TOOL, json!({"code":record_code("42")})),
            done(),
        ]));
        let a = Agent::builder()
            .behavior_version("crabber-extensions-tests.v1")
            .store(store.clone())
            .provider(provider.clone())
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
            assert_eq!(result_value(&records[0]), json!("permission denied"));
            assert_eq!(
                *probe.classes.lock().unwrap(),
                vec![(records[0].id.clone(), ToolOutcomeClass::PermissionDenied)]
            );
        }
        assert_eq!(h.ext.live_runners(), 0);
        assert!(pgids(h.dir.path()).is_empty());
        assert_eq!(
            std::fs::read_dir(h.dir.path().join("temporary"))
                .unwrap()
                .count(),
            0
        );
        if matches!(decision, PermissionDecision::Deny) {
            assert_provider_record(&provider, 1, &records[0]);
            assert_settled(&store, &session, &records).await;
        }
        a.close_extensions().await.unwrap();
        h.close().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn denied_clear_preserves_state() {
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
        ])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = a.prompt(None, "assign").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let denied = agent(
        h.ext.clone(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            call(CLEAR_TOOL, json!({})),
            done(),
        ])),
        PermissionDecision::Deny,
        h.dir.path(),
    );
    denied
        .prompt(Some(session.clone()), "clear")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(
        result_value(records.last().unwrap()),
        json!("permission denied")
    );
    let read = agent(
        h.ext.clone(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            call(EXECUTE_TOOL, json!({"code":record_code("x")})),
            done(),
        ])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    read.prompt(Some(session.clone()), "read")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(
        result_value(records.last().unwrap())["result"]["text"],
        "42"
    );
    a.close_extensions().await.unwrap();
    denied.close_extensions().await.unwrap();
    read.close_extensions().await.unwrap();
    h.close().await;
}

#[path = "runtime/input.rs"]
mod input;
#[path = "runtime/lifecycle.rs"]
mod process_lifecycle;
