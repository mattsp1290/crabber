use super::*;

#[tokio::test]
async fn wait_input_validation() {
    let (_, handle, tools) = mounted(Arc::new(extension(Response::Completed(String::new())))).await;
    let id = "task_00000000000000000000000000000000_0000000000000001";
    for args in [
        json!({"task_ids":[]}),
        json!({"task_ids":[id,id]}),
        json!({"task_ids":[id,id,id,id,id]}),
        json!({"task_ids":[id],"timeout_seconds":null}),
        json!({"task_ids":[id],"timeout_seconds":0}),
        json!({"task_ids":[id],"timeout_seconds":6}),
        json!({"task_ids":["bad"]}),
        json!({"task_ids":[id],"unknown":1}),
        json!({"task_ids":[id]}),
    ] {
        assert!(
            tools[WAIT_TOOL]
                .execute_json_with_context(context(CancellationToken::new()), args)
                .await
                .unwrap_err()
                .to_string()
                .contains("input invalid")
        );
    }
    handle.close().await.unwrap();
}

#[tokio::test]
async fn missing_workspace_id_is_an_owner_failure() {
    let (_, handle, tools) = mounted(Arc::new(extension(Response::Completed(String::new())))).await;
    let id = "task_00000000000000000000000000000000_0000000000000001";
    for (name, args) in [
        (START_TOOL, arguments()),
        (STATUS_TOOL, json!({"task_id":id})),
        (LIST_TOOL, json!({})),
        (WAIT_TOOL, json!({"task_ids":[id]})),
    ] {
        let context = context_for(SessionId::from("session"), "");
        assert_eq!(
            tools[name]
                .execute_json_with_context(context, args)
                .await
                .unwrap_err()
                .to_string(),
            "tool execution failed: delegate task failed: owner"
        );
    }
    handle.close().await.unwrap();
}

#[tokio::test]
async fn other_session_and_other_workspace_cannot_see_tasks() {
    let mut j = Journey::new(runner(Response::Completed("ok".into())), limits());
    let id = j.start().await;
    j.wait(std::slice::from_ref(&id)).await;
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        status_call(&id),
        wait_call(std::slice::from_ref(&id), None),
        list_call(),
        done(),
    ]));
    let other = agent(
        j.ext.clone(),
        store.clone(),
        provider,
        PermissionDecision::Allow,
    );
    let run = other.prompt(None, "foreign session").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records[0].status, crabber::core::ToolCallStatus::Failed);
    assert_eq!(records[1].status, crabber::core::ToolCallStatus::Failed);
    assert_eq!(result_value(&records[2]), json!({"tasks":[]}));
    // Crabber fixes the workspace at session admission. Its public context API
    // additionally proves the same session cannot read through another workspace.
    let (_, handle, tools) = mounted(j.ext.clone()).await;
    for (name, args) in [
        (STATUS_TOOL, json!({"task_id":id})),
        (WAIT_TOOL, json!({"task_ids":[id]})),
        (LIST_TOOL, json!({})),
    ] {
        let context = context_for(j.session.clone().unwrap(), "other");
        let reply = tools[name].execute_json_with_context(context, args).await;
        if name == LIST_TOOL {
            assert_eq!(reply.unwrap(), json!({"tasks":[]}));
        } else {
            assert!(
                reply
                    .unwrap_err()
                    .to_string()
                    .contains("input invalid: task_id")
            );
        }
    }
    handle.close().await.unwrap();
    j.close().await;
}

#[tokio::test]
async fn start_input_validation_matches_inline() {
    let mut j = Journey::new(runner(Response::Completed(String::new())), limits());
    for args in [
        json!({"task":"","profile":"x"}),
        json!({"task":"x".repeat(101),"profile":"x"}),
        json!({"task":"x\0","profile":"x"}),
        json!({"task":"x","profile":""}),
        json!({"task":"x","profile":"bad space"}),
        json!({"task":"x","profile":"x".repeat(51)}),
        json!({"task":"x","profile":"x","extra":0}),
    ] {
        let record = j.call(START_TOOL, args).await;
        assert_eq!(record.status, crabber::core::ToolCallStatus::Failed);
    }
    for args in [
        json!({"task":"x".repeat(100),"profile":"x".repeat(50)}),
        json!({"task":"é".repeat(50),"profile":"a._:/-9"}),
    ] {
        let id = read_task_id(&j.call(START_TOOL, args).await);
        j.wait(&[id]).await;
    }
    j.close().await;
}

#[tokio::test]
async fn policy_deny_and_ask_do_not_touch_runner_or_registry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let host: Runner = {
        let calls = calls.clone();
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Response::Completed(String::new())) })
        })
    };
    let ext = Arc::new(configured(host, limits()));
    let (_registry, handle, tools) = mounted(ext.clone()).await;
    for decision in [PermissionDecision::Deny, PermissionDecision::Ask] {
        let id = "task_00000000000000000000000000000000_0000000000000001";
        for (name, args) in [
            (START_TOOL, arguments()),
            (STATUS_TOOL, json!({"task_id":id})),
            (LIST_TOOL, json!({})),
            (WAIT_TOOL, json!({"task_ids":[id]})),
        ] {
            let store = Arc::new(MemoryStore::new());
            let provider = Arc::new(FakeProvider::scripted(vec![tool_call(name, args), done()]));
            let a = agent(ext.clone(), store.clone(), provider, decision);
            let run = a.prompt(None, "denied").await.unwrap();
            let session = run.session_id().clone();
            run.done().await.unwrap();
            assert_eq!(
                snapshot_tool(&store, &session).await.status,
                if matches!(decision, PermissionDecision::Ask) {
                    crabber::core::ToolCallStatus::Pending
                } else {
                    crabber::core::ToolCallStatus::Failed
                }
            );
            // A pending approval keeps the durable session busy. Inspect the exact
            // owner's registry through the public executor without granting approval.
            assert_eq!(
                tools[LIST_TOOL]
                    .execute_json_with_context(context_for(session, "workspace"), json!({}))
                    .await
                    .unwrap(),
                json!({"tasks":[]})
            );
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    handle.close().await.unwrap();
}
