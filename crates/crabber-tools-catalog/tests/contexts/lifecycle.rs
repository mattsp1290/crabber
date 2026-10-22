use super::*;
#[tokio::test]
async fn cross_session_isolation_and_targeted_close() {
    let _cwd = Cwd::new();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    for (root, text) in [(a.path(), "A"), (b.path(), "B")] {
        std::fs::write(root.join("x"), text).unwrap();
    }
    let script = (0..6)
        .flat_map(|_| [turn("file_read", json!({"path":"x"})), done()])
        .collect();
    let (agent, store) = agent(a.path(), b.path(), base(), script, allow());
    let first = execution(SessionId::new(), "ws-a", a.path());
    let second = execution(SessionId::new(), "ws-a", a.path());
    let other = execution(SessionId::new(), "ws-b", b.path());
    for ctx in [&first, &second, &other] {
        admit(&agent, ctx, "read").await.done().await.unwrap();
    }
    agent
        .close_session_resources(first.session(), Duration::from_secs(2))
        .await
        .unwrap();
    agent
        .prompt_in(&second, "still here", None)
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(
        value(&calls(&store, second.session().session_id()).await[1])["content"],
        "A"
    );
    agent
        .close_workspace_resources(first.session(), Duration::from_secs(2))
        .await
        .unwrap();
    let replacement = execution(SessionId::new(), "ws-a", a.path());
    assert!(
        agent
            .prompt_keyed_in(
                &replacement,
                "closed",
                admission_options(&agent, "closed"),
                None
            )
            .await
            .is_err()
    );
    agent
        .prompt_in(&other, "unrelated", None)
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert!(
        calls(&store, other.session().session_id())
            .await
            .iter()
            .all(|r| value(r)["content"] == "B")
    );
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn frontend_name_collision_is_rejected() {
    use crabber::core::{FrontendTool, InputEnvelope, SessionRevision};
    let _cwd = Cwd::new();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let (agent, store) = agent(
        a.path(),
        b.path(),
        base(),
        vec![
            turn("file_write", json!({"path":"never","content":"bad"})),
            done(),
        ],
        allow(),
    );
    let ctx = execution(SessionId::new(), "ws-a", a.path());
    let mut wire =
        serde_json::to_value(InputEnvelope::text("collision", SessionRevision::INITIAL).unwrap())
            .unwrap();
    wire["frontend_tools"] = json!([FrontendTool {
        name: "file_read".into(),
        description: "collision".into(),
        input_parameters: json!({"type":"object"}),
        result_contract: None
    }]);
    let input: InputEnvelope = serde_json::from_value(wire).unwrap();
    let result = agent
        .prompt_input_keyed_in(&ctx, input, admission_options(&agent, "collision"), None)
        .await;
    let error = match result {
        Err(error) => error,
        Ok(crabber::Admission::Started { handle, .. }) => handle.done().await.unwrap_err(),
        Ok(_) => panic!("fresh collision replayed"),
    };
    assert!(error.to_string().contains("tool name collision"), "{error}");
    assert!(!a.path().join("never").exists());
    let _ = store;
    agent.close_extensions().await.unwrap();
}
