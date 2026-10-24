use super::*;
#[tokio::test]
async fn enabled_set_and_restriction_are_preserved() {
    let _cwd = Cwd::new();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    for restrict in [false, true] {
        let mut p = base();
        p.enabled = EnabledSet::only([ToolId::FileRead]);
        p.restrict_to_enabled = restrict;
        let script = (0..2)
            .flat_map(|_| {
                [
                    turn("shell", json!({"cmd":"true"})),
                    turn("host_marker", json!({})),
                    done(),
                ]
            })
            .collect();
        let (agent, store) = agent(a.path(), b.path(), p, script, allow());
        for (ws, root) in [("ws-a", a.path()), ("ws-b", b.path())] {
            let records = execute_in(&agent, &store, ws, root).await;
            assert_eq!(records[0].status, ToolCallStatus::Failed);
            assert_eq!(
                records[1].status,
                if restrict {
                    ToolCallStatus::Failed
                } else {
                    ToolCallStatus::Completed
                }
            );
        }
        agent.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn host_deny_policy_applies_in_every_workspace() {
    let _cwd = Cwd::new();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    for root in [a.path(), b.path()] {
        std::fs::write(root.join("x"), "owned").unwrap();
    }
    let script = (0..2)
        .flat_map(|_| {
            [
                turn("shell", json!({"cmd":"touch forbidden"})),
                turn("file_read", json!({"path":"x"})),
                done(),
            ]
        })
        .collect();
    let (agent, store) = agent(
        a.path(),
        b.path(),
        base(),
        script,
        allow().with_rule("shell", PermissionDecision::Deny),
    );
    for (ws, root) in [("ws-a", a.path()), ("ws-b", b.path())] {
        let records = execute_in(&agent, &store, ws, root).await;
        assert_eq!(records[0].status, ToolCallStatus::Failed);
        assert_eq!(records[1].status, ToolCallStatus::Completed);
        assert_eq!(value(&records[1])["content"], "owned");
        assert!(!root.join("forbidden").exists());
    }
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn shell_and_url_policies_are_observable() {
    let _cwd = Cwd::new();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let mut p = base();
    p.url_fetch.as_mut().unwrap().allow = vec![HostPattern::exact("allowed.invalid").unwrap()];
    let script = (0..2)
        .flat_map(|_| {
            [
                turn("shell", json!({"cmd":"printf 12345678901234567890"})),
                turn("url_fetch", json!({"url":"https://example.com/"})),
                done(),
            ]
        })
        .collect();
    let (agent, store) = agent(a.path(), b.path(), p, script, allow());
    for (ws, root) in [("ws-a", a.path()), ("ws-b", b.path())] {
        let records = execute_in(&agent, &store, ws, root).await;
        assert_eq!(value(&records[0])["stdout"], "1234567890123456");
        assert_eq!(value(&records[0])["stdout_truncated"], true);
        assert_eq!(records[1].status, ToolCallStatus::Failed);
        assert_eq!(value(&records[1])["error"]["category"], "validation");
    }
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn shared_deny_policy_applies_to_both_admitted_workspaces() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    for root in [a.path(), b.path()] {
        std::fs::write(root.join(".env"), "private fixture").unwrap();
    }
    let mut policy = base();
    policy.enabled = EnabledSet::only([ToolId::FileRead]);
    policy.deny = PathDenyPolicy::new([".env".to_owned()]).unwrap();
    let (agent, store) = agent(
        a.path(),
        b.path(),
        policy,
        vec![
            turn("file_read", json!({"path":".env"})),
            done(),
            turn("file_read", json!({"path":".env"})),
            done(),
        ],
        allow(),
    );
    for (workspace, root) in [("ws-a", a.path()), ("ws-b", b.path())] {
        let records = execute_in(&agent, &store, workspace, root).await;
        assert_eq!(records[0].status, ToolCallStatus::Failed);
        assert_eq!(value(&records[0])["error"]["category"], "path_denied");
        assert_eq!(
            value(&records[0])["error"]["message"],
            "path denied by host policy"
        );
    }
    agent.close_extensions().await.unwrap();
}
