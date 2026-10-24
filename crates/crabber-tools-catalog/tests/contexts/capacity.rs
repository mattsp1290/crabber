use super::*;
async fn capacity_case(same_workspace: bool) {
    let _cwd = Cwd::new();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    for root in [a.path(), b.path()] {
        std::fs::write(root.join("x"), "owned").unwrap();
    }
    let script = vec![
        turn(
            "shell",
            json!({"cmd":"touch started; while [ ! -f release ]; do sleep 0.01; done"}),
        ),
        turn("file_read", json!({"path":"x"})),
        done(),
        done(),
        turn("file_read", json!({"path":"x"})),
        done(),
    ];
    let (agent, store) = agent(a.path(), b.path(), base(), script, allow());
    let ctx = execution(SessionId::new(), "ws-a", a.path());
    let held = admit(&agent, &ctx, "hold").await;
    wait_file(&a.path().join("started")).await;
    let (ws, root) = if same_workspace {
        ("ws-a", a.path())
    } else {
        ("ws-b", b.path())
    };
    let records = execute_in(&agent, &store, ws, root).await;
    assert_eq!(
        records[0].status,
        if same_workspace {
            ToolCallStatus::Failed
        } else {
            ToolCallStatus::Completed
        }
    );
    if same_workspace {
        assert_eq!(value(&records[0])["error"]["category"], "unavailable");
    }
    std::fs::write(a.path().join("release"), "").unwrap();
    held.done().await.unwrap();
    let records = execute_in(&agent, &store, "ws-a", a.path()).await;
    assert_eq!(records[0].status, ToolCallStatus::Completed);
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn independent_capacity_per_workspace() {
    capacity_case(false).await;
}
#[tokio::test]
async fn shared_capacity_within_a_workspace() {
    capacity_case(true).await;
}
