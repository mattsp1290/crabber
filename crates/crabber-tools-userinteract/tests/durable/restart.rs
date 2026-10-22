use super::*;
#[tokio::test]
async fn restart_resume_does_not_repeat_completed_effects() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::new());
    let writes = Arc::new(AtomicUsize::new(0));
    let mut batch = turn("file_write", json!({"path":"once","content":"owned"}));
    batch.pop();
    batch.extend(turn("user_interact", json!({"question":"Q?"})));
    let provider = Arc::new(FakeProvider::scripted(vec![batch, done()]));
    let make = |policy: UserInteractPolicy| {
        let count = writes.clone();
        let limits = limits();
        let file =
            crabber_tools_fileops::write::definition(Arc::new(crabber_tools_fileops::Options {
                root: crabber_tools_core::WorkspaceRoot::open(root.path()).unwrap(),
                capacity: crabber_tools_core::Capacity::new(&limits).unwrap(),
                limits,
                pre_write: Some(Arc::new(move || {
                    count.fetch_add(1, Ordering::SeqCst);
                })),
            }));
        Agent::builder()
            .behavior_version("durable-tests/0.4.0")
            .store(store.clone())
            .provider(provider.clone())
            .config(config(root.path()))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .execution_mode(ExecutionMode::Parallel { max: 2 })
            .shared_tool(file)
            .shared_tool(tool(policy))
            .build()
            .unwrap()
    };
    let original = make(UserInteractPolicy::durable(authority()));
    let handle = original.prompt(None, "write then ask").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    let snapshot = original
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let receipt = original
        .answer_tool_interaction(answer(
            &snapshot,
            0,
            ToolInteractionReply::Answer {
                value: json!("yes"),
            },
        ))
        .await
        .unwrap();
    original.close_extensions().await.unwrap();
    drop(original);
    let changed = make(UserInteractPolicy::durable(
        AuthorityRequirement::new("changed", "1").unwrap(),
    ));
    let request = ContinueTool {
        id: ControlId::new(),
        session_id: session.clone(),
        run_id: run,
        continuation: snapshot.continuation.clone(),
        expected_revision: receipt.revision,
    };
    assert!(changed.continue_tool(request).await.is_err());
    changed.close_extensions().await.unwrap();
    let rebuilt = make(UserInteractPolicy::durable(authority()));
    resume(&rebuilt, &snapshot, receipt.revision).await;
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        std::fs::read_to_string(root.path().join("once")).unwrap(),
        "owned"
    );
    assert_eq!(
        calls(&store, &session)
            .await
            .iter()
            .filter(|r| r.name == "file_write")
            .count(),
        1
    );
    rebuilt.close_extensions().await.unwrap();
}
