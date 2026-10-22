use super::*;
#[tokio::test(flavor = "multi_thread")]
async fn registered_contract_is_exact() {
    let h = Harness::new(|_| {}).await;
    let plan = h.registry.acquire(&SessionId::from("session"));
    assert_eq!(plan.tools.len(), 4);
    for (name, permission, retry) in [
        (START_TOOL, PERMISSION_START, false),
        (STATUS_TOOL, PERMISSION_READ, true),
        (LIST_TOOL, PERMISSION_READ, true),
        (KILL_TOOL, PERMISSION_KILL, false),
    ] {
        let tool = plan.tools.iter().find(|t| t.info.name == name).unwrap();
        assert_eq!(tool.info.retry_safe, retry);
        assert_eq!(tool.info.required_permissions, vec![permission.to_string()]);
        let expected = match name {
            START_TOOL => {
                json!({"type":"object","additionalProperties":false,"required":["command"],"properties":{"command":{"type":"string"},"working_directory":{"type":"string"},"timeout_seconds":{"type":"integer"}}})
            }
            LIST_TOOL => json!({"type":"object","additionalProperties":false,"properties":{}}),
            _ => {
                json!({"type":"object","additionalProperties":false,"required":["id"],"properties":{"id":{"type":"string"}}})
            }
        };
        assert_eq!(tool.info.parameters, expected);
        assert!(tool.executor.execute(json!({})).await.is_err());
    }
    assert_eq!(h.ext.id(), "crabber-extensions/background-jobs");
    assert_eq!(h.ext.version(), env!("CARGO_PKG_VERSION"));
    assert_eq!(h.ext.config_hash().len(), 64);
    drop(plan);
    h.close().await;
}
#[test]
fn worst_case_helpers_cover_caps_and_saturate_safely() {
    let mut l = limits();
    l.max_tracked = 256;
    l.max_output_bytes_per_stream = 1048576;
    assert_eq!(l.worst_case_retained_bytes(), 512 * 1024 * 1024);
    assert!(l.worst_case_status_bytes() >= 12 * 1048576);
    assert!(l.worst_case_list_bytes() >= 256 * 200);
    l.max_tracked = usize::MAX;
    l.max_output_bytes_per_stream = usize::MAX;
    assert_eq!(l.worst_case_retained_bytes(), usize::MAX);
    assert_eq!(l.worst_case_status_bytes(), usize::MAX);
    assert_eq!(l.worst_case_list_bytes(), usize::MAX);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_collision_rolls_back_without_closing_existing_manager() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    // Registry rolls back the failed registrar, without shutting down an
    // extension whose install succeeded. Reusing this instance remains safe.
    assert!(matches!(
        h.registry.mount(h.ext.clone(), Scope::Global).await,
        Err(ExtensionError::ToolCollision(_))
    ));
    let plan = h.registry.acquire(&SessionId::from("session"));
    assert_eq!(plan.tools.len(), 4);
    for tool in &plan.tools {
        assert!(Arc::ptr_eq(&tool.executor, &h.tools[&tool.info.name]));
    }
    drop(plan);
    let job = h.start("exit 0").await;
    assert_eq!(
        h.terminal(job["id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn session_mounts_are_visible_only_to_their_session() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let first = Arc::new(BackgroundJobs::new(options(dir.path())).unwrap());
    let second = Arc::new(BackgroundJobs::new(options(dir.path())).unwrap());
    let a = registry
        .mount(first.clone(), Scope::Session(SessionId::from("a")))
        .await
        .unwrap();
    assert_eq!(registry.acquire(&SessionId::from("a")).tools.len(), 4);
    assert!(registry.acquire(&SessionId::from("b")).tools.is_empty());
    let b = registry
        .mount(second.clone(), Scope::Session(SessionId::from("b")))
        .await
        .unwrap();
    assert_eq!(registry.acquire(&SessionId::from("b")).tools.len(), 4);
    a.close().await.unwrap();
    assert!(registry.acquire(&SessionId::from("a")).tools.is_empty());
    assert_eq!(registry.acquire(&SessionId::from("b")).tools.len(), 4);
    b.close().await.unwrap();
    assert_eq!(first.live_jobs() + second.live_jobs(), 0);
}
