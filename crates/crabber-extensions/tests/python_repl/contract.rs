use super::*;
#[tokio::test(flavor = "multi_thread")]
async fn registered_contract_is_exact() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let plan = h.registry.acquire(&SessionId::from("session"));
    assert_eq!(plan.tools.len(), 2);
    for (name, permission, retry) in [
        (EXECUTE_TOOL, PERMISSION_EXECUTE, false),
        (CLEAR_TOOL, PERMISSION_MANAGE, false),
    ] {
        let tool = plan.tools.iter().find(|t| t.info.name == name).unwrap();
        assert_eq!(
            tool.info.description,
            if name == EXECUTE_TOOL {
                "Execute Python in state scoped to the durable session and workspace. Python has host-user authority and results are bounded. Child-process output is discarded unless captured, for example with subprocess.run(capture_output=True)."
            } else {
                "Discard live Python interpreter state for the durable session and workspace without recreating it."
            }
        );
        assert_eq!(tool.info.retry_safe, retry);
        assert_eq!(tool.info.required_permissions, vec![permission.to_string()]);
        let expected = if name == EXECUTE_TOOL {
            json!({"type":"object","additionalProperties":false,"required":["code"],"properties":{"code":{"type":"string"},"timeout_seconds":{"type":"integer","minimum":0,"maximum":options(h.dir.path()).limits.max_timeout.as_secs(),"description":"Whole seconds; 0 or omitted uses the host default. Queue wait and runner startup are additional."}}})
        } else {
            json!({"type":"object","additionalProperties":false,"properties":{}})
        };
        assert_eq!(tool.info.parameters, expected);
        assert!(tool.executor.execute(json!({})).await.is_err());
    }
    assert_eq!(h.ext.id(), "crabber-extensions/python-repl");
    assert_eq!(h.ext.version(), env!("CARGO_PKG_VERSION"));
    assert_eq!(
        h.ext.config_hash(),
        PythonRepl::new(options(h.dir.path()))
            .unwrap()
            .config_hash()
    );
    drop(plan);
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn failed_collision_rolls_back_without_closing_existing_manager() {
    if !python_available() {
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
    assert_eq!(plan.tools.len(), 2);
    for tool in &plan.tools {
        assert!(Arc::ptr_eq(&tool.executor, &h.tools[&tool.info.name]));
    }
    drop(plan);
    assert_eq!(
        h.execute(EXECUTE_TOOL, json!({"code":record_code("42")}))
            .await
            .unwrap()["result"]["text"],
        "42"
    );
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn session_mounts_are_visible_only_to_their_session() {
    if !python_available() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("temporary")).unwrap();
    let registry = Registry::new();
    let first = Arc::new(PythonRepl::new(options(dir.path())).unwrap());
    let second = Arc::new(PythonRepl::new(options(dir.path())).unwrap());
    let a = registry
        .mount(first.clone(), Scope::Session(SessionId::from("a")))
        .await
        .unwrap();
    assert_eq!(registry.acquire(&SessionId::from("a")).tools.len(), 2);
    assert!(registry.acquire(&SessionId::from("b")).tools.is_empty());
    let b = registry
        .mount(second.clone(), Scope::Session(SessionId::from("b")))
        .await
        .unwrap();
    assert_eq!(registry.acquire(&SessionId::from("b")).tools.len(), 2);
    a.close().await.unwrap();
    assert!(registry.acquire(&SessionId::from("a")).tools.is_empty());
    assert_eq!(registry.acquire(&SessionId::from("b")).tools.len(), 2);
    b.close().await.unwrap();
    assert_eq!(first.live_runners() + second.live_runners(), 0);
}
