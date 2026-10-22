use super::*;
use crabber::extension::{Registrar, Registry, ToolOutcomeClass, TransformOutput};
#[tokio::test]
async fn registration_shape_and_repeated_mounts_are_accepted() {
    if !shell_available() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let o = options(dir.path(), "exit 0");
    let ext = Arc::new(RtkReducer::new(o.clone()).unwrap());
    assert_eq!(ext.id(), "crabber-extensions/rtk-reducer");
    assert_eq!(ext.version(), env!("CARGO_PKG_VERSION"));
    assert_eq!(ext.config_hash(), RtkReducer::new(o).unwrap().config_hash());
    let registry = Registry::new();
    let a = registry.mount(ext.clone(), Scope::Global).await.unwrap();
    let b = registry.mount(ext.clone(), Scope::Global).await.unwrap();
    a.close().await.unwrap();
    b.close().await.unwrap();
    assert!(ext.stats().closed);
}
struct MarkError;

#[tokio::test(flavor = "multi_thread")]
async fn stats_snapshot_counts_accepted_no_gain_and_denied_calls() {
    if !shell_available() {
        return;
    }
    let mut j = Journey::new(
        "/bin/cat >/dev/null; printf R:x",
        json!("x".repeat(300)),
        "fixture_text",
        json!({}),
        |_| {},
    );
    j.provider = Arc::new(FakeProvider::scripted(vec![
        call("fixture_text", json!({})),
        done(),
        call("fixture_text", json!({})),
        done(),
        call("fixture_text", json!({})),
        done(),
    ]));
    j.agent = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(j.store.clone())
        .provider(j.provider.clone())
        .config(agent_config(j.dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(tool("fixture_text", j.executor.clone()))
        .shared_extension(j.ext.clone(), Scope::Global)
        .build()
        .unwrap();
    for (value, expected) in [
        (
            json!("x".repeat(300)),
            json!(marker(Filter::Log, 300) + "R:x"),
        ),
        (json!("short"), json!("short")),
    ] {
        *j.executor.value.lock().unwrap() = value;
        let session = j.run().await;
        let records = snapshot_tools(&j.store, &session).await;
        observe::assert_three_surfaces(&j.store, &session, &j.provider, &records[0].id, &expected)
            .await;
    }
    let denied = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(j.store.clone())
        .provider(j.provider.clone())
        .config(agent_config(j.dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Deny)))
        .shared_tool(tool("fixture_text", j.executor.clone()))
        .shared_extension(j.ext.clone(), Scope::Global)
        .build()
        .unwrap();
    let run = denied.prompt(None, "deny").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&j.store, &session).await;
    observe::assert_three_surfaces(
        &j.store,
        &session,
        &j.provider,
        &records[0].id,
        &json!("permission denied"),
    )
    .await;
    let stats = j.ext.stats();
    assert_eq!(
        (stats.reduced, stats.no_gain, stats.passed_through),
        (1, 1, 1)
    );
    denied.close_extensions().await.unwrap();
    j.close().await;
}
#[async_trait::async_trait]
impl Extension for MarkError {
    fn id(&self) -> &str {
        "fixture/mark-error"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixture/mark-error".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        r.on_result_transform(
            -1,
            "mark-error",
            Arc::new(|_, value| Box::pin(async move { Ok(TransformOutput::marked_error(value)) })),
        );
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn error_classes_and_prior_mark_error_never_spawn() {
    if !shell_available() {
        return;
    }
    for mode in [
        "deny",
        "ask",
        "execution",
        "unknown",
        "prepare",
        "mark-error",
    ] {
        let mut j = Journey::new(
            "/bin/cat >/dev/null; printf R:x",
            json!("x".repeat(300)),
            "fixture_text",
            json!({}),
            |_| {},
        );
        if mode == "execution" {
            j.executor = Arc::new(FixtureTool {
                value: Mutex::new(json!("x".repeat(300))),
                executions: AtomicUsize::new(0),
                fail: true,
            });
        }
        if mode == "unknown" {
            j.provider = Arc::new(FakeProvider::scripted(vec![
                call("unknown", json!({})),
                done(),
            ]));
        }
        if mode == "prepare" {
            j.provider = Arc::new(FakeProvider::scripted(vec![
                call("fixture_text", json!([])),
                done(),
            ]));
        }
        let decision = match mode {
            "deny" => PermissionDecision::Deny,
            "ask" => PermissionDecision::Ask,
            _ => PermissionDecision::Allow,
        };
        let mut builder = Agent::builder()
            .behavior_version("crabber-extensions-tests.v1")
            .store(j.store.clone())
            .provider(j.provider.clone())
            .config(agent_config(j.dir.path()))
            .policy(Arc::new(StaticPolicy::new(decision)))
            .shared_tool(tool("fixture_text", j.executor.clone()))
            .shared_extension(j.ext.clone(), Scope::Global)
            .shared_extension(
                Arc::new(observe::ProbeMount(j.probe.clone())),
                Scope::Global,
            );
        if mode == "mark-error" {
            builder = builder.shared_extension(Arc::new(MarkError), Scope::Global);
        }
        j.agent = builder.build().unwrap();
        let run = j.agent.prompt(None, "class fixture").await.unwrap();
        let session = run.session_id().clone();
        assert_eq!(
            run.done().await.unwrap().status,
            if mode == "ask" {
                RunStatus::Paused
            } else {
                RunStatus::Completed
            }
        );
        let records = snapshot_tools(&j.store, &session).await;
        if mode == "ask" {
            assert_eq!(records[0].status, crabber::core::ToolCallStatus::Pending);
            assert!(records[0].result.is_none());
            assert!(j.probe.classes.lock().unwrap().is_empty());
            assert!(j.argv().is_empty());
            assert_eq!(j.ext.stats().passed_through, 0);
            j.close().await;
            continue;
        }
        let expected_class = match mode {
            "deny" | "ask" => ToolOutcomeClass::PermissionDenied,
            "execution" => ToolOutcomeClass::ExecutionFailed,
            "unknown" => ToolOutcomeClass::UnknownTool,
            "prepare" => ToolOutcomeClass::PrepareFailed,
            _ => ToolOutcomeClass::Succeeded,
        };
        assert_eq!(j.probe.classes.lock().unwrap()[0].1, expected_class);
        assert_eq!(
            records[0].result.as_ref().unwrap().status,
            crabber::core::ToolResultStatus::Failed
        );
        observe::assert_three_surfaces(
            &j.store,
            &session,
            &j.provider,
            &records[0].id,
            &result_value(&records[0]),
        )
        .await;
        assert!(j.argv().is_empty());
        assert_eq!(j.ext.stats().passed_through, 1);
        j.close().await;
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn closed_instance_is_single_use_and_options_errors_are_sanitized() {
    if !shell_available() {
        return;
    }
    let mut j = Journey::new(
        "/bin/cat >/dev/null; printf R:x",
        json!("x".repeat(300)),
        "fixture_text",
        json!({}),
        |_| {},
    );
    // Agent extensions install lazily at first admission; explicitly mount
    // before closing to exercise an instance that was actually installed.
    let registry = Registry::new();
    registry.mount(j.ext.clone(), Scope::Global).await.unwrap();
    registry.close_all().await.unwrap();
    assert!(j.ext.stats().closed);
    j.agent.close_extensions().await.unwrap();
    j.agent = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(j.store.clone())
        .provider(j.provider.clone())
        .config(agent_config(j.dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(tool("fixture_text", j.executor.clone()))
        .shared_extension(j.ext.clone(), Scope::Global)
        .build()
        .unwrap();
    let session = j.run().await;
    let records = snapshot_tools(&j.store, &session).await;
    observe::assert_three_surfaces(
        &j.store,
        &session,
        &j.provider,
        &records[0].id,
        &json!("x".repeat(300)),
    )
    .await;
    assert!(j.argv().is_empty());
    assert!(j.ext.stats().closed);
    j.close().await;
    let mut o = options(j.dir.path(), "exit 0");
    o.executable_sha256 = "0".repeat(64);
    let error = RtkReducer::new(o.clone()).err().unwrap().to_string();
    assert!(error.ends_with("executable-digest-mismatch"));
    assert!(!error.contains(&o.executable_sha256));
    assert!(!error.contains(j.dir.path().to_str().unwrap()));
}
