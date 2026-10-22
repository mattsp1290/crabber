use super::*;
use crabber::JsonToolExecutor;

struct ChildWait(Arc<tokio::sync::Notify>);
#[async_trait::async_trait]
impl JsonToolExecutor for ChildWait {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.0.notify_one();
        std::future::pending().await
    }
}
struct ChildTool(Arc<tokio::sync::Notify>);
#[async_trait::async_trait]
impl Extension for ChildTool {
    fn id(&self) -> &str {
        "fixture/child-wait"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixture-v1".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "child_wait".into(),
                description: "Synthetic wait".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(ChildWait(self.0.clone())),
        }));
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn interrupted_run_leaves_the_job_owned() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let entered = Arc::new(tokio::sync::Notify::new());
    let a = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![calls(&[
            (START_TOOL, json!({"command":command("sleep 60")})),
            ("child_wait", json!({})),
        ])])))
        .config(agent_config(h.dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(ExecutionMode::Parallel { max: 2 })
        .shared_extension(h.ext.clone(), Scope::Global)
        .shared_extension(Arc::new(ChildTool(entered.clone())), Scope::Global)
        .build()
        .unwrap();
    let run = a.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    wait_pgids(h.dir.path(), 1).await;
    timeout(Duration::from_secs(5), async {
        loop {
            if snapshot_tools(&store, &session)
                .await
                .iter()
                .any(|r| r.name == START_TOOL && r.status == ToolCallStatus::Completed)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    run.interrupt();
    assert!(matches!(
        run.done().await,
        Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
    ));
    assert_eq!(h.ext.live_jobs(), 1);
    assert!(!group_members(pgids(h.dir.path())[0]).is_empty());
    a.close_extensions().await.unwrap();
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_starts_respect_running_capacity() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        calls(&[
            (START_TOOL, json!({"command":command("sleep 60")})),
            (START_TOOL, json!({"command":command("sleep 60")})),
            (START_TOOL, json!({"command":command("sleep 60")})),
        ]),
        done(),
    ]));
    let a = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store.clone())
        .provider(provider)
        .config(agent_config(h.dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(ExecutionMode::Parallel { max: 3 })
        .shared_extension(h.ext.clone(), Scope::Global)
        .build()
        .unwrap();
    let run = a.prompt(None, "start three").await.unwrap();
    let session = run.session_id().clone();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(
        records
            .iter()
            .filter(|r| r.status == ToolCallStatus::Completed)
            .count(),
        2
    );
    let failed = records
        .iter()
        .find(|r| r.status == ToolCallStatus::Failed)
        .unwrap();
    assert!(
        result_value(failed)
            .as_str()
            .unwrap()
            .ends_with("capacity-exhausted")
    );
    assert_eq!(h.ext.live_jobs(), 2);
    wait_pgids(h.dir.path(), 2).await;
    a.close_extensions().await.unwrap();
    h.close().await;
}
