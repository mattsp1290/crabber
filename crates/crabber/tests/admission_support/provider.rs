use super::*;

#[derive(Clone)]
struct LedgerProvider {
    fake: FakeProvider,
    dir: PathBuf,
    gate: bool,
}
#[async_trait]
impl Resolver for LedgerProvider {
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
impl Streamer for LedgerProvider {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        append(&self.dir.join("ledger"), "provider");
        if self.gate {
            fs::write(self.dir.join("executing"), "ready").unwrap();
            wait(&self.dir.join("execute-release")).await;
        }
        self.fake.stream(request).await
    }
}
struct LedgerTool(PathBuf, bool);
#[async_trait]
impl crabber::ToolExecutor for LedgerTool {
    async fn execute(
        &self,
        _: serde_json::Value,
    ) -> Result<serde_json::Value, crabber::ExtensionError> {
        append(&self.0.join("ledger"), "tool");
        if self.1 {
            fs::write(self.0.join("tool-running"), "ready").unwrap();
            wait(&self.0.join("tool-release")).await;
        }
        Ok(serde_json::json!({"ok":true}))
    }
}
pub(super) fn agent(
    store: Arc<dyn Store>,
    dir: &Path,
    gate: bool,
    capture: Arc<ContextCapture>,
    tool_loss: bool,
) -> Agent {
    let call = ToolCallId::new();
    let fake = FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call.clone(),
                name: "effect".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone { call_id: call },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ]);
    Agent::builder()
        .store(store)
        .observer(capture)
        .provider(Arc::new(LedgerProvider {
            fake,
            dir: dir.to_owned(),
            gate,
        }))
        .config(config())
        .policy(Arc::new(crabber::StaticPolicy::new(
            crabber::PermissionDecision::Allow,
        )))
        .tool(Arc::new(crabber::ToolDefinition {
            info: crabber::core::ToolInfo {
                name: "effect".into(),
                description: "controlled test effect".into(),
                parameters: serde_json::json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(LedgerTool(dir.to_owned(), tool_loss)),
        }))
        .build()
        .unwrap()
}
