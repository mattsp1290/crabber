use super::*;

#[derive(Clone)]
struct LedgerProvider {
    fake: FakeProvider,
    dir: PathBuf,
    gate: bool,
}
#[async_trait]
impl Resolver for LedgerProvider {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, crabber::providers::ProviderBindingError>
    {
        self.fake.binding_descriptor(selection)
    }

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
impl crabber::JsonToolExecutor for LedgerTool {
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
    let scripts = vec![
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
    ];
    let fake = FakeProvider::scripted(if dir.join("text-only").exists() {
        vec![vec![
            StreamDelta::TextDelta("original recovered output".into()),
            StreamDelta::Completed,
        ]]
    } else {
        scripts
    });
    Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .observer(capture)
        .provider(Arc::new(LedgerProvider {
            fake,
            dir: dir.to_owned(),
            gate,
        }))
        .config(config())
        .extension_factory(Arc::new(LedgerFactory(dir.to_owned())))
        .policy(Arc::new(crabber::StaticPolicy::new(
            crabber::PermissionDecision::Allow,
        )))
        .shared_tool(Arc::new(crabber::ToolDefinition {
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

struct LedgerFactory(PathBuf);
struct EmptyPreparation;
#[async_trait]
impl crabber::extension::ExtensionFactory for LedgerFactory {
    fn identity(&self) -> crabber::extension::ComponentIdentity {
        crabber::extension::ComponentIdentity {
            id: "process-preparation".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "v1".into()
    }
    fn scope(&self) -> crabber::extension::FactoryScope {
        crabber::extension::FactoryScope::Session
    }
    async fn create(
        &self,
        _: crabber::extension::FactoryContext,
    ) -> Result<Arc<dyn crabber::extension::Extension>, crabber::ExtensionError> {
        append(&self.0.join("ledger"), "factory");
        Ok(Arc::new(EmptyPreparation))
    }
}
#[async_trait]
impl crabber::extension::Extension for EmptyPreparation {
    fn id(&self) -> &'static str {
        "process-empty"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "v1".into()
    }
    async fn install(
        &self,
        _: &mut crabber::extension::Registrar,
    ) -> Result<(), crabber::ExtensionError> {
        Ok(())
    }
}
