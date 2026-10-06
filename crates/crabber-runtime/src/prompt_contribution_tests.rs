//! Runtime acceptance coverage for the per-attempt contributor contract.
mod failures;
mod lifecycle;
mod refresh;

use crate::{Orchestrator, PermissionDecision, Request, StaticPolicy};
use async_trait::async_trait;
use crabber_core::{RunStatus, ToolCallId, ToolInfo};
use crabber_extension::{
    Extension, ExtensionError, MiddlewareDescriptor, ModelAttemptContext, ModelRequestError, Point,
    PromptAttemptContext, PromptContributor, PromptSection, Registrar, Registry, Scope,
    SystemPromptMiddleware, ToolDefinition, ToolExecutor, WorkspaceContext, WorkspaceReadError,
    WorkspaceReadErrorKind, WorkspaceReader, WorkspaceReaderResolver,
};
use crabber_providers::{FakeProvider, ProviderError, ProviderErrorKind, Selection, StreamDelta};
use crabber_session::{MemoryStore, Store};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

struct TestExtension {
    id: String,
    install: Arc<dyn Fn(&mut Registrar) + Send + Sync>,
}
#[async_trait]
impl Extension for TestExtension {
    fn id(&self) -> &str {
        &self.id
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        String::new()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        (self.install)(r);
        Ok(())
    }
}
async fn mount(
    registry: &Registry,
    id: &str,
    scope: Scope,
    install: impl Fn(&mut Registrar) + Send + Sync + 'static,
) {
    registry
        .mount(
            Arc::new(TestExtension {
                id: id.into(),
                install: Arc::new(install),
            }),
            scope,
        )
        .await
        .unwrap();
}
fn request() -> Request {
    Request {
        session_id: None,
        workspace_id: "workspace".into(),
        directory: "/workspace".into(),
        title: "test".into(),
        text: "hello".into(),
        selection: Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
        system_prompt: Some("base".into()),
        max_output_tokens: None,
    }
}
fn text_script(text: &str) -> Vec<StreamDelta> {
    vec![StreamDelta::TextDelta(text.into()), StreamDelta::Completed]
}
fn call_script() -> Vec<StreamDelta> {
    let call_id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: "echo".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: "{}".into(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}
fn error_script(kind: ProviderErrorKind) -> Vec<StreamDelta> {
    let retryable = kind != ProviderErrorKind::ContextOverflow;
    vec![StreamDelta::Error(ProviderError {
        kind,
        message: "provider error".into(),
        retryable,
    })]
}
fn runtime(store: Arc<MemoryStore>, fake: &FakeProvider, registry: Registry) -> Orchestrator {
    Orchestrator::builder()
        .store(store)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(registry))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap()
}
fn static_prompt(r: &mut Registrar) {
    r.prompt(Arc::new(PromptSection {
        name: "static".into(),
        order: 0,
        text: "static".into(),
    }));
}
fn constant(value: &str) -> PromptContributor {
    let value = value.to_owned();
    Arc::new(move |_| {
        let value = value.clone();
        Box::pin(async move { Ok(Some(value)) })
    })
}
struct RewriteTool(PathBuf);
#[async_trait]
impl ToolExecutor for RewriteTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        std::fs::write(&self.0, "v2").unwrap();
        Ok(json!("done"))
    }
}
fn rewrite_tool(r: &mut Registrar, file: PathBuf) {
    r.tool(Arc::new(ToolDefinition {
        info: ToolInfo {
            name: "echo".into(),
            description: "rewrite".into(),
            parameters: json!({"type":"object"}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(RewriteTool(file)),
    }));
}
struct FileHarness {
    _directory: tempfile::TempDir,
    file: PathBuf,
    contexts: Arc<Mutex<Vec<PromptAttemptContext>>>,
    registry: Registry,
    store: Arc<MemoryStore>,
    fake: FakeProvider,
    runtime: Orchestrator,
}
impl FileHarness {
    async fn new(scripts: Vec<Vec<StreamDelta>>, tool: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("instructions");
        std::fs::write(&file, "v1").unwrap();
        let contexts = Arc::new(Mutex::new(Vec::new()));
        let registry = Registry::new();
        mount(&registry, "file", Scope::Global, {
            let file = file.clone();
            let contexts = contexts.clone();
            move |r| {
                static_prompt(r);
                let file_cb = file.clone();
                let contexts = contexts.clone();
                r.prompt_contributor(
                    0,
                    "instructions",
                    Arc::new(move |context| {
                        contexts.lock().unwrap().push(context);
                        let text = std::fs::read_to_string(&file_cb).unwrap();
                        Box::pin(async move { Ok(Some(text)) })
                    }),
                );
                let file_error = file.clone();
                r.on_transform(
                    ModelRequestError::ID,
                    0,
                    "rewrite-on-error",
                    Arc::new(move |input| {
                        std::fs::write(&file_error, "v2").unwrap();
                        Box::pin(async move { Ok(input) })
                    }),
                );
                if tool {
                    rewrite_tool(r, file.clone());
                }
            }
        })
        .await;
        let store = Arc::new(MemoryStore::new());
        let fake = FakeProvider::scripted(scripts);
        let runtime = runtime(store.clone(), &fake, registry.clone());
        Self {
            _directory: directory,
            file,
            contexts,
            registry,
            store,
            fake,
            runtime,
        }
    }
    async fn run(&self) {
        assert_eq!(
            self.runtime
                .start(request())
                .await
                .unwrap()
                .done()
                .await
                .unwrap()
                .status,
            RunStatus::Completed
        );
    }
}
