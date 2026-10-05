//! End-to-end acceptance coverage for the first-party typed model middleware.

use crate::{
    InterruptPolicy, Orchestrator, PermissionDecision, PermissionPolicy, Request, RuntimeError,
    StaticPolicy,
};
use async_trait::async_trait;
use crabber_core::{RunStatus, ToolCallId, ToolInfo, Usage};
use crabber_extension::{
    Extension, ExtensionError, MiddlewareDescriptor, ModelAttemptContext, ModelRequestError, Point,
    Registrar, Registry, Scope, SystemPromptMiddleware, ToolDefinition, ToolExecutor,
    WorkspaceContext, WorkspaceReadError, WorkspaceReadErrorKind, WorkspaceReader,
    WorkspaceReaderResolver,
};
use crabber_middleware::{AGENTS_MD_MAX_FILE_BYTES, AgentsMdConfig, AgentsMdExtension};
use crabber_providers::{
    FakeProvider, ModelRequest, ProviderError, ProviderErrorKind, Selection, StreamDelta,
};
#[cfg(feature = "postgres")]
use crabber_session::PostgresStore;
use crabber_session::{MemoryStore, Store};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

const OBSERVER_HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const FRAME_V1: &str = "## Workspace instructions: AGENTS.md\n<!-- crabber:agentsmd bytes=2 -->\nv1\n## End workspace instructions: AGENTS.md\n";
const FRAME_V2: &str = "## Workspace instructions: AGENTS.md\n<!-- crabber:agentsmd bytes=2 -->\nv2\n## End workspace instructions: AGENTS.md\n";

type FileReplies = HashMap<String, Result<Vec<u8>, WorkspaceReadErrorKind>>;

#[derive(Clone)]
struct TestExtension {
    id: &'static str,
    install: Arc<dyn Fn(&mut Registrar) + Send + Sync>,
}

#[async_trait]
impl Extension for TestExtension {
    fn id(&self) -> &str {
        self.id
    }

    fn version(&self) -> &'static str {
        "1"
    }

    fn config_hash(&self) -> String {
        String::new()
    }

    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        (self.install)(registrar);
        Ok(())
    }
}

async fn mount_test(
    registry: &Registry,
    id: &'static str,
    scope: Scope,
    install: impl Fn(&mut Registrar) + Send + Sync + 'static,
) -> crabber_extension::MountHandle {
    registry
        .mount(
            Arc::new(TestExtension {
                id,
                install: Arc::new(install),
            }),
            scope,
        )
        .await
        .unwrap()
}

fn request() -> Request {
    Request {
        session_id: None,
        workspace_id: "workspace".into(),
        directory: "/workspace".into(),
        title: "typed middleware test".into(),
        text: "hello".into(),
        selection: Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
        system_prompt: Some("base".into()),
    }
}

fn text_script(text: &str) -> Vec<StreamDelta> {
    vec![StreamDelta::TextDelta(text.into()), StreamDelta::Completed]
}

fn error_script(kind: ProviderErrorKind) -> Vec<StreamDelta> {
    vec![StreamDelta::Error(ProviderError {
        retryable: kind != ProviderErrorKind::ContextOverflow,
        kind,
        message: "scripted failure".into(),
    })]
}

fn call_script() -> Vec<StreamDelta> {
    let call_id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: "rewrite".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: "{}".into(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}

#[derive(Clone)]
struct WorkspaceBackend {
    files: Arc<Mutex<FileReplies>>,
    resolve_failure: Arc<Mutex<Option<WorkspaceReadErrorKind>>>,
    resolves: Arc<Mutex<Vec<WorkspaceContext>>>,
    reads: Arc<Mutex<Vec<(String, usize)>>>,
    blocked_path: Arc<Mutex<Option<String>>>,
    read_entered: Arc<Notify>,
}

impl WorkspaceBackend {
    fn with_file(path: &str, text: &str) -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::from([(
                path.into(),
                Ok(text.as_bytes().to_vec()),
            )]))),
            resolve_failure: Arc::new(Mutex::new(None)),
            resolves: Arc::new(Mutex::new(Vec::new())),
            reads: Arc::new(Mutex::new(Vec::new())),
            blocked_path: Arc::new(Mutex::new(None)),
            read_entered: Arc::new(Notify::new()),
        }
    }

    fn empty() -> Self {
        let backend = Self::with_file("unused", "unused");
        backend.files.lock().unwrap().clear();
        backend
    }

    fn set(&self, path: &str, text: &str) {
        self.files
            .lock()
            .unwrap()
            .insert(path.into(), Ok(text.as_bytes().to_vec()));
    }

    fn fail_read(&self, path: &str, kind: WorkspaceReadErrorKind) {
        self.files.lock().unwrap().insert(path.into(), Err(kind));
    }
}

#[async_trait]
impl WorkspaceReader for WorkspaceBackend {
    async fn read_limited(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, WorkspaceReadError> {
        self.reads.lock().unwrap().push((path.into(), max_bytes));
        if self.blocked_path.lock().unwrap().as_deref() == Some(path) {
            self.read_entered.notify_one();
            futures::future::pending().await
        } else {
            self.files
                .lock()
                .unwrap()
                .get(path)
                .cloned()
                .unwrap_or(Err(WorkspaceReadErrorKind::NotFound))
                .map_err(WorkspaceReadError::new)
        }
    }
}

#[async_trait]
impl WorkspaceReaderResolver for WorkspaceBackend {
    async fn resolve(
        &self,
        workspace: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        self.resolves.lock().unwrap().push(workspace.clone());
        if let Some(kind) = *self.resolve_failure.lock().unwrap() {
            Err(WorkspaceReadError::new(kind))
        } else {
            Ok(Arc::new(self.clone()))
        }
    }
}

fn runtime(
    store: Arc<MemoryStore>,
    fake: &FakeProvider,
    registry: Registry,
    backend: &WorkspaceBackend,
) -> Orchestrator {
    Orchestrator::builder()
        .store(store)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(registry))
        .workspace_reader_resolver(Arc::new(backend.clone()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap()
}

async fn mount_agents(
    registry: &Registry,
    scope: Scope,
    config: AgentsMdConfig,
    order: i32,
) -> crabber_extension::MountHandle {
    registry
        .mount(
            Arc::new(AgentsMdExtension::new(config).unwrap().with_order(order)),
            scope,
        )
        .await
        .unwrap()
}

async fn mount_retry_refresh(registry: &Registry, backend: &WorkspaceBackend) {
    mount_test(registry, "retry-refresh", Scope::Global, {
        let backend = backend.clone();
        move |registrar| {
            let backend = backend.clone();
            registrar.on_transform(
                ModelRequestError::ID,
                0,
                "refresh",
                Arc::new(move |input| {
                    backend.set("AGENTS.md", "v2");
                    Box::pin(async move { Ok(input) })
                }),
            );
        }
    })
    .await;
}

fn retry_scripts() -> Vec<Vec<StreamDelta>> {
    vec![
        error_script(ProviderErrorKind::RateLimited),
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Usage(Usage {
                input_tokens: 7,
                output_tokens: 3,
            }),
            StreamDelta::Completed,
        ],
    ]
}

fn assert_cross_run_request_semantics(baseline: &ModelRequest, with_middleware: &ModelRequest) {
    assert_eq!(with_middleware.selection, baseline.selection);
    assert_eq!(with_middleware.messages.len(), baseline.messages.len());
    for (baseline_message, middleware_message) in
        baseline.messages.iter().zip(&with_middleware.messages)
    {
        // Message/part IDs and creation times are also generated per run. Keep
        // messages intact and compare every semantic field plus each envelope's
        // relationship to its own independently allocated request identity.
        assert_eq!(middleware_message.role, baseline_message.role);
        assert_eq!(
            middleware_message.parent_id.is_some(),
            baseline_message.parent_id.is_some()
        );
        assert_eq!(middleware_message.parts.len(), baseline_message.parts.len());
        assert_eq!(
            middleware_message.session_id,
            with_middleware.identity.session_id
        );
        assert_eq!(
            middleware_message.run_id.as_ref(),
            Some(&with_middleware.identity.run_id)
        );
        assert_eq!(baseline_message.session_id, baseline.identity.session_id);
        assert_eq!(
            baseline_message.run_id.as_ref(),
            Some(&baseline.identity.run_id)
        );
        for (baseline_part, middleware_part) in
            baseline_message.parts.iter().zip(&middleware_message.parts)
        {
            assert_eq!(middleware_part.ordinal, baseline_part.ordinal);
            assert_eq!(middleware_part.kind, baseline_part.kind);
            assert_eq!(middleware_part.content, baseline_part.content);
            assert_eq!(middleware_part.message_id, middleware_message.id);
            assert_eq!(baseline_part.message_id, baseline_message.id);
        }
    }
    assert_eq!(with_middleware.tools, baseline.tools);
    assert_eq!(with_middleware.temperature, baseline.temperature);
    assert_eq!(with_middleware.max_tokens, baseline.max_tokens);
    assert_eq!(with_middleware.tool_choice, baseline.tool_choice);
}

fn assert_retry_changes_only_system(requests: &[ModelRequest]) {
    let mut first = requests[0].clone();
    let mut retry = requests[1].clone();
    first.system = None;
    retry.system = None;
    assert_eq!(retry, first);
}

struct RewriteTool(WorkspaceBackend);

#[async_trait]
impl ToolExecutor for RewriteTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.0.set("AGENTS.md", "v2");
        Ok(json!("rewritten"))
    }
}

fn install_tool(registrar: &mut Registrar, backend: WorkspaceBackend) {
    registrar.tool(Arc::new(ToolDefinition {
        info: ToolInfo {
            name: "rewrite".into(),
            description: "rewrite the in-memory workspace".into(),
            parameters: json!({"type": "object"}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(RewriteTool(backend)),
    }));
}

struct PausePolicy;

impl PermissionPolicy for PausePolicy {
    fn decide(&self, _: &ToolInfo, _: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }

    fn interrupt_policy(&self, _: &ToolInfo, _: &Value) -> InterruptPolicy {
        InterruptPolicy::Pause
    }
}

#[tokio::test]
async fn model_middleware_orders_base_static_and_typed_recipe_deterministically() {
    let backend = WorkspaceBackend::with_file("AGENTS.md", "v1");
    let registry = Registry::new();
    mount_test(&registry, "static", Scope::Global, |registrar| {
        registrar.prompt(Arc::new(crabber_extension::PromptSection {
            name: "static".into(),
            order: 99,
            text: "static".into(),
        }));
    })
    .await;
    mount_agents(&registry, Scope::Global, AgentsMdConfig::default(), -50).await;
    let fake = FakeProvider::scripted(vec![text_script("done")]);

    let result = runtime(Arc::new(MemoryStore::new()), &fake, registry, &backend)
        .start(request())
        .await
        .unwrap()
        .done()
        .await
        .unwrap();

    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(fake.requests().len(), 1, "filter must match a real request");
    assert_eq!(
        fake.requests()[0].system.as_deref(),
        Some(format!("base\nstatic\n{FRAME_V1}").as_str())
    );
    assert_eq!(backend.resolves.lock().unwrap().len(), 1);
    assert_eq!(
        *backend.reads.lock().unwrap(),
        vec![("AGENTS.md".into(), AGENTS_MD_MAX_FILE_BYTES)]
    );
}

#[tokio::test]
async fn model_middleware_scope_and_same_name_shadow_match_generic_contributors() {
    // Typed global is shadowed by a same-name generic session registration.
    let backend = WorkspaceBackend::with_file("AGENTS.md", "global");
    let registry = Registry::new();
    mount_agents(&registry, Scope::Global, AgentsMdConfig::default(), 0).await;
    let store = Arc::new(MemoryStore::new());
    let bootstrap = FakeProvider::scripted(vec![text_script("bootstrap")]);
    let mut initial = request();
    initial.system_prompt = None;
    let handle = runtime(store.clone(), &bootstrap, Registry::new(), &backend)
        .start(initial.clone())
        .await
        .unwrap();
    let session_id = handle.session_id().clone();
    handle.done().await.unwrap();
    mount_test(
        &registry,
        "session-generic",
        Scope::Session(session_id.clone()),
        |registrar| {
            registrar.prompt_contributor(
                0,
                "workspace-agents-md",
                Arc::new(|_| Box::pin(async { Ok(Some("generic-session".into())) })),
            );
        },
    )
    .await;
    initial.session_id = Some(session_id);
    let fake = FakeProvider::scripted(vec![text_script("global"), text_script("session")]);
    let rt = runtime(store, &fake, registry, &backend);
    let mut global_session = request();
    global_session.system_prompt = None;
    rt.start(global_session)
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    rt.start(initial).await.unwrap().done().await.unwrap();
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0]
            .system
            .as_deref()
            .unwrap()
            .contains("\nglobal\n")
    );
    assert_eq!(requests[1].system.as_deref(), Some("generic-session"));

    // Typed session likewise shadows a same-name generic global registration.
    let backend = WorkspaceBackend::with_file("AGENTS.md", "v1");
    let registry = Registry::new();
    mount_test(&registry, "global-generic", Scope::Global, |registrar| {
        registrar.prompt_contributor(
            0,
            "workspace-agents-md",
            Arc::new(|_| Box::pin(async { Ok(Some("generic-global".into())) })),
        );
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let bootstrap = FakeProvider::scripted(vec![text_script("bootstrap")]);
    let handle = runtime(store.clone(), &bootstrap, Registry::new(), &backend)
        .start(request())
        .await
        .unwrap();
    let session_id = handle.session_id().clone();
    handle.done().await.unwrap();
    mount_agents(
        &registry,
        Scope::Session(session_id.clone()),
        AgentsMdConfig::default(),
        0,
    )
    .await;
    let mut resumed_session = request();
    resumed_session.session_id = Some(session_id);
    let fake = FakeProvider::scripted(vec![text_script("done")]);
    runtime(store, &fake, registry, &backend)
        .start(resumed_session)
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(
        fake.requests()[0].system.as_deref(),
        Some(format!("base\n{FRAME_V1}").as_str())
    );
}

#[tokio::test]
async fn model_middleware_retry_reresolves_refreshes_and_changes_only_system_text() {
    let backend = WorkspaceBackend::with_file("AGENTS.md", "v1");
    let registry = Registry::new();
    mount_agents(&registry, Scope::Global, AgentsMdConfig::default(), 0).await;
    mount_retry_refresh(&registry, &backend).await;
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(retry_scripts());
    let result = runtime(store.clone(), &fake, registry, &backend)
        .start(request())
        .await
        .unwrap()
        .done()
        .await
        .unwrap();

    // Exercise the same request, provider scripts, and retry transform without
    // installing AgentsMdExtension. This is the true no-middleware baseline.
    let baseline_backend = WorkspaceBackend::with_file("AGENTS.md", "v1");
    let baseline_registry = Registry::new();
    mount_retry_refresh(&baseline_registry, &baseline_backend).await;
    let baseline_store = Arc::new(MemoryStore::new());
    let baseline_fake = FakeProvider::scripted(retry_scripts());
    let baseline_result = runtime(
        baseline_store.clone(),
        &baseline_fake,
        baseline_registry,
        &baseline_backend,
    )
    .start(request())
    .await
    .unwrap()
    .done()
    .await
    .unwrap();

    let requests = fake.requests();
    let baseline_requests = baseline_fake.requests();
    assert_eq!(requests.len(), 2, "two physical provider attempts required");
    assert_eq!(
        baseline_requests.len(),
        2,
        "the no-middleware baseline must make the same two attempts"
    );
    assert_eq!(
        requests[0].system.as_deref(),
        Some(format!("base\n{FRAME_V1}").as_str())
    );
    assert_eq!(
        requests[1].system.as_deref(),
        Some(format!("base\n{FRAME_V2}").as_str())
    );
    assert_eq!(baseline_requests[0].system.as_deref(), Some("base"));
    assert_eq!(baseline_requests[1].system.as_deref(), Some("base"));

    // Retry preserves the complete request identity within each independent run.
    // Cross-run UUIDs are independently allocated, so compare their relationship
    // rather than replacing the turn IDs to manufacture equality.
    assert_eq!(requests[0].identity, requests[1].identity);
    assert_eq!(baseline_requests[0].identity, baseline_requests[1].identity);
    assert_retry_changes_only_system(&requests);
    assert_retry_changes_only_system(&baseline_requests);
    for (baseline, with_middleware) in baseline_requests.iter().zip(&requests) {
        assert_cross_run_request_semantics(baseline, with_middleware);
    }

    assert_eq!(result.status, baseline_result.status);
    assert_eq!(result.usage, baseline_result.usage);
    assert_eq!(result.usage.input_tokens, 7);
    assert_eq!(result.usage.output_tokens, 3);
    for request in &requests {
        assert_eq!(result.run_id, request.identity.run_id);
        assert_eq!(result.session_id, request.identity.session_id);
    }
    for request in &baseline_requests {
        assert_eq!(baseline_result.run_id, request.identity.run_id);
        assert_eq!(baseline_result.session_id, request.identity.session_id);
    }
    assert_eq!(backend.resolves.lock().unwrap().len(), 2);
    assert_eq!(backend.reads.lock().unwrap().len(), 2);
    assert_eq!(baseline_backend.resolves.lock().unwrap().len(), 0);
    assert_eq!(baseline_backend.reads.lock().unwrap().len(), 0);
    assert_eq!(
        store.get_run(&result.run_id).await.unwrap().unwrap().status,
        result.status
    );
    assert_eq!(
        baseline_store
            .get_run(&baseline_result.run_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        baseline_result.status
    );
}

#[tokio::test]
async fn model_middleware_tool_turn_reresolves_rereads_and_refreshes() {
    let backend = WorkspaceBackend::with_file("AGENTS.md", "v1");
    let registry = Registry::new();
    mount_agents(&registry, Scope::Global, AgentsMdConfig::default(), 0).await;
    mount_test(&registry, "rewrite-tool", Scope::Global, {
        let backend = backend.clone();
        move |registrar| install_tool(registrar, backend.clone())
    })
    .await;
    let fake = FakeProvider::scripted(vec![call_script(), text_script("done")]);
    let result = runtime(Arc::new(MemoryStore::new()), &fake, registry, &backend)
        .start(request())
        .await
        .unwrap()
        .done()
        .await
        .unwrap();

    assert_eq!(result.status, RunStatus::Completed);
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].system.as_deref().unwrap().contains("\nv1\n"));
    assert!(requests[1].system.as_deref().unwrap().contains("\nv2\n"));
    assert_ne!(requests[0].identity.turn_id, requests[1].identity.turn_id);
    assert_eq!(backend.resolves.lock().unwrap().len(), 2);
    assert_eq!(backend.reads.lock().unwrap().len(), 2);
}

struct AttemptObserver(Arc<Mutex<Vec<(u32, bool)>>>);

#[async_trait]
impl SystemPromptMiddleware for AttemptObserver {
    async fn contribute(&self, context: ModelAttemptContext) -> Result<Option<String>, String> {
        self.0
            .lock()
            .unwrap()
            .push((context.attempt(), context.after_compaction()));
        Ok(None)
    }
}

#[tokio::test]
async fn model_middleware_bypasses_internal_compaction_then_refreshes_agent_turn() {
    let backend = WorkspaceBackend::with_file("AGENTS.md", "v1");
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let registry = Registry::new();
    mount_agents(&registry, Scope::Global, AgentsMdConfig::default(), 0).await;
    mount_test(&registry, "compaction-observer", Scope::Global, {
        let contexts = contexts.clone();
        move |registrar| {
            registrar.system_prompt_middleware(
                "attempt-observer",
                1,
                MiddlewareDescriptor::new("test-observer", "1", OBSERVER_HASH).unwrap(),
                Arc::new(AttemptObserver(contexts.clone())),
            );
        }
    })
    .await;
    mount_test(&registry, "compaction-refresh", Scope::Global, {
        let backend = backend.clone();
        move |registrar| {
            let backend = backend.clone();
            registrar.on_transform(
                ModelRequestError::ID,
                0,
                "refresh",
                Arc::new(move |input| {
                    backend.set("AGENTS.md", "v2");
                    Box::pin(async move { Ok(input) })
                }),
            );
        }
    })
    .await;
    let fake = FakeProvider::scripted(vec![
        error_script(ProviderErrorKind::ContextOverflow),
        text_script("summary"),
        text_script("done"),
    ]);
    let result = runtime(Arc::new(MemoryStore::new()), &fake, registry, &backend)
        .start(request())
        .await
        .unwrap()
        .done()
        .await
        .unwrap();

    assert_eq!(result.status, RunStatus::Completed);
    let requests = fake.requests();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].system.as_deref().unwrap().contains("\nv1\n"));
    // Internal compaction keeps its dedicated prompt and bypasses agent-turn middleware.
    assert_eq!(
        requests[1].system.as_deref(),
        Some(
            "Summarize this context for continuation. Preserve every standing instruction and unresolved task from the previous summary and new context."
        )
    );
    // The agent-turn retry recollects middleware and observes the changed workspace file.
    assert!(requests[2].system.as_deref().unwrap().contains("\nv2\n"));
    assert_eq!(*contexts.lock().unwrap(), vec![(1, false), (2, true)]);
    assert_eq!(backend.resolves.lock().unwrap().len(), 2);
    assert_eq!(backend.reads.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn model_middleware_optional_missing_file_preserves_baseline_system_bytes() {
    let backend = WorkspaceBackend::empty();
    let registry = Registry::new();
    mount_agents(&registry, Scope::Global, AgentsMdConfig::default(), 0).await;
    let fake = FakeProvider::scripted(vec![text_script("done")]);
    let baseline = "base\nwith trailing bytes\n\n";
    let mut input = request();
    input.system_prompt = Some(baseline.into());
    runtime(Arc::new(MemoryStore::new()), &fake, registry, &backend)
        .start(input)
        .await
        .unwrap()
        .done()
        .await
        .unwrap();

    assert_eq!(fake.requests().len(), 1);
    assert_eq!(fake.requests()[0].system.as_deref(), Some(baseline));
}

#[tokio::test]
async fn model_middleware_reader_and_resolver_failures_are_sanitized_before_provider() {
    for resolver_failure in [true, false] {
        let backend = WorkspaceBackend::with_file("AGENTS.md", "secret");
        if resolver_failure {
            *backend.resolve_failure.lock().unwrap() = Some(WorkspaceReadErrorKind::Denied);
        } else {
            backend.fail_read("AGENTS.md", WorkspaceReadErrorKind::Io);
        }
        let registry = Registry::new();
        mount_agents(&registry, Scope::Global, AgentsMdConfig::default(), 0).await;
        let fake = FakeProvider::scripted(vec![text_script("unused")]);
        let store = Arc::new(MemoryStore::new());
        let handle = runtime(store.clone(), &fake, registry, &backend)
            .start(request())
            .await
            .unwrap();
        let run_id = handle.run_id().clone();
        let error = handle.done().await.unwrap_err();

        assert!(matches!(error, RuntimeError::Extension(_)));
        assert_eq!(
            error.to_string(),
            "extension: prompt contribution failed: workspace-agents-md"
        );
        assert_eq!(fake.requests().len(), 0);
        assert_eq!(
            store
                .get_run(&run_id)
                .await
                .unwrap()
                .unwrap()
                .error
                .as_deref(),
            Some("extension: prompt contribution failed: workspace-agents-md")
        );
    }
}

#[tokio::test]
async fn model_middleware_cancellation_interrupts_blocked_read_and_skips_later_contribution() {
    let backend = WorkspaceBackend::with_file("AGENTS.md", "never returned");
    *backend.blocked_path.lock().unwrap() = Some("AGENTS.md".into());
    let later_calls = Arc::new(AtomicUsize::new(0));
    let registry = Registry::new();
    mount_agents(&registry, Scope::Global, AgentsMdConfig::default(), 0).await;
    mount_test(&registry, "later", Scope::Global, {
        let later_calls = later_calls.clone();
        move |registrar| {
            let later_calls = later_calls.clone();
            registrar.prompt_contributor(
                1,
                "later",
                Arc::new(move |_| {
                    later_calls.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Ok(Some("must not run".into())) })
                }),
            );
        }
    })
    .await;
    let fake = FakeProvider::scripted(vec![text_script("unused")]);
    let handle = runtime(Arc::new(MemoryStore::new()), &fake, registry, &backend)
        .start(request())
        .await
        .unwrap();
    backend.read_entered.notified().await;
    handle.interrupt();
    let result = tokio::time::timeout(Duration::from_millis(250), handle.done())
        .await
        .expect("blocked read must be cancelled promptly")
        .unwrap();

    assert_eq!(result.status, RunStatus::Interrupted);
    assert_eq!(later_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 0);
}

fn paused_runtime(
    registry: Registry,
    backend: &WorkspaceBackend,
    fake: &FakeProvider,
    store: Arc<dyn Store>,
) -> Orchestrator {
    Orchestrator::builder()
        .store(store)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(registry))
        .workspace_reader_resolver(Arc::new(backend.clone()))
        .policy(Arc::new(PausePolicy))
        .build()
        .unwrap()
}

#[cfg(feature = "postgres")]
async fn paused_registry(config: AgentsMdConfig) -> Registry {
    let registry = Registry::new();
    mount_agents(&registry, Scope::Global, config, 0).await;
    mount_test(&registry, "pause-tool", Scope::Global, |registrar| {
        install_tool(registrar, WorkspaceBackend::empty());
    })
    .await;
    registry
}

#[tokio::test]
async fn model_middleware_resume_rejects_every_sealed_descriptor_change_without_advancing() {
    enum Change {
        Files,
        FileOrder,
        Required,
        RegistrationOrder,
    }
    for change in [
        Change::Files,
        Change::FileOrder,
        Change::Required,
        Change::RegistrationOrder,
    ] {
        let backend = WorkspaceBackend::with_file("a", "a");
        backend.set("b", "b");
        let registry = Registry::new();
        let original = AgentsMdConfig::new(vec!["a".into(), "b".into()], false).unwrap();
        let agents = mount_agents(&registry, Scope::Global, original, 0).await;
        mount_test(&registry, "pause-tool", Scope::Global, |registrar| {
            install_tool(registrar, WorkspaceBackend::empty());
        })
        .await;
        let store = Arc::new(MemoryStore::new());
        let fake = FakeProvider::scripted(vec![call_script(), text_script("unused")]);
        let runtime = paused_runtime(registry.clone(), &backend, &fake, store.clone());
        let handle = runtime.start(request()).await.unwrap();
        let run_id = handle.run_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        let before = store.get_run(&run_id).await.unwrap().unwrap();
        agents.close().await.unwrap();
        let (files, required, order) = match change {
            Change::Files => (vec!["a".into()], false, 0),
            Change::FileOrder => (vec!["b".into(), "a".into()], false, 0),
            Change::Required => (vec!["a".into(), "b".into()], true, 0),
            Change::RegistrationOrder => (vec!["a".into(), "b".into()], false, 1),
        };
        mount_agents(
            &registry,
            Scope::Global,
            AgentsMdConfig::new(files, required).unwrap(),
            order,
        )
        .await;

        assert!(matches!(
            runtime.resume(&run_id).await,
            Err(RuntimeError::PlanChanged)
        ));
        assert_eq!(store.get_run(&run_id).await.unwrap().unwrap(), before);
        assert_eq!(fake.requests().len(), 1);
    }
}

#[tokio::test]
async fn model_middleware_resume_keeps_plan_seal_but_refreshes_host_content() {
    let backend_a = WorkspaceBackend::with_file("AGENTS.md", "v1");
    let backend_b = WorkspaceBackend::with_file("AGENTS.md", "v2");
    let registry = Registry::new();
    mount_agents(&registry, Scope::Global, AgentsMdConfig::default(), 0).await;
    mount_test(&registry, "pause-tool", Scope::Global, |registrar| {
        install_tool(registrar, WorkspaceBackend::empty());
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![call_script(), text_script("done")]);
    let runtime_a = paused_runtime(registry.clone(), &backend_a, &fake, store.clone());
    let handle = runtime_a.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);

    // A distinct resolver may represent a new authorized host root or policy.
    // Resolver instance, backing root/policy, and content are intentionally outside
    // the plan fingerprint; only the middleware config/descriptor is sealed.
    let runtime_b = paused_runtime(registry, &backend_b, &fake, store);
    let result = runtime_b.resume(&run_id).await.unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].system.as_deref(),
        Some(format!("base\n{FRAME_V1}").as_str())
    );
    assert_eq!(
        requests[1].system.as_deref(),
        Some(format!("base\n{FRAME_V2}").as_str())
    );
    let resolves_a = backend_a.resolves.lock().unwrap();
    let resolves_b = backend_b.resolves.lock().unwrap();
    assert_eq!(resolves_a.len(), 1);
    assert_eq!(resolves_b.len(), 1);
    assert_eq!(resolves_a[0], resolves_b[0]);
    assert_eq!(
        resolves_a[0],
        WorkspaceContext::from_persisted("workspace", "/workspace")
    );
    let expected_reads = vec![("AGENTS.md".into(), AGENTS_MD_MAX_FILE_BYTES)];
    assert_eq!(*backend_a.reads.lock().unwrap(), expected_reads);
    assert_eq!(*backend_b.reads.lock().unwrap(), expected_reads);
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn model_middleware_postgres_resume_reconstructs_plan_and_rejects_changes_before_claim() {
    let url = match std::env::var("CRABBER_TEST_POSTGRES_URL") {
        Ok(url) => url,
        Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_))
            if std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref() == Ok("1") =>
        {
            panic!("CRABBER_TEST_POSTGRES_URL is required")
        }
        Err(_error) => {
            eprintln!("skipping PostgreSQL test: CRABBER_TEST_POSTGRES_URL is unset");
            return;
        }
    };
    PostgresStore::migrate(&url).await.unwrap();

    // Persist a paused run, then let every runtime-owned object used to admit it
    // fall out of scope. Resume uses a new pool, registry, extension instance,
    // resolver instance, and orchestrator.
    let run_id = {
        let admission_provider = FakeProvider::scripted(vec![call_script()]);
        let store = Arc::new(PostgresStore::connect(&url).await.unwrap());
        let registry = paused_registry(AgentsMdConfig::default()).await;
        let backend = WorkspaceBackend::with_file("AGENTS.md", "v1");
        let runtime = paused_runtime(registry, &backend, &admission_provider, store);
        let handle = runtime.start(request()).await.unwrap();
        let run_id = handle.run_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        assert_eq!(admission_provider.requests().len(), 1);
        run_id
    };
    let resume_provider = FakeProvider::scripted(vec![text_script("done")]);
    let store = Arc::new(PostgresStore::connect(&url).await.unwrap());
    let registry = paused_registry(AgentsMdConfig::default()).await;
    let backend = WorkspaceBackend::with_file("AGENTS.md", "v2");
    let runtime = paused_runtime(registry, &backend, &resume_provider, store);
    assert_eq!(
        runtime.resume(&run_id).await.unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(resume_provider.requests().len(), 1);
    assert!(
        resume_provider.requests()[0]
            .system
            .as_deref()
            .unwrap()
            .contains("\nv2\n")
    );

    // A second durable run proves a changed reconstruction is refused before
    // claiming the row or dispatching another provider request.
    let changed_run_id = {
        let admission_provider = FakeProvider::scripted(vec![call_script()]);
        let store = Arc::new(PostgresStore::connect(&url).await.unwrap());
        let registry = paused_registry(AgentsMdConfig::default()).await;
        let backend = WorkspaceBackend::with_file("AGENTS.md", "original");
        let runtime = paused_runtime(registry, &backend, &admission_provider, store);
        let handle = runtime.start(request()).await.unwrap();
        let run_id = handle.run_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        assert_eq!(admission_provider.requests().len(), 1);
        run_id
    };
    let resume_provider = FakeProvider::scripted(vec![text_script("must-not-run")]);
    let changed_store = Arc::new(PostgresStore::connect(&url).await.unwrap());
    let before = changed_store
        .get_run(&changed_run_id)
        .await
        .unwrap()
        .unwrap();
    let changed_registry =
        paused_registry(AgentsMdConfig::new(vec!["DIFFERENT.md".into()], false).unwrap()).await;
    let changed_backend = WorkspaceBackend::with_file("DIFFERENT.md", "changed");
    let changed_runtime = paused_runtime(
        changed_registry,
        &changed_backend,
        &resume_provider,
        changed_store.clone(),
    );
    assert!(matches!(
        changed_runtime.resume(&changed_run_id).await,
        Err(RuntimeError::PlanChanged)
    ));
    assert_eq!(
        changed_store
            .get_run(&changed_run_id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(resume_provider.requests().len(), 0);
    assert_eq!(changed_backend.resolves.lock().unwrap().len(), 0);
}

// The forged-context authority boundary is intentionally covered by
// `prompt_contribution_tests::failures::model_middleware_workspace_resolver_denies_forged_context_without_host_dispatch`.
// These tests additionally prove that typed middleware changes only `system` on a physical request.
