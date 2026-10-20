//! Workspace filters and factory ownership are exercised through actual runs.
use super::{Error, call, done, selection};
use async_trait::async_trait;
use crabber::{
    Admission, Agent, AgentConfig, ExecutionContext, ExtensionError, ExtensionFactory,
    FactoryContext, FactoryScope, FakeProvider, JsonToolExecutor, PermissionDecision, RuntimeError,
    SessionContext, StaticPolicy, ToolDefinition,
    core::{AdmissionKey, AdmissionOptions, InputFingerprint, RunStatus, SessionId, ToolInfo},
    extension::{ComponentIdentity, Extension, Registrar, Scope, ToolContext},
    session::Store,
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Seen {
    created: AtomicUsize,
    closed: AtomicUsize,
    shared: Arc<AtomicUsize>,
    session_shared: Arc<AtomicUsize>,
    calls: Mutex<Vec<(String, usize)>>,
}
struct Factory(Arc<Seen>);
struct Instance {
    workspace: String,
    count: AtomicUsize,
    seen: Arc<Seen>,
}
struct Counter(Arc<Instance>);
#[async_trait]
impl JsonToolExecutor for Counter {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Rejected(
            "actual workspace context required",
        ))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        assert_eq!(
            context.workspace().workspace_id(),
            Some(self.0.workspace.as_str())
        );
        let count = self.0.count.fetch_add(1, Ordering::SeqCst) + 1;
        self.0
            .seen
            .calls
            .lock()
            .unwrap()
            .push((self.0.workspace.clone(), count));
        Ok(json!({"workspace":self.0.workspace,"count":count}))
    }
}
struct Mounted(Arc<Instance>);
#[async_trait]
impl Extension for Mounted {
    fn id(&self) -> &'static str {
        "probe-workspace-instance"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "workspace-count.v1".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(definition(
            "workspace_count",
            Arc::new(Counter(self.0.clone())),
        ));
        Ok(())
    }
}
#[async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "probe-workspace-factory".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "workspace-owner.v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Workspace
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        assert!(
            context.session_id().is_none(),
            "workspace owner must not borrow a session identity"
        );
        self.0.created.fetch_add(1, Ordering::SeqCst);
        let seen = self.0.clone();
        let closing = context.cleanup().closing();
        context.cleanup().spawn(async move {
            closing.await;
            seen.closed.fetch_add(1, Ordering::SeqCst);
        });
        Ok(Arc::new(Mounted(Arc::new(Instance {
            workspace: context.workspace().workspace_id().unwrap().into(),
            count: AtomicUsize::new(0),
            seen: self.0.clone(),
        }))))
    }
}
struct Shared(Arc<AtomicUsize>);
#[async_trait]
impl JsonToolExecutor for Shared {
    async fn execute(&self, value: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(value)
    }
}
struct SharedExtension {
    id: &'static str,
    name: &'static str,
    counter: Arc<AtomicUsize>,
}
#[async_trait]
impl Extension for SharedExtension {
    fn id(&self) -> &'static str {
        self.id
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        format!("scope-filter.v1:{}", self.name)
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(definition(
            self.name,
            Arc::new(Shared(self.counter.clone())),
        ));
        Ok(())
    }
}
fn definition(
    name: &str,
    executor: Arc<dyn crabber::extension::ToolExecutor>,
) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: name.into(),
            description: "workspace probe".into(),
            parameters: json!({"type":"object"}),
            retry_safe: false,
            required_permissions: vec![],
        },
        executor,
    })
}
pub(crate) async fn run(store: Arc<dyn Store>) -> Result<(), Error> {
    let seen = Arc::new(Seen::default());
    let selected_session = SessionId::new();
    let (host, provider) = configured(store, &seen, &selected_session)?;
    // Distinct sessions in A reuse ONE A factory; B owns its independent counter.
    for (workspace, text, session) in [
        ("scope-a", "A first", SessionId::new()),
        ("scope-b", "B first", SessionId::new()),
        ("scope-a", "A second", SessionId::new()),
        ("scope-a", "A shared", SessionId::new()),
        ("scope-a", "A selected session", selected_session),
    ] {
        admit(&host, workspace, text, session).await?;
    }
    assert_eq!(seen.created.load(Ordering::SeqCst), 2);
    assert_eq!(seen.shared.load(Ordering::SeqCst), 1);
    assert_eq!(seen.session_shared.load(Ordering::SeqCst), 1);
    assert_eq!(
        *seen.calls.lock().unwrap(),
        [
            ("scope-a".into(), 1),
            ("scope-b".into(), 1),
            ("scope-a".into(), 2)
        ]
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 10);
    for (index, request) in requests.iter().enumerate() {
        assert!(
            request
                .tools
                .iter()
                .any(|tool| tool.name == "workspace_count")
        );
        assert_eq!(
            request
                .tools
                .iter()
                .any(|tool| tool.name == "workspace_shared"),
            !(2..4).contains(&index)
        );
        assert_eq!(
            request
                .tools
                .iter()
                .any(|tool| tool.name == "session_shared"),
            index >= 8
        );
    }
    host.close_extensions().await?;
    assert_eq!(seen.closed.load(Ordering::SeqCst), 2);
    let closed = host.close(std::time::Duration::from_secs(30)).await;
    assert!(closed.local_drained());
    assert!(matches!(
        host.prompt(None, "fresh work after close").await,
        Err(RuntimeError::HostClosed)
    ));
    assert_eq!(provider.requests().len(), 10);
    assert_eq!(seen.created.load(Ordering::SeqCst), 2);
    assert_eq!(seen.shared.load(Ordering::SeqCst), 1);
    assert_eq!(seen.session_shared.load(Ordering::SeqCst), 1);
    Ok(())
}

async fn admit(host: &Agent, workspace: &str, text: &str, session: SessionId) -> Result<(), Error> {
    let context = ExecutionContext::new(
        SessionContext::new(session, workspace, "/workspace-probe"),
        selection(),
    );
    let options = AdmissionOptions {
        key: AdmissionKey::new(crabber::core::ControlId::new().0)?,
        fingerprint: InputFingerprint::new("a".repeat(64))?,
        behavior_fingerprint: host.behavior_fingerprint().clone(),
    };
    let Admission::Started { handle, .. } =
        host.prompt_keyed_in(&context, text, options, None).await?
    else {
        panic!("new workspace admission must start");
    };
    assert_eq!(handle.done().await?.status, RunStatus::Completed);
    Ok(())
}

fn configured(
    store: Arc<dyn Store>,
    seen: &Arc<Seen>,
    selected_session: &SessionId,
) -> Result<(Agent, FakeProvider), Error> {
    let provider = FakeProvider::scripted(vec![
        call("workspace_count"),
        done(),
        call("workspace_count"),
        done(),
        call("workspace_count"),
        done(),
        call("workspace_shared"),
        done(),
        call("session_shared"),
        done(),
    ]);
    let host = Agent::builder()
        .behavior_version("workspace-ownership-public-probe.v1")
        .store(store)
        .initialization_timeout(std::time::Duration::from_secs(30))
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(
            Arc::new(SharedExtension {
                id: "probe-workspace-filter",
                name: "workspace_shared",
                counter: seen.shared.clone(),
            }),
            Scope::Workspace("scope-a".into()),
        )
        .shared_extension(
            Arc::new(SharedExtension {
                id: "probe-session-filter",
                name: "session_shared",
                counter: seen.session_shared.clone(),
            }),
            Scope::SessionIn {
                workspace_id: "scope-a".into(),
                session_id: selected_session.clone(),
            },
        )
        .extension_factory(Arc::new(Factory(seen.clone())))
        .build()?;
    Ok((host, provider))
}
