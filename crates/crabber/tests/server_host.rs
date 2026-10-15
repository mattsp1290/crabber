#[path = "server_host/approval.rs"]
mod approval;
#[path = "server_host/factories.rs"]
mod factories;
#[path = "server_host/interaction_hooks.rs"]
mod interaction_hooks;
#[path = "server_host/management.rs"]
mod management;
use async_trait::async_trait;
use crabber::{
    Admission, AdmissionKey, AdmissionOptions, Agent, AgentConfig, ExecutionContext,
    ExtensionError, FakeProvider, InputFingerprint, JsonToolExecutor, PermissionDecision,
    RuntimeError, Scope, Selection, SessionContext, SessionId, StaticPolicy, StreamDelta,
    ToolDefinition,
    core::{CoreError, RunStatus, ToolCallId, ToolInfo},
    extension::{Extension, Registrar, ToolContext},
    session::{MemoryStore, Store},
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn selection(model: &str) -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: model.into(),
    }
}
fn context(session: &str, workspace: &str, directory: &str, model: &str) -> ExecutionContext {
    ExecutionContext::new(
        SessionContext::new(session.into(), workspace, directory),
        selection(model),
    )
}
fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("host-input").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}
fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
fn call(id: &str) -> Vec<StreamDelta> {
    vec![
        StreamDelta::ToolCallStart {
            call_id: ToolCallId::from(id),
            name: "identity".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: ToolCallId::from(id),
            text: "{\"directory\":\"forged\"}".into(),
        },
        StreamDelta::ToolCallDone {
            call_id: ToolCallId::from(id),
        },
        StreamDelta::Completed,
    ]
}

struct IdentityTool {
    observed: Arc<Mutex<Vec<SessionContext>>>,
    together: Arc<tokio::sync::Barrier>,
}
#[async_trait]
impl JsonToolExecutor for IdentityTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("runtime must provide contextual execution")
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let identity = SessionContext::new(
            context.session_id.clone(),
            context.workspace().workspace_id().unwrap_or_default(),
            context.workspace().directory().unwrap_or_default(),
        );
        self.observed.lock().unwrap().push(identity);
        self.together.wait().await;
        Ok(json!({"observed":true}))
    }
}
struct SharedExtension {
    installs: Arc<AtomicUsize>,
    observed: Arc<Mutex<Vec<SessionContext>>>,
}
#[async_trait]
impl Extension for SharedExtension {
    fn id(&self) -> &'static str {
        "host-fixture"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "shared-fixture-v1".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        self.installs.fetch_add(1, Ordering::SeqCst);
        registrar.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "identity".into(),
                description: "Read runtime routing identity".into(),
                parameters: json!({"type":"object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(IdentityTool {
                observed: self.observed.clone(),
                together: Arc::new(tokio::sync::Barrier::new(2)),
            }),
        }));
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_configured_agent_admits_two_explicit_workspaces_without_mutating_defaults() {
    let cwd = std::env::current_dir().unwrap();
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        call("a"),
        call("b"),
        done(),
        done(),
        done(),
    ]));
    let installs = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(vec![]));
    let agent = Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .provider(provider.clone())
        .config(AgentConfig::new(selection("configured")))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(
            Arc::new(SharedExtension {
                installs: installs.clone(),
                observed: observed.clone(),
            }),
            Scope::Global,
        )
        .build()
        .unwrap();
    let (left_ready, right_ready) = tokio::join!(agent.ready(), agent.ready());
    left_ready.unwrap();
    right_ready.unwrap();
    assert_eq!(installs.load(Ordering::SeqCst), 1);
    let a = context("a", "workspace-a", "/admitted/a", "model-a");
    let b = context("b", "workspace-b", "/admitted/b", "model-b");
    let (left, right) = tokio::join!(
        agent.prompt_keyed_in(&a, "left", options(), None),
        agent.prompt_keyed_in(&b, "right", options(), None)
    );
    let Admission::Started { handle: left, .. } = left.unwrap() else {
        panic!("new left session")
    };
    let Admission::Started { handle: right, .. } = right.unwrap() else {
        panic!("new right session")
    };
    let (left, right) = tokio::join!(left.done(), right.done());
    assert_eq!(left.unwrap().status, RunStatus::Completed);
    assert_eq!(right.unwrap().status, RunStatus::Completed);
    let observations = observed.lock().unwrap().clone();
    assert_eq!(observations.len(), 2);
    assert!(observations.contains(a.session()));
    assert!(observations.contains(b.session()));
    for execution in [&a, &b] {
        let session = store
            .get_session(execution.session().session_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(SessionContext::from(&session), *execution.session());
        let requests: Vec<_> = provider
            .requests()
            .into_iter()
            .filter(|r| &r.identity.session_id == execution.session().session_id())
            .collect();
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|r| &r.selection == execution.selection())
        );
        assert!(
            requests
                .iter()
                .all(|r| r.tools.iter().any(|tool| tool.name == "identity"))
        );
    }
    let ordinary = agent
        .prompt(None, "configured defaults")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let persisted = store
        .get_session(&ordinary.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.workspace_id, "default");
    assert_eq!(persisted.directory, ".");
    assert_eq!(
        provider.requests().last().unwrap().selection,
        selection("configured")
    );
    assert_eq!(std::env::current_dir().unwrap(), cwd);
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn explicit_keyed_context_replays_after_defaults_change_and_rejects_drift_before_mounting() {
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![done()]));
    let agent = Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .provider(provider.clone())
        .config(AgentConfig::new(selection("unused-default")))
        .build()
        .unwrap();
    let admitted = context("keyed", "workspace", "/admitted/root", "chosen");
    let first = agent
        .prompt_keyed_in(&admitted, "original", options(), None)
        .await
        .unwrap();
    let receipt = first.receipt().clone();
    let Admission::Started { handle, .. } = first else {
        panic!("fresh admission required")
    };
    handle.done().await.unwrap();
    let replacement = Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .provider(provider.clone())
        .config(AgentConfig::new(selection("different-default")))
        .build()
        .unwrap();
    assert!(
        matches!(replacement.recover_admission_in(&admitted, "original", options(), None).await.unwrap(), Admission::Replayed(r) if r == receipt)
    );
    let changed_model = ExecutionContext::new(admitted.session().clone(), selection("changed"));
    assert!(matches!(
        replacement
            .prompt_keyed_in(&changed_model, "original", options(), None)
            .await,
        Err(RuntimeError::Store(CoreError::AdmissionConflict))
    ));
    let installs = Arc::new(AtomicUsize::new(0));
    let drifted = context("keyed", "workspace", "/admitted/root/", "chosen");
    let guarded = Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .provider(provider.clone())
        .config(AgentConfig::new(selection("configured")))
        .shared_extension(
            Arc::new(SharedExtension {
                installs: installs.clone(),
                observed: Arc::new(Mutex::new(vec![])),
            }),
            Scope::Global,
        )
        .build()
        .unwrap();
    assert!(matches!(
        guarded.prompt_in(&drifted, "drift", None).await,
        Err(RuntimeError::Store(CoreError::SessionIdentityMismatch))
    ));
    assert_eq!(installs.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(
        store
            .get_session(&SessionId::from("keyed"))
            .await
            .unwrap()
            .unwrap()
            .directory,
        "/admitted/root"
    );
}

#[tokio::test]
async fn empty_workspace_context_stays_unavailable_and_close_without_extensions_stops_ingress() {
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![done(), done()]));
    let agent = Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .provider(provider.clone())
        .config(AgentConfig::new(selection("default")))
        .build()
        .unwrap();
    let unavailable = context("empty", "", "", "chosen");
    let Admission::Started { handle, .. } = agent
        .prompt_keyed_in(&unavailable, "input", options(), None)
        .await
        .unwrap()
    else {
        panic!("new session")
    };
    handle.done().await.unwrap();
    agent
        .prompt_in(&unavailable, "next", None)
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let session = store
        .get_session(unavailable.session().session_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(SessionContext::from(&session), *unavailable.session());
    agent.close_extensions().await.unwrap();
    assert!(matches!(agent.ready().await, Err(RuntimeError::HostClosed)));
    assert!(matches!(
        agent.prompt_in(&unavailable, "late", None).await,
        Err(RuntimeError::HostClosed)
    ));
    assert_eq!(provider.requests().len(), 2);
}

#[path = "server_host/tool_resources.rs"]
mod tool_resources;
