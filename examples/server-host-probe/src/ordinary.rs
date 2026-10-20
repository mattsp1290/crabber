//! One configured host, two immutable contexts, real session-scoped instances.
use super::{Error, call, done, selection};
use async_trait::async_trait;
use crabber::{
    Admission, Agent, AgentConfig, ExecutionContext, ExtensionError, ExtensionFactory,
    FactoryContext, FactoryScope, JsonToolExecutor, PermissionDecision, SessionContext,
    ToolDefinition,
    core::{AdmissionKey, AdmissionOptions, InputFingerprint, RunStatus, SessionId, ToolInfo},
    extension::{ComponentIdentity, Extension, Registrar, ToolContext},
    runtime::PermissionPolicy,
    session::Store,
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
struct Policy(Arc<AtomicUsize>);
impl PermissionPolicy for Policy {
    fn decide(&self, tool: &ToolInfo, _: &Value) -> PermissionDecision {
        self.0.fetch_add(1, Ordering::SeqCst);
        if tool.name == "scoped_count" {
            PermissionDecision::Allow
        } else {
            PermissionDecision::Deny
        }
    }
}
struct Factory {
    created: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    observations: Arc<Mutex<Vec<(String, String, usize)>>>,
}
struct Instance {
    session: SessionId,
    workspace: String,
    count: AtomicUsize,
    observations: Arc<Mutex<Vec<(String, String, usize)>>>,
}
struct Counter(Arc<Instance>);
#[async_trait]
impl JsonToolExecutor for Counter {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Rejected("context required"))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        assert_eq!(context.session_id, self.0.session);
        assert_eq!(
            context.workspace().workspace_id(),
            Some(self.0.workspace.as_str())
        );
        let count = self.0.count.fetch_add(1, Ordering::SeqCst) + 1;
        self.0.observations.lock().unwrap().push((
            context.session_id.to_string(),
            self.0.workspace.clone(),
            count,
        ));
        Ok(json!({"session":context.session_id,"workspace":self.0.workspace,"count":count}))
    }
}
struct Mounted(Arc<Instance>);
#[async_trait]
impl Extension for Mounted {
    fn id(&self) -> &'static str {
        "probe-scoped"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "probe-scoped.v1".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "scoped_count".into(),
                description: "Count in this session".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(Counter(self.0.clone())),
        }));
        Ok(())
    }
}
#[async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "probe-factory".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "probe-factory.v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.created.fetch_add(1, Ordering::SeqCst);
        let closing = context.cleanup().closing();
        let closed = self.closed.clone();
        context.cleanup().spawn(async move {
            closing.await;
            closed.fetch_add(1, Ordering::SeqCst);
        });
        Ok(Arc::new(Mounted(Arc::new(Instance {
            session: context.session_id().unwrap().clone(),
            workspace: context.workspace().workspace_id().unwrap().into(),
            count: AtomicUsize::new(0),
            observations: self.observations.clone(),
        }))))
    }
}
pub(crate) async fn run(store: Arc<dyn Store>) -> Result<(usize, usize, SessionId), Error> {
    let created = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(Mutex::new(vec![]));
    let decisions = Arc::new(AtomicUsize::new(0));
    let provider = crabber::FakeProvider::scripted(vec![
        call("scoped_count"),
        done(),
        call("scoped_count"),
        done(),
    ]);
    let agent = Agent::builder()
        .behavior_version("server-host-probe.v1")
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(Policy(decisions.clone())))
        .extension_factory(Arc::new(Factory {
            created: created.clone(),
            closed: closed.clone(),
            observations: observations.clone(),
        }))
        .build()?;
    let mut originals = Vec::new();
    for (workspace, directory) in [
        ("workspace-a", "/admitted/a"),
        ("workspace-b", "/admitted/b"),
    ] {
        let context = ExecutionContext::new(
            SessionContext::new(SessionId::new(), workspace, directory),
            selection(),
        );
        let options = AdmissionOptions {
            key: AdmissionKey::new(crabber::core::ControlId::new().0)?,
            fingerprint: InputFingerprint::new("a".repeat(64))?,
            behavior_fingerprint: agent.behavior_fingerprint().clone(),
        };
        let Admission::Started { receipt, handle } = agent
            .prompt_keyed_in(&context, "count", options.clone(), None)
            .await?
        else {
            panic!("fresh admission")
        };
        assert_eq!(handle.done().await?.status, RunStatus::Completed);
        let session = store.get_session(&receipt.session_id).await?.unwrap();
        assert_eq!(session.workspace_id, workspace);
        assert_eq!(session.directory, directory);
        originals.push((context, options, receipt));
    }
    let source = originals[0].2.session_id.clone();
    agent.close_extensions().await?;
    for (context, options, receipt) in originals {
        assert!(
            matches!(agent.prompt_keyed_in(&context,"count",options,None).await?,Admission::Replayed(actual) if actual==receipt)
        );
    }
    assert_eq!(created.load(Ordering::SeqCst), 2);
    assert_eq!(closed.load(Ordering::SeqCst), 2);
    let observations = observations.lock().unwrap();
    assert_eq!(observations.len(), 2);
    assert_ne!(observations[0].0, observations[1].0);
    assert_ne!(observations[0].1, observations[1].1);
    assert!(observations.iter().all(|value| value.2 == 1));
    assert_eq!(decisions.load(Ordering::SeqCst), 2);
    assert_eq!(provider.requests().len(), 4);
    Ok((created.load(Ordering::SeqCst), observations.len(), source))
}
