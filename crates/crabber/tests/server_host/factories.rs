use super::{call, context, done, options, selection};
use async_trait::async_trait;
use crabber::{
    Admission, Agent, AgentConfig, ExecutionContext, ExtensionError, ExtensionFactory,
    FactoryContext, FactoryScope, PermissionDecision, RunHandle, StaticPolicy, StreamDelta,
    ToolDefinition, ToolExecutor,
    core::{RunStatus, SessionId, ToolInfo},
    extension::{ComponentIdentity, Extension, Registrar, ToolContext},
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Factory {
    created: Arc<AtomicUsize>,
    observed: Arc<Mutex<Vec<(String, usize)>>>,
    stopped: Arc<Mutex<Vec<String>>>,
    together: Arc<tokio::sync::Barrier>,
}
struct State {
    session: SessionId,
    workspace: String,
    directory: String,
    counter: AtomicUsize,
    observed: Arc<Mutex<Vec<(String, usize)>>>,
    together: Arc<tokio::sync::Barrier>,
}
struct ScopedFs(String);
#[async_trait]
impl crabber::extension::WorkspaceFs for ScopedFs {
    async fn read(&self, _: &str) -> Result<Vec<u8>, String> {
        Ok(self.0.as_bytes().to_vec())
    }
}
struct SharedProcess;
#[async_trait]
impl crabber::extension::Subprocess for SharedProcess {
    async fn run(&self, _: &str, _: &[String]) -> Result<Value, String> {
        Ok(json!("shared-process"))
    }
}
struct Counter(Arc<State>);
#[async_trait]
impl ToolExecutor for Counter {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("context required")
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
        assert_eq!(
            context.workspace().directory(),
            Some(self.0.directory.as_str())
        );
        let bytes = context
            .host
            .workspace_fs
            .as_ref()
            .unwrap()
            .read("fixture")
            .await
            .unwrap();
        assert_eq!(bytes, self.0.directory.as_bytes());
        assert_eq!(
            context
                .host
                .subprocess
                .as_ref()
                .unwrap()
                .run("fixture", &[])
                .await
                .unwrap(),
            json!("shared-process")
        );
        let count = self.0.counter.fetch_add(1, Ordering::SeqCst) + 1;
        self.0
            .observed
            .lock()
            .unwrap()
            .push((self.0.session.to_string(), count));
        self.0.together.wait().await;
        Ok(json!({"count":count}))
    }
}
struct ExtensionInstance(Arc<State>);
#[async_trait]
impl Extension for ExtensionInstance {
    fn id(&self) -> &'static str {
        "scoped-counter"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "counter-v1".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.host_services(crabber::extension::HostServices {
            workspace_fs: Some(Arc::new(ScopedFs(self.0.directory.clone()))),
            ..Default::default()
        });
        registrar.tool(tool("owner_count", Arc::new(Counter(self.0.clone()))));
        Ok(())
    }
}
#[async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "scoped-counter-factory".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "counter-config-v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.created.fetch_add(1, Ordering::SeqCst);
        let session = context.session_id().unwrap().clone();
        let stopped = self.stopped.clone();
        let label = session.to_string();
        let closing = context.cleanup().closing();
        context.cleanup().spawn(async move {
            closing.await;
            stopped.lock().unwrap().push(label);
        });
        Ok(Arc::new(ExtensionInstance(Arc::new(State {
            session,
            workspace: context.workspace().workspace_id().unwrap().into(),
            directory: context.workspace().directory().unwrap().into(),
            counter: AtomicUsize::new(0),
            observed: self.observed.clone(),
            together: self.together.clone(),
        }))))
    }
}
struct SharedMarker;
#[async_trait]
impl ToolExecutor for SharedMarker {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Ok(json!(true))
    }
}
fn tool(name: &str, executor: Arc<dyn ToolExecutor>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: name.into(),
            description: "Fixture".into(),
            parameters: json!({"type":"object"}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor,
    })
}
fn counter_call(id: &str) -> Vec<StreamDelta> {
    call(id)
        .into_iter()
        .map(|delta| match delta {
            StreamDelta::ToolCallStart { call_id, .. } => StreamDelta::ToolCallStart {
                call_id,
                name: "owner_count".into(),
            },
            other => other,
        })
        .collect()
}
async fn run_pair(agent: &Agent, a: &ExecutionContext, b: &ExecutionContext, first: bool) {
    let (left, right): (RunHandle, RunHandle) = if first {
        let (left, right) = tokio::join!(
            agent.prompt_keyed_in(a, "first", options(), None),
            agent.prompt_keyed_in(b, "first", options(), None)
        );
        let Admission::Started { handle: left, .. } = left.unwrap() else {
            panic!("new a")
        };
        let Admission::Started { handle: right, .. } = right.unwrap() else {
            panic!("new b")
        };
        (left, right)
    } else {
        let (left, right) = tokio::join!(
            agent.prompt_in(a, "next", None),
            agent.prompt_in(b, "next", None)
        );
        (left.unwrap(), right.unwrap())
    };
    let (left, right) = tokio::join!(left.done(), right.done());
    assert_eq!(left.unwrap().status, RunStatus::Completed);
    assert_eq!(right.unwrap().status, RunStatus::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_agent_uses_real_session_instances_and_preserves_shared_tools() {
    let created = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(vec![]));
    let stopped = Arc::new(Mutex::new(vec![]));
    let provider = Arc::new(crabber::FakeProvider::scripted(vec![
        counter_call("a"),
        counter_call("b"),
        done(),
        done(),
        counter_call("c"),
        counter_call("d"),
        done(),
        done(),
    ]));
    let agent = Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .memory()
        .provider(provider.clone())
        .config(AgentConfig::new(selection("configured")))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(tool("shared_marker", Arc::new(SharedMarker)))
        .shared_host_services(
            ComponentIdentity {
                id: "shared-services".into(),
                version: "1".into(),
            },
            crabber::InputFingerprint::new("e".repeat(64)).unwrap(),
            crabber::extension::HostServices {
                workspace_fs: Some(Arc::new(ScopedFs("wrong-global-root".into()))),
                subprocess: Some(Arc::new(SharedProcess)),
                ..Default::default()
            },
        )
        .extension_factory(Arc::new(Factory {
            created: created.clone(),
            observed: observed.clone(),
            stopped: stopped.clone(),
            together: Arc::new(tokio::sync::Barrier::new(2)),
        }))
        .build()
        .unwrap();
    let a = context("factory-a", "workspace-a", "/admitted/a", "model-a");
    let b = context("factory-b", "workspace-b", "/admitted/b", "model-b");
    for first in [true, false] {
        tokio::time::timeout(Duration::from_secs(5), run_pair(&agent, &a, &b, first))
            .await
            .expect("independent sessions must both progress");
    }
    assert_eq!(created.load(Ordering::SeqCst), 2);
    let mut actual = observed.lock().unwrap().clone();
    actual.sort();
    assert_eq!(
        actual,
        [
            ("factory-a".into(), 1),
            ("factory-a".into(), 2),
            ("factory-b".into(), 1),
            ("factory-b".into(), 2)
        ]
    );
    assert!(
        provider
            .requests()
            .iter()
            .all(|r| r.tools.iter().any(|tool| tool.name == "shared_marker"))
    );
    agent
        .close_session_resources(a.session(), Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(*stopped.lock().unwrap(), ["factory-a"]);
    assert!(matches!(
        agent.prompt_in(&a, "closed owner", None).await,
        Err(crabber::RuntimeError::ExtensionPlan(
            ExtensionError::ContextOwnerClosed
        ))
    ));
    agent
        .close_workspace_resources(b.session(), Duration::from_secs(1))
        .await
        .unwrap();
    agent.close_extensions().await.unwrap();
    let mut stopped = stopped.lock().unwrap().clone();
    stopped.sort();
    assert_eq!(stopped, ["factory-a", "factory-b"]);
}
