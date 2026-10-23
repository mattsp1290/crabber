//! Workspace factory contracts using persisted host identity and public APIs.
use crabber::{
    Agent, FakeProvider, PermissionDecision, StaticPolicy, ToolDefinition,
    core::{SessionId, ToolCallStatus},
    session::{MemoryStore, Store},
};
use crabber_tools_catalog::prelude::*;
use crabber_tools_testkit as support;
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use support::*;
struct Cwd(std::path::PathBuf);
impl Cwd {
    fn new() -> Self {
        Self(std::env::current_dir().unwrap())
    }
}
impl Drop for Cwd {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            assert_eq!(std::env::current_dir().unwrap(), self.0);
        }
    }
}
fn policies() -> Policies {
    Policies {
        deny: crabber_tools_catalog::prelude::PathDenyPolicy::none(),
        enabled: EnabledSet::all(),
        restrict_to_enabled: false,
        shell: ShellPolicy {
            shell_binary: "/bin/sh".into(),
            startup_mode: StartupMode::NonLogin,
            env: EnvPolicy::minimal_allowlist(),
            run_as: None,
            output_cap_bytes: 16,
        },
        search: SearchPolicy::resolve_rg_from_path(EnvPolicy::minimal_allowlist()).unwrap(),
        url_fetch: Some(UrlFetchPolicy::default()),
        tracker: None,
        user_interact: Some(UserInteractPolicy::pending()),
        limits: Limits {
            max_in_flight: 1,
            max_blocking_wait: Duration::from_millis(30),
        },
    }
}
fn base() -> Policies {
    let mut p = policies();
    p.enabled = EnabledSet::only([
        ToolId::FileRead,
        ToolId::FileWrite,
        ToolId::FileList,
        ToolId::Shell,
        ToolId::UrlFetch,
        ToolId::UserInteract,
    ]);
    p
}
struct Host;
#[async_trait::async_trait]
impl crabber::JsonToolExecutor for Host {
    async fn execute(&self, _: Value) -> Result<Value, crabber::ExtensionError> {
        Ok(json!({"host":true}))
    }
}
fn host() -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: crabber::core::ToolInfo {
            name: "host_marker".into(),
            description: "synthetic host tool".into(),
            parameters: json!({"type":"object"}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(Host),
    })
}
fn agent(
    a: &Path,
    b: &Path,
    p: Policies,
    script: Vec<Vec<crabber::StreamDelta>>,
    policy: StaticPolicy,
) -> (Agent, Arc<MemoryStore>) {
    let store = Arc::new(MemoryStore::new());
    let roots = ExactRoots::new([
        ("ws-a".into(), a.canonicalize().unwrap()),
        ("ws-b".into(), b.canonicalize().unwrap()),
    ])
    .unwrap();
    let agent = Agent::builder()
        .behavior_version("context-tests/0.4.0")
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(script)))
        .config(config(a))
        .policy(Arc::new(policy))
        .extension_factory(Arc::new(
            StandardToolsFactory::new(p, Arc::new(roots)).unwrap(),
        ))
        .shared_tool(host())
        .build()
        .unwrap();
    (agent, store)
}
fn allow() -> StaticPolicy {
    StaticPolicy::new(PermissionDecision::Allow)
}
async fn execute_in(
    agent: &Agent,
    store: &MemoryStore,
    workspace: &str,
    root: &Path,
) -> Vec<crabber::core::ToolCallRecord> {
    let ctx = execution(SessionId::new(), workspace, root);
    let run = admit(agent, &ctx, "context").await;
    let id = run.session_id().clone();
    run.done().await.unwrap();
    calls(store, &id).await
}

#[path = "contexts/policies.rs"]
mod policies;

#[path = "contexts/admission.rs"]
mod admission;

#[path = "contexts/capacity.rs"]
mod capacity;

#[path = "contexts/lifecycle.rs"]
mod lifecycle;
