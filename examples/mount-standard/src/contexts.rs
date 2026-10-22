//! One configured host admits independent workspace instances without changing cwd.
use async_trait::async_trait;
use crabber::{
    Admission, Agent, AgentConfig, ExecutionContext, ExtensionError, FakeProvider,
    PermissionDecision, Selection, SessionContext, StaticPolicy, StreamDelta, ToolDefinition,
    core::{
        AdmissionKey, AdmissionOptions, ControlId, InputFingerprint, SessionId, ToolCallId,
        ToolInfo,
    },
    extension::{Extension, Registrar, Scope},
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use crabber_tools_catalog::prelude::*;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
fn turn(name: &str, args: Value) -> Vec<StreamDelta> {
    let id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: name.into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: args.to_string(),
        },
        StreamDelta::ToolCallDone { call_id: id },
        StreamDelta::Completed,
    ]
}
struct Marker(Arc<AtomicUsize>);
#[async_trait]
impl crabber::JsonToolExecutor for Marker {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"seen":true}))
    }
}
fn marker(name: &str, seen: Arc<AtomicUsize>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: name.into(),
            description: "Synthetic host marker".into(),
            parameters: json!({"type":"object"}),
            retry_safe: false,
            required_permissions: vec![],
        },
        executor: Arc::new(Marker(seen)),
    })
}
struct Host(Arc<AtomicUsize>);
#[async_trait]
impl Extension for Host {
    fn id(&self) -> &str {
        "contexts/host"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn config_hash(&self) -> String {
        "contexts-host-v1".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        r.tool(marker("extension_marker", self.0.clone()));
        Ok(())
    }
}
fn selection() -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    }
}
fn context(id: &str, directory: &std::path::Path) -> ExecutionContext {
    let directory = directory
        .canonicalize()
        .expect("host fixture workspace exists");
    ExecutionContext::new(
        SessionContext::new(SessionId::new(), id, directory.to_str().unwrap()),
        Selection {
            provider_id: "fake".into(),
            model_id: id.into(),
        },
    )
}
async fn admit(
    agent: &Agent,
    ctx: &ExecutionContext,
) -> Result<crabber::RunHandle, Box<dyn std::error::Error>> {
    let options = AdmissionOptions {
        key: AdmissionKey::new(ControlId::new().0)?,
        fingerprint: InputFingerprint::new(crabber_tools_core_hash())?,
        behavior_fingerprint: agent.behavior_fingerprint().clone(),
    };
    let Admission::Started { handle, .. } = agent
        .prompt_keyed_in(ctx, "workspace", options, None)
        .await?
    else {
        panic!("fresh admission")
    };
    Ok(handle)
}
// Fixed synthetic input fingerprint; every call uses the same text and a fresh key.
fn crabber_tools_core_hash() -> String {
    "a".repeat(64)
}
/// Exercise concurrent roots, custom registrations and targeted workspace close.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cwd = std::env::current_dir()?;
    let a = tempfile::tempdir()?;
    let b = tempfile::tempdir()?;
    std::fs::write(a.path().join("a-only"), "A")?;
    std::fs::write(b.path().join("b-only"), "B")?;
    let roots = ExactRoots::new([
        ("ws-a".into(), a.path().canonicalize()?),
        ("ws-b".into(), b.path().canonicalize()?),
    ])?;
    let policies = Policies {
        enabled: EnabledSet::only([ToolId::FileWrite, ToolId::FileRead, ToolId::FileList]),
        restrict_to_enabled: false,
        shell: ShellPolicy {
            shell_binary: "/bin/sh".into(),
            startup_mode: StartupMode::NonLogin,
            env: EnvPolicy::minimal_allowlist(),
            run_as: None,
            output_cap_bytes: 65536,
        },
        search: SearchPolicy::resolve_rg_from_path(EnvPolicy::minimal_allowlist())?,
        url_fetch: None,
        tracker: None,
        user_interact: None,
        limits: Limits {
            max_in_flight: 2,
            max_blocking_wait: Duration::from_secs(2),
        },
    };
    let seen = Arc::new(AtomicUsize::new(0));
    // Per-workspace scripts carry fresh globally unique tool-call IDs.
    let batch = || {
        let mut batch = turn("file_write", json!({"path":"notes.txt","content":"owned"}));
        batch.pop();
        batch.extend(turn("file_list", json!({})));
        batch.pop();
        batch.extend(turn("host_marker", json!({})));
        batch.pop();
        batch.extend(turn("extension_marker", json!({})));
        batch
    };
    let finish = vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ];
    let provider = Arc::new(Providers {
        a: FakeProvider::scripted(vec![batch(), finish.clone()]),
        b: FakeProvider::scripted(vec![
            batch(),
            finish.clone(),
            turn("file_read", json!({"path":"notes.txt"})),
            finish,
        ]),
    });
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("contexts/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(provider)
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .extension_factory(Arc::new(StandardToolsFactory::new(
            policies,
            Arc::new(roots),
        )?))
        .shared_tool(marker("host_marker", seen.clone()))
        .shared_extension(Arc::new(Host(seen.clone())), Scope::Global)
        .build()?;
    agent.ready().await?;
    let ca = context("ws-a", a.path());
    let cb = context("ws-b", b.path());
    let (ra, rb) = tokio::join!(admit(&agent, &ca), admit(&agent, &cb));
    let (da, db) = tokio::join!(ra?.done(), rb?.done());
    da?;
    db?;
    assert_eq!(
        std::fs::read_to_string(a.path().join("notes.txt"))?,
        "owned"
    );
    assert_eq!(
        std::fs::read_to_string(b.path().join("notes.txt"))?,
        "owned"
    );
    for (ctx, own, other) in [(&ca, "a-only", "b-only"), (&cb, "b-only", "a-only")] {
        let SnapshotOutcome::Page(page) = store
            .snapshot(SnapshotRequest {
                session_id: ctx.session().session_id().clone(),
                limits: SnapshotLimits {
                    messages: 100,
                    tool_calls: 100,
                    parts: 100,
                    text_bytes: 1_000_000,
                    encoded_bytes: 2_000_000,
                },
                continuation: None,
            })
            .await?
        else {
            return Err("snapshot required".into());
        };
        assert_eq!(page.tool_calls.len(), 4);
        assert!(
            page.tool_calls
                .iter()
                .all(|r| r.status == crabber::core::ToolCallStatus::Completed)
        );
        let listing = page
            .tool_calls
            .iter()
            .find(|r| r.name == "file_list")
            .unwrap();
        let content = serde_json::to_string(listing.result.as_ref().unwrap())?;
        assert!(content.contains(own));
        assert!(!content.contains(other));
    }
    assert_eq!(seen.load(Ordering::SeqCst), 4);
    agent
        .close_workspace_resources(ca.session(), Duration::from_secs(2))
        .await?;
    agent
        .prompt_in(&cb, "still admitted", None)
        .await?
        .done()
        .await?;
    assert_eq!(std::env::current_dir()?, cwd);
    agent.close_extensions().await?;
    println!("{}", json!({"check":"two_workspaces","ok":true}));
    Ok(())
}

// Independent deterministic scripts keep concurrent session scheduling irrelevant.
struct Providers {
    a: FakeProvider,
    b: FakeProvider,
}
#[async_trait]
impl crabber::providers::Resolver for Providers {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, crabber::providers::ProviderBindingError>
    {
        crabber::providers::Resolver::binding_descriptor(&self.a, selection)
    }
    async fn resolve(
        &self,
        selection: &Selection,
    ) -> Result<Arc<dyn crabber::providers::Streamer>, crabber::providers::ProviderError> {
        crabber::providers::Resolver::resolve(
            if selection.model_id == "ws-b" {
                &self.b
            } else {
                &self.a
            },
            selection,
        )
        .await
    }
}
