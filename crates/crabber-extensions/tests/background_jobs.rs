#![cfg(unix)]
use crate::json_output::JsonOutputExecutor as _;
use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection, StaticPolicy, StreamDelta,
    core::{RunId, RunStatus, SessionId, ToolCallId, ToolCallRecord, ToolCallStatus},
    extension::{
        Extension, ExtensionError, HostServices, MountHandle, Registry, Scope, ToolContext,
        ToolExecutor, WorkspaceContext,
    },
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use crabber_extensions::background_jobs::*;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

fn limits() -> Limits {
    Limits {
        max_running: 2,
        max_tracked: 4,
        max_command_bytes: 4096,
        max_working_directory_bytes: 1024,
        max_output_bytes_per_stream: 4096,
        max_environment_entries: 16,
        max_environment_bytes: 4096,
        default_timeout: Duration::ZERO,
        max_timeout: Duration::from_secs(30),
        terminate_grace: Duration::from_millis(500),
        kill_wait: Duration::from_secs(3),
        shutdown_grace: Duration::from_secs(5),
    }
}
fn options(dir: &Path) -> Options {
    Options {
        shell_path: "/bin/sh".into(),
        shell_identity: "test-sh-v1".into(),
        environment: Environment {
            mode: EnvironmentMode::ExplicitOnly,
            overrides: BTreeMap::from([
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("CANARY".into(), dir.join("canary").to_str().unwrap().into()),
                (
                    "PGID_FILE".into(),
                    dir.join("pgid").to_str().unwrap().into(),
                ),
            ]),
            identity: "test-env-v1".into(),
        },
        limits: limits(),
    }
}
fn context(dir: &Path, cancel: CancellationToken) -> ToolContext {
    context_for(dir, cancel, SessionId::from("session"), "workspace")
}
fn context_for(
    dir: &Path,
    cancel: CancellationToken,
    session: SessionId,
    workspace: &str,
) -> ToolContext {
    ToolContext::new(
        session,
        RunId::from("run"),
        ToolCallId::from("call"),
        cancel,
        HostServices::default(),
        WorkspaceContext::from_persisted(workspace, dir.to_str().unwrap()),
        Arc::new(|_| {}),
    )
}
fn agent_config(dir: &Path) -> AgentConfig {
    let mut c = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    c.workspace_id = "workspace".into();
    c.directory = dir.to_str().unwrap().into();
    c
}
fn agent(
    ext: Arc<BackgroundJobs>,
    store: Arc<MemoryStore>,
    provider: Arc<FakeProvider>,
    decision: PermissionDecision,
    dir: &Path,
) -> Agent {
    Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store)
        .provider(provider)
        .config(agent_config(dir))
        .policy(Arc::new(StaticPolicy::new(decision)))
        .shared_extension(ext, Scope::Global)
        .build()
        .unwrap()
}
fn command(script: &str) -> String {
    format!("printf '%s' \"$PPID\" > \"$PGID_FILE.$PPID\"; {script}")
}
fn call(tool: &str, args: Value) -> Vec<StreamDelta> {
    calls(&[(tool, args)])
}
fn calls(items: &[(&str, Value)]) -> Vec<StreamDelta> {
    let mut stream = vec![];
    for (name, args) in items {
        let id = ToolCallId::new();
        stream.extend([
            StreamDelta::ToolCallStart {
                call_id: id.clone(),
                name: (*name).into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: id.clone(),
                text: args.to_string(),
            },
            StreamDelta::ToolCallDone { call_id: id },
        ]);
    }
    stream.push(StreamDelta::Completed);
    stream
}
fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
async fn snapshot_tools(store: &MemoryStore, session: &SessionId) -> Vec<ToolCallRecord> {
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session.clone(),
            limits: SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 2_000_000,
                encoded_bytes: 4_000_000,
            },
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("snapshot")
    };
    page.tool_calls
}
fn result_text(record: &ToolCallRecord) -> &str {
    let [crabber::core::ContentBlock::Text { text }] =
        record.result.as_ref().unwrap().content.as_slice()
    else {
        panic!("result")
    };
    text
}
fn result_value(record: &ToolCallRecord) -> Value {
    serde_json::from_str(result_text(record)).unwrap()
}
fn shell_available() -> bool {
    use std::os::unix::fs::PermissionsExt;
    let available = std::fs::metadata("/bin/sh").is_ok_and(|m| m.permissions().mode() & 0o111 != 0)
        && std::process::Command::new("ps")
            .args(["-A", "-o", "pid=,pgid=,stat="])
            .output()
            .is_ok_and(|o| o.status.success());
    if !available {
        assert_ne!(
            std::env::var("BACKGROUND_JOBS_REQUIRE_SHELL").as_deref(),
            Ok("1"),
            "required shell/ps absent"
        );
        eprintln!("background_jobs: /bin/sh or ps absent, skipping");
    }
    available
}
fn group_members(pgid: i32) -> Vec<u32> {
    let output = std::process::Command::new("ps")
        .args(["-A", "-o", "pid=,pgid=,stat="])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter_map(|line| {
            let mut f = line.split_whitespace();
            let pid = f.next()?.parse().ok()?;
            let group: i32 = f.next()?.parse().ok()?;
            let state = f.next()?;
            (group == pgid && !state.starts_with('Z')).then_some(pid)
        })
        .collect()
}
fn pgids(dir: &Path) -> Vec<i32> {
    let mut ids: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_str()?
                .strip_prefix("pgid.")?
                .parse()
                .ok()
        })
        .collect();
    ids.sort();
    ids
}
async fn groups_gone(dir: &Path) {
    for pgid in pgids(dir) {
        timeout(Duration::from_secs(5), async {
            while !group_members(pgid).is_empty() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
async fn wait_file(path: &Path) {
    timeout(Duration::from_secs(5), async {
        while !path.exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
async fn wait_pgids(dir: &Path, count: usize) {
    timeout(Duration::from_secs(5), async {
        while pgids(dir).len() < count {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

struct Harness {
    registry: Registry,
    handle: MountHandle,
    ext: Arc<BackgroundJobs>,
    dir: tempfile::TempDir,
    tools: BTreeMap<String, Arc<dyn ToolExecutor>>,
}
impl Harness {
    async fn new(edit: impl FnOnce(&mut Options)) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut o = options(dir.path());
        edit(&mut o);
        let ext = Arc::new(BackgroundJobs::new(o).unwrap());
        Self::mount(ext, dir).await
    }
    async fn mount(ext: Arc<BackgroundJobs>, dir: tempfile::TempDir) -> Self {
        let registry = Registry::new();
        let handle = registry.mount(ext.clone(), Scope::Global).await.unwrap();
        let plan = registry.acquire(&SessionId::from("session"));
        let tools = plan
            .tools
            .iter()
            .map(|t| (t.info.name.clone(), t.executor.clone()))
            .collect();
        drop(plan);
        Self {
            registry,
            handle,
            ext,
            dir,
            tools,
        }
    }
    async fn execute(&self, tool: &str, args: Value) -> Result<Value, ExtensionError> {
        self.tools[tool]
            .execute_json_with_context(context(self.dir.path(), CancellationToken::new()), args)
            .await
    }
    async fn start(&self, script: &str) -> Value {
        let count = pgids(self.dir.path()).len();
        let result = self
            .execute(START_TOOL, json!({"command":command(script)}))
            .await
            .unwrap();
        wait_pgids(self.dir.path(), count + 1).await;
        result
    }
    async fn status(&self, id: &str) -> Value {
        self.execute(STATUS_TOOL, json!({"id":id})).await.unwrap()
    }
    async fn terminal(&self, id: &str) -> Value {
        timeout(Duration::from_secs(10), async {
            loop {
                let s = self.status(id).await;
                if s["state"] != "running" {
                    return s;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
    async fn kill(&self, id: &str) -> Value {
        self.execute(KILL_TOOL, json!({"id":id})).await.unwrap()
    }
    async fn close(&self) {
        self.handle.close().await.unwrap();
        groups_gone(self.dir.path()).await;
        assert_eq!(self.ext.live_jobs(), 0);
        assert!(
            self.registry
                .acquire(&SessionId::from("session"))
                .tools
                .is_empty()
        );
    }
}
#[path = "background_jobs/compose.rs"]
mod compose;
#[path = "background_jobs/contract.rs"]
mod contract;
#[path = "background_jobs/lifecycle.rs"]
mod lifecycle;
#[path = "background_jobs/resume.rs"]
mod resume;
#[path = "background_jobs/runtime.rs"]
mod runtime;

#[path = "support/owned_process.rs"]
mod owned_process;
impl Drop for Harness {
    fn drop(&mut self) {
        // Stop owned groups/holder even if an assertion unwinds the test.
        let mut cleanup = owned_process::ProcessCleanup::new(self.dir.path());
        if self.ext.live_jobs() == 0 {
            cleanup.disarm_groups();
        }
    }
}

#[path = "background_jobs/observe.rs"]
mod observe;

#[path = "support/json_output.rs"]
mod json_output;
