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
use crabber_extensions::python_repl::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;
#[path = "support/python.rs"]
mod interpreter;

fn python_available() -> bool {
    static AVAILABLE: OnceLock<bool> = OnceLock::new();
    *AVAILABLE.get_or_init(|| {
        let Some(python) = interpreter::python() else {
            return false;
        };
        let supported = std::process::Command::new(python)
            .args([
                "-c",
                "import sys; assert (3,11) <= sys.version_info[:2] <= (3,14)",
            ])
            .status()
            .unwrap()
            .success();
        if !supported {
            assert_ne!(
                std::env::var("PYTHON_REPL_REQUIRE_PYTHON").as_deref(),
                Ok("1"),
                "required Python version outside 3.11–3.14"
            );
            eprintln!("python_repl: unsupported Python version, skipping");
        }
        supported
    })
}
fn limits() -> Limits {
    Limits {
        max_sessions: 2,
        max_queued_per_session: 2,
        max_code_bytes: 4096,
        max_output_bytes_per_stream: 4096,
        max_result_bytes: 4096,
        max_exception_bytes: 8192,
        max_environment_entries: 16,
        max_environment_bytes: 4096,
        default_timeout: Duration::from_secs(10),
        max_timeout: Duration::from_secs(30),
        runner_start_timeout: Duration::from_secs(10),
        terminate_grace: Duration::from_millis(300),
        kill_wait: Duration::from_secs(3),
        shutdown_grace: Duration::from_secs(5),
    }
}
fn options(dir: &Path) -> Options {
    Options {
        python_path: interpreter::python().unwrap(),
        python_identity: "test-python-v1".into(),
        temp_root: dir.join("temporary"),
        environment: Environment {
            mode: EnvironmentMode::ExplicitOnly,
            overrides: BTreeMap::from([
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("FIXTURE_ROOT".into(), dir.to_str().unwrap().into()),
                ("FIXTURE_SECRET".into(), "synthetic-python-secret".into()),
            ]),
            identity: "test-env-v1".into(),
        },
        limits: limits(),
    }
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
fn context(dir: &Path, cancel: CancellationToken) -> ToolContext {
    context_for(dir, cancel, "session".into(), "workspace")
}
fn agent_config(dir: &Path) -> AgentConfig {
    let mut config = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = "workspace".into();
    config.directory = dir.to_str().unwrap().into();
    config
}
fn agent(
    ext: Arc<PythonRepl>,
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
fn calls(items: &[(&str, Value)]) -> Vec<StreamDelta> {
    let mut stream = Vec::new();
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
fn call(tool: &str, args: Value) -> Vec<StreamDelta> {
    calls(&[(tool, args)])
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
        panic!("inline result")
    };
    text
}
fn result_value(record: &ToolCallRecord) -> Value {
    serde_json::from_str(result_text(record)).unwrap()
}
fn record_code(code: &str) -> String {
    format!(
        "import os as _fixture_os, pathlib as _fixture_pathlib\n_fixture_pathlib.Path(_fixture_os.environ['FIXTURE_ROOT'], 'pgid.' + str(_fixture_os.getpgrp())).write_text(str(_fixture_os.getpid()))\n{code}"
    )
}
fn pgids(dir: &Path) -> Vec<i32> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            e.unwrap()
                .file_name()
                .to_str()?
                .strip_prefix("pgid.")?
                .parse()
                .ok()
        })
        .collect()
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
            let _state = f.next()?;
            (group == pgid).then_some(pid)
        })
        .collect()
}
async fn groups_gone(dir: &Path) {
    for id in pgids(dir) {
        timeout(Duration::from_secs(5), async {
            while !group_members(id).is_empty() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
async fn wait_file(path: &Path) {
    timeout(Duration::from_secs(10), async {
        while !path.exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
struct Harness {
    registry: Registry,
    handle: MountHandle,
    ext: Arc<PythonRepl>,
    dir: tempfile::TempDir,
    tools: BTreeMap<String, Arc<dyn ToolExecutor>>,
}
impl Harness {
    async fn new(edit: impl FnOnce(&mut Options)) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("temporary")).unwrap();
        let mut options = options(dir.path());
        edit(&mut options);
        let ext = Arc::new(PythonRepl::new(options).unwrap());
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
    async fn close(&self) {
        self.handle.close().await.unwrap();
        groups_gone(self.dir.path()).await;
        assert_eq!(self.ext.live_runners(), 0);
        assert_eq!(
            std::fs::read_dir(self.dir.path().join("temporary"))
                .unwrap()
                .count(),
            0
        );
        assert!(
            self.registry
                .acquire(&SessionId::from("session"))
                .tools
                .is_empty()
        );
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        if self.ext.live_runners() != 0 {
            for id in pgids(self.dir.path()) {
                if let Some(pid) = rustix::process::Pid::from_raw(id) {
                    let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
                }
            }
        }
    }
}
#[path = "python_repl/contract.rs"]
mod contract;
#[path = "python_repl/lifecycle.rs"]
mod lifecycle;
#[path = "python_repl/observe.rs"]
mod observe;
#[path = "python_repl/resume.rs"]
mod resume;
#[path = "python_repl/runtime.rs"]
mod runtime;

#[path = "python_repl/compose.rs"]
mod compose;

#[path = "support/json_output.rs"]
mod json_output;
