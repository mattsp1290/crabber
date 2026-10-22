#![cfg(unix)]
use crabber::JsonToolExecutor;
use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection as ModelSelection,
    StaticPolicy, StreamDelta,
    core::{RunStatus, SessionId, ToolCallId, ToolCallRecord, ToolInfo},
    extension::{Extension, ExtensionError, Scope, ToolDefinition},
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use crabber_extensions::rtk_reducer::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn shell_available() -> bool {
    if Path::new("/bin/sh").is_file() {
        return true;
    }
    assert_ne!(
        std::env::var("BACKGROUND_JOBS_REQUIRE_SHELL").as_deref(),
        Ok("1"),
        "required /bin/sh missing"
    );
    eprintln!("rtk_reducer: /bin/sh absent, skipping synthetic fixture");
    false
}
fn options(dir: &Path, body: &str) -> Options {
    let executable_path = dir.join("rtk");
    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}/argv.log'\nprintf '%s\\n' \"$$\" >> '{}/pids'\n{body}\n",
        dir.display(),
        dir.display()
    );
    let mut file = fs::File::create(&executable_path).unwrap();
    file.write_all(script.as_bytes()).unwrap();
    file.sync_all().unwrap();
    drop(file);
    fs::set_permissions(&executable_path, fs::Permissions::from_mode(0o700)).unwrap();
    let temp_root = dir.join("temporary");
    fs::create_dir_all(&temp_root).unwrap();
    fs::set_permissions(&temp_root, fs::Permissions::from_mode(0o700)).unwrap();
    Options {
        order: DEFAULT_ORDER,
        executable_path,
        executable_sha256: format!("{:x}", Sha256::digest(script.as_bytes())),
        temp_root,
        bindings: vec![
            Binding {
                tool_name: "fixture_text".into(),
                match_input: None,
                selection: Selection::Text(Filter::Log),
            },
            Binding {
                tool_name: "fixture_json".into(),
                match_input: Some(InputMatch {
                    field: "cmd".into(),
                    equals: vec!["cargo test --workspace".into(), "git diff".into()],
                }),
                selection: Selection::JsonFields(vec![
                    Field {
                        name: "stdout".into(),
                        filter: Filter::CargoTest,
                    },
                    Field {
                        name: "diff".into(),
                        filter: Filter::GitDiff,
                    },
                ]),
            },
        ],
        limits: Limits {
            max_bindings: 8,
            max_fields_per_binding: 8,
            max_input_matches: 8,
            max_config_bytes: 16 * 1024,
            max_input_bytes: 4 * 1024,
            max_field_bytes: 1024 * 1024,
            max_concurrent: 2,
            max_stdout_bytes: 256 * 1024,
            timeout: Duration::from_secs(10),
            kill_wait: Duration::from_secs(3),
            shutdown_grace: Duration::from_secs(30),
        },
    }
}
struct FixtureTool {
    value: Mutex<Value>,
    executions: AtomicUsize,
    fail: bool,
}
impl FixtureTool {
    fn new(value: Value) -> Self {
        Self {
            value: Mutex::new(value),
            executions: AtomicUsize::new(0),
            fail: false,
        }
    }
}
#[async_trait::async_trait]
impl JsonToolExecutor for FixtureTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.executions.fetch_add(1, Ordering::Relaxed);
        if self.fail {
            Err(ExtensionError::Tool("synthetic failure".into()))
        } else {
            Ok(self.value.lock().unwrap().clone())
        }
    }
}
fn tool(name: &str, executor: Arc<FixtureTool>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: name.into(),
            description: "Synthetic output".into(),
            parameters: json!({"type":"object"}),
            retry_safe: false,
            required_permissions: vec!["fixture.execute".into()],
        },
        executor,
    })
}
fn agent_config(dir: &Path) -> AgentConfig {
    let mut c = AgentConfig::new(ModelSelection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    c.workspace_id = "fixture".into();
    c.directory = dir.to_str().unwrap().into();
    c
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
fn call(name: &str, args: Value) -> Vec<StreamDelta> {
    calls(&[(name, args)])
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
                text_bytes: 4_000_000,
                encoded_bytes: 8_000_000,
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
    if result_text(record) == crabber::runtime::INTERRUPTED_RESULT_TEXT {
        return json!(crabber::runtime::INTERRUPTED_RESULT_TEXT);
    }
    serde_json::from_str(result_text(record)).unwrap()
}
async fn groups_gone(dir: &Path) {
    let Ok(pids) = fs::read_to_string(dir.join("pids")) else {
        return;
    };
    for pid in pids.lines() {
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let ps = std::process::Command::new("ps")
                    .args(["-A", "-o", "pid=,pgid=,stat="])
                    .output()
                    .unwrap();
                assert!(ps.status.success());
                if !String::from_utf8(ps.stdout)
                    .unwrap()
                    .lines()
                    .any(|line| line.split_whitespace().nth(1) == Some(pid))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
async fn wait_file(path: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
struct Journey {
    dir: tempfile::TempDir,
    ext: Arc<RtkReducer>,
    store: Arc<MemoryStore>,
    provider: Arc<FakeProvider>,
    executor: Arc<FixtureTool>,
    probe: Arc<observe::ResultProbe>,
    agent: Agent,
}
impl Journey {
    fn new(
        body: &str,
        value: Value,
        name: &str,
        args: Value,
        edit: impl FnOnce(&mut Options),
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut o = options(dir.path(), body);
        edit(&mut o);
        let ext = Arc::new(RtkReducer::new(o).unwrap());
        let executor = Arc::new(FixtureTool::new(value));
        let store = Arc::new(MemoryStore::new());
        let provider = Arc::new(FakeProvider::scripted(vec![call(name, args), done()]));
        let probe = Arc::new(observe::ResultProbe::default());
        let agent = Agent::builder()
            .behavior_version("crabber-extensions-tests.v1")
            .store(store.clone())
            .provider(provider.clone())
            .config(agent_config(dir.path()))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .shared_tool(tool("fixture_text", executor.clone()))
            .shared_tool(tool("fixture_json", executor.clone()))
            .shared_extension(ext.clone(), Scope::Global)
            .shared_extension(Arc::new(observe::ProbeMount(probe.clone())), Scope::Global)
            .build()
            .unwrap();
        Self {
            dir,
            ext,
            store,
            provider,
            executor,
            probe,
            agent,
        }
    }
    async fn run(&self) -> SessionId {
        let run = self.agent.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
        session
    }
    async fn close(&self) {
        self.agent.close_extensions().await.unwrap();
        groups_gone(self.dir.path()).await;
        assert_eq!(self.ext.live_children(), 0);
        assert_eq!(
            fs::read_dir(self.dir.path().join("temporary"))
                .unwrap()
                .count(),
            0
        );
    }
    fn argv(&self) -> Vec<String> {
        fs::read_to_string(self.dir.path().join("argv.log"))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}
#[path = "rtk_reducer/compose.rs"]
mod compose;
#[path = "rtk_reducer/contract.rs"]
mod contract;
#[path = "rtk_reducer/lifecycle.rs"]
mod lifecycle;
#[path = "rtk_reducer/observe.rs"]
mod observe;
#[path = "rtk_reducer/real_rtk.rs"]
mod real_rtk;
#[path = "rtk_reducer/resume.rs"]
mod resume;
#[path = "rtk_reducer/runtime.rs"]
mod runtime;
