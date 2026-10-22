use super::{agent_support, options, owned_process};
use crabber::{
    Agent, AgentConfig, ExecutionContext, PermissionDecision, Selection, StaticPolicy, StreamDelta,
    core::{ContentBlock, RunId, SessionContext, ToolCallId, ToolCallRecord},
    extension::FactoryScope,
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use crabber_extensions::{
    background_jobs as jobs, delegate_task as delegate, python_repl as python,
};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};

pub use crate::scripted::Scripts;
pub fn selection() -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    }
}
pub fn context(root: &Path, workspace: &str, session: &str) -> ExecutionContext {
    ExecutionContext::new(
        SessionContext::new(session.into(), workspace, root.to_str().unwrap()),
        selection(),
    )
}
pub fn calls(items: &[(&str, Value)]) -> Vec<StreamDelta> {
    let mut deltas = Vec::new();
    for (name, arguments) in items {
        let call_id = ToolCallId::new();
        deltas.extend([
            StreamDelta::ToolCallStart {
                call_id: call_id.clone(),
                name: (*name).into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call_id.clone(),
                text: arguments.to_string(),
            },
            StreamDelta::ToolCallDone { call_id },
        ]);
    }
    deltas.push(StreamDelta::Completed);
    deltas
}
pub fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
pub fn value(record: &ToolCallRecord) -> Value {
    let [ContentBlock::Text { text }] = &record.result.as_ref().unwrap().content[..] else {
        panic!("JSON result");
    };
    serde_json::from_str(text).unwrap()
}
pub struct Harness {
    // Struct fields drop in declaration order: retain PID files until this
    // failure-path guard has signalled every fixture-owned process group.
    cleanup: owned_process::ProcessCleanup,
    pub root: tempfile::TempDir,
    pub agent: Agent,
    pub store: Arc<MemoryStore>,
    pub scripts: Arc<Scripts>,
    pub jobs: Arc<jobs::Factory>,
    pub python: Arc<python::Factory>,
    pub delegate: Arc<delegate::Factory>,
}
impl Harness {
    pub fn new(scope: FactoryScope, runner: delegate::Runner) -> Option<Self> {
        Self::with_pools(scope, runner, [None, None, None])
    }
    pub fn with_pools(
        scope: FactoryScope,
        runner: delegate::Runner,
        pools: [Option<jobs::SharedCapacity>; 3],
    ) -> Option<Self> {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("temporary")).unwrap();
        std::fs::create_dir(root.path().join("a")).unwrap();
        std::fs::create_dir(root.path().join("b")).unwrap();
        let [job_pool, python_pool, delegate_pool] = pools;
        let jobs = jobs::Factory::new(options::jobs(root.path()), scope).unwrap();
        let jobs = Arc::new(if let Some(pool) = job_pool {
            jobs.with_shared_capacity(pool)
        } else {
            jobs
        });
        let python = python::Factory::new(options::python(root.path())?, scope).unwrap();
        let python = Arc::new(if let Some(pool) = python_pool {
            python.with_shared_capacity(pool)
        } else {
            python
        });
        let delegate = delegate::Factory::new(options::delegate(runner), scope).unwrap();
        let delegate = Arc::new(if let Some(pool) = delegate_pool {
            delegate.with_shared_capacity(pool)
        } else {
            delegate
        });
        let scripts = Arc::new(Scripts::default());
        let store = Arc::new(MemoryStore::new());
        let agent = agent_support::builder()
            .store(store.clone())
            .provider(scripts.clone())
            .config(AgentConfig::new(selection()))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .extension_factory(jobs.clone())
            .extension_factory(python.clone())
            .extension_factory(delegate.clone())
            .build()
            .unwrap();
        let cleanup = owned_process::ProcessCleanup::new(root.path());
        Some(Self {
            root,
            agent,
            store,
            scripts,
            jobs,
            python,
            delegate,
            cleanup,
        })
    }
    pub async fn records(&self, execution: &ExecutionContext, run: &RunId) -> Vec<ToolCallRecord> {
        let SnapshotOutcome::Page(page) = self
            .store
            .snapshot(SnapshotRequest {
                session_id: execution.session().session_id().clone(),
                limits: SnapshotLimits {
                    messages: 100,
                    tool_calls: 100,
                    parts: 100,
                    text_bytes: 1024 * 1024,
                    encoded_bytes: 2 * 1024 * 1024,
                },
                continuation: None,
            })
            .await
            .unwrap()
        else {
            panic!("snapshot page");
        };
        assert!(page.continuation.is_none());
        page.tool_calls
            .into_iter()
            .filter(|record| record.run_id == *run)
            .collect()
    }
    pub async fn first(
        &self,
        execution: &ExecutionContext,
        items: &[(&str, Value)],
    ) -> Vec<ToolCallRecord> {
        self.scripts.set(vec![calls(items), done()]);
        let run = agent_support::first_prompt(&self.agent, execution, "first")
            .await
            .unwrap();
        let id = run.run_id().clone();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), run.done())
                .await
                .unwrap()
                .unwrap()
                .status,
            crabber::core::RunStatus::Completed
        );
        self.records(execution, &id).await
    }
    pub async fn next(
        &self,
        execution: &ExecutionContext,
        items: &[(&str, Value)],
    ) -> Vec<ToolCallRecord> {
        self.scripts.set(vec![calls(items), done()]);
        let run = agent_support::next_prompt(&self.agent, execution, "next")
            .await
            .unwrap();
        let id = run.run_id().clone();
        assert_eq!(
            run.done().await.unwrap().status,
            crabber::core::RunStatus::Completed
        );
        self.records(execution, &id).await
    }
    pub async fn close(mut self) {
        let report = self.agent.close(Duration::from_secs(10)).await;
        assert_eq!(
            report.observation,
            crabber::CloseObservation::Complete,
            "{report:?}"
        );
        assert!(report.local_drained());
        let groups: Vec<_> = std::fs::read_dir(self.root.path())
            .unwrap()
            .map(Result::unwrap)
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()?
                    .strip_prefix("pgid.")?
                    .parse::<i32>()
                    .ok()
            })
            .filter_map(rustix::process::Pid::from_raw)
            .collect();
        tokio::time::timeout(Duration::from_secs(5), async {
            while groups
                .iter()
                .any(|pid| rustix::process::test_kill_process_group(*pid).is_ok())
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        self.cleanup.disarm_groups();
    }
}
pub fn seed_calls() -> Vec<StreamDelta> {
    calls(&[
        (
            jobs::START_TOOL,
            json!({"command":"printf '%s' \"$PPID\" > \"$FIXTURE_ROOT/pgid.$PPID\"; sleep 60"}),
        ),
        (
            python::EXECUTE_TOOL,
            json!({"code":"import os,pathlib; pathlib.Path(os.environ['FIXTURE_ROOT'],'pgid.'+str(os.getpgrp())).write_text(str(os.getpgrp())); x=globals().get('x',0)+1; (x,os.getcwd())"}),
        ),
        (
            delegate::TOOL_NAME,
            json!({"task":"inline","profile":"test"}),
        ),
        (
            delegate::START_TOOL,
            json!({"task":"background","profile":"test"}),
        ),
    ])
}

pub fn runner() -> delegate::Runner {
    Arc::new(|request| {
        Box::pin(async move {
            if request.background() {
                request.cancellation().cancelled().await;
            }
            Ok(delegate::Response::Completed("owned".into()))
        })
    })
}
pub fn record_code(code: &str) -> String {
    format!(
        "import os,pathlib; pathlib.Path(os.environ['FIXTURE_ROOT'],'pgid.'+str(os.getpgrp())).write_text(str(os.getpgrp())); {code}"
    )
}
