use super::*;
use crabber::JsonToolExecutor;

struct PausePolicy(bool);
impl PermissionPolicy for PausePolicy {
    fn decide(&self, _: &ToolInfo, _: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(&self, _: &ToolInfo, _: &Value) -> InterruptPolicy {
        if self.0 {
            InterruptPolicy::Pause
        } else {
            InterruptPolicy::Continue
        }
    }
}
struct Noop;
#[async_trait::async_trait]
impl JsonToolExecutor for Noop {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Ok(json!("ok"))
    }
}
fn noop() -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: "fixture_noop".into(),
            description: "Synthetic no-op".into(),
            parameters: json!({"type":"object"}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(Noop),
    })
}
pub(super) fn build(
    store: Arc<MemoryStore>,
    ext: Option<Arc<PythonRepl>>,
    dir: &Path,
    pause: bool,
    scripts: Vec<Vec<StreamDelta>>,
) -> Agent {
    let mut builder = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store)
        .provider(Arc::new(FakeProvider::scripted(scripts)))
        .config(agent_config(dir))
        .policy(Arc::new(PausePolicy(pause)))
        .shared_tool(noop());
    if let Some(ext) = ext {
        builder = builder.shared_extension(ext, Scope::Global);
    }
    builder.build().unwrap()
}
pub(super) struct Paused {
    pub(super) dir: tempfile::TempDir,
    pub(super) store: Arc<MemoryStore>,
    pub(super) ext: Arc<PythonRepl>,
    pub(super) agent: Agent,
    pub(super) run: RunId,
    pub(super) session: SessionId,
    pub(super) options: Options,
    pub(super) mounted: bool,
}
pub(super) async fn pause(mounted: bool) -> Paused {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("temporary")).unwrap();
    let mut o = options(dir.path());
    // Inherited-mode drift must be valid without depending on host entry count.
    o.limits.max_environment_entries = 512;
    o.limits.max_environment_bytes = 131072;
    let ext = Arc::new(PythonRepl::new(o.clone()).unwrap());
    let store = Arc::new(MemoryStore::new());
    let script = if mounted {
        call(
            EXECUTE_TOOL,
            json!({"code":record_code("import pathlib,os; p=pathlib.Path(os.environ['FIXTURE_ROOT'],'executions'); p.write_text((p.read_text() if p.exists() else '')+'x'); len(p.read_text())")}),
        )
    } else {
        call("fixture_noop", json!({}))
    };
    let agent = build(
        store.clone(),
        mounted.then(|| ext.clone()),
        dir.path(),
        true,
        vec![script],
    );
    let run = agent.prompt(None, "pause").await.unwrap();
    let session = run.session_id().clone();
    let id = run.run_id().clone();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Paused);
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, ToolCallStatus::Pending);
    assert_eq!(ext.live_runners(), 0);
    assert!(pgids(dir.path()).is_empty());
    Paused {
        dir,
        store,
        ext,
        agent,
        run: id,
        session,
        options: o,
        mounted,
    }
}
pub(super) async fn resumed(paused: &Paused, ext: Arc<PythonRepl>) -> Agent {
    let a = build(
        paused.store.clone(),
        paused.mounted.then(|| ext.clone()),
        paused.dir.path(),
        false,
        vec![done()],
    );
    assert_eq!(
        a.resume(&paused.run).await.unwrap().status,
        RunStatus::Completed
    );
    let records = snapshot_tools(&paused.store, &paused.session).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, ToolCallStatus::Completed);
    if paused.mounted {
        assert_eq!(result_value(&records[0])["result"]["text"], "1");
        assert_eq!(result_value(&records[0])["generation"], 0);
        assert_eq!(
            std::fs::read_to_string(paused.dir.path().join("executions")).unwrap(),
            "x"
        );
        assert_eq!(ext.live_runners(), 1);
    } else {
        assert_eq!(ext.live_runners(), 0);
    }
    a
}
pub(super) type Edit = fn(&mut Options);
pub(super) fn mutations() -> Vec<(&'static str, Edit)> {
    vec![
        ("max_sessions", |o| o.limits.max_sessions += 1),
        ("max_queued_per_session", |o| {
            o.limits.max_queued_per_session += 1
        }),
        ("max_code_bytes", |o| o.limits.max_code_bytes += 1),
        ("max_output_bytes_per_stream", |o| {
            o.limits.max_output_bytes_per_stream += 1
        }),
        ("max_result_bytes", |o| o.limits.max_result_bytes += 1),
        ("max_exception_bytes", |o| o.limits.max_exception_bytes += 1),
        ("max_environment_entries", |o| {
            o.limits.max_environment_entries += 1
        }),
        ("max_environment_bytes", |o| {
            o.limits.max_environment_bytes += 1
        }),
        ("default_timeout", |o| {
            o.limits.default_timeout += Duration::from_secs(1)
        }),
        ("max_timeout", |o| {
            o.limits.max_timeout += Duration::from_secs(1)
        }),
        ("runner_start_timeout", |o| {
            o.limits.runner_start_timeout += Duration::from_secs(1)
        }),
        ("terminate_grace", |o| {
            o.limits.terminate_grace += Duration::from_secs(1)
        }),
        ("kill_wait", |o| {
            o.limits.kill_wait += Duration::from_secs(1)
        }),
        ("shutdown_grace", |o| {
            o.limits.shutdown_grace += Duration::from_secs(1)
        }),
        ("python_identity", |o| o.python_identity.push('x')),
        ("environment.identity", |o| o.environment.identity.push('x')),
        ("environment.mode", |o| {
            o.environment.mode = EnvironmentMode::InheritAndOverride
        }),
    ]
}
pub(super) async fn persistence(p: &Paused) -> String {
    let snapshot = p
        .store
        .snapshot(SnapshotRequest {
            session_id: p.session.clone(),
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
        .unwrap();
    format!(
        "{:?}|{:?}|{:?}",
        p.store.get_run(&p.run).await.unwrap(),
        snapshot,
        p.store.list_events(&p.session, None, 100).await.unwrap()
    )
}
