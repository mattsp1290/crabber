use super::*;
use crabber::{
    RuntimeError,
    core::{RunId, ToolCallStatus},
    runtime::{InterruptPolicy, PermissionPolicy},
};
#[path = "resume/mutations.rs"]
mod mutations;
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
fn build(
    store: Arc<MemoryStore>,
    provider: Arc<FakeProvider>,
    ext: Option<Arc<RtkReducer>>,
    executor: Arc<FixtureTool>,
    dir: &Path,
    pause: bool,
) -> Agent {
    let mut b = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store)
        .provider(provider)
        .config(agent_config(dir))
        .policy(Arc::new(PausePolicy(pause)))
        .shared_tool(tool("fixture_text", executor));
    if let Some(ext) = ext {
        b = b.shared_extension(ext, Scope::Global);
    }
    b.build().unwrap()
}
struct Paused {
    dir: tempfile::TempDir,
    options: Options,
    ext: Arc<RtkReducer>,
    agent: Agent,
    store: Arc<MemoryStore>,
    executor: Arc<FixtureTool>,
    run: RunId,
    session: SessionId,
}
async fn pause() -> Paused {
    let dir = tempfile::tempdir().unwrap();
    let options = options(dir.path(), "/bin/cat >/dev/null; printf R:x");
    let ext = Arc::new(RtkReducer::new(options.clone()).unwrap());
    let store = Arc::new(MemoryStore::new());
    let executor = Arc::new(FixtureTool::new(json!("x".repeat(300))));
    let provider = Arc::new(FakeProvider::scripted(vec![call(
        "fixture_text",
        json!({}),
    )]));
    let agent = build(
        store.clone(),
        provider,
        Some(ext.clone()),
        executor.clone(),
        dir.path(),
        true,
    );
    let run = agent.prompt(None, "pause").await.unwrap();
    let session = run.session_id().clone();
    let id = run.run_id().clone();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Paused);
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, ToolCallStatus::Pending);
    assert_eq!(executor.executions.load(Ordering::Relaxed), 0);
    assert!(!dir.path().join("argv.log").exists());
    Paused {
        dir,
        options,
        ext,
        agent,
        store,
        executor,
        run: id,
        session,
    }
}
async fn persistence(p: &Paused) -> String {
    format!(
        "{:?}|{:?}|{:?}|{:?}",
        p.store.get_run(&p.run).await.unwrap(),
        snapshot_tools(&p.store, &p.session).await,
        p.store.list_all_messages(&p.session).await.unwrap(),
        p.store.list_events(&p.session, None, 100).await.unwrap()
    )
}
async fn resumed(p: &Paused, options: Options) {
    let temp_root = options.temp_root.clone();
    let ext = Arc::new(RtkReducer::new(options).unwrap());
    assert_eq!(ext.config_hash(), p.ext.config_hash());
    let provider = Arc::new(FakeProvider::scripted(vec![done()]));
    let agent = build(
        p.store.clone(),
        provider.clone(),
        Some(ext.clone()),
        p.executor.clone(),
        p.dir.path(),
        false,
    );
    assert_eq!(
        agent.resume(&p.run).await.unwrap().status,
        RunStatus::Completed
    );
    let records = snapshot_tools(&p.store, &p.session).await;
    assert_eq!(records[0].status, ToolCallStatus::Completed);
    observe::assert_three_surfaces(
        &p.store,
        &p.session,
        &provider,
        &records[0].id,
        &json!(marker(Filter::Log, 300) + "R:x"),
    )
    .await;
    assert_eq!(p.executor.executions.load(Ordering::Relaxed), 1);
    assert_eq!(
        fs::read_to_string(p.dir.path().join("argv.log")).unwrap(),
        "pipe --filter log\n"
    );
    agent.close_extensions().await.unwrap();
    p.agent.close_extensions().await.unwrap();
    groups_gone(p.dir.path()).await;
    assert_eq!(ext.live_children(), 0);
    assert_eq!(fs::read_dir(temp_root).unwrap().count(), 0);
}
#[tokio::test(flavor = "multi_thread")]
async fn paused_pending_call_reduces_once_on_resume() {
    if !shell_available() {
        return;
    }
    let p = pause().await;
    resumed(&p, p.options.clone()).await;
}
#[tokio::test(flavor = "multi_thread")]
async fn executable_path_and_temp_root_do_not_change_the_fingerprint() {
    if !shell_available() {
        return;
    }
    for root_change in [false, true] {
        let p = pause().await;
        let mut o = p.options.clone();
        if root_change {
            o.temp_root = p.dir.path().join("other-private");
            fs::create_dir(&o.temp_root).unwrap();
            fs::set_permissions(&o.temp_root, fs::Permissions::from_mode(0o700)).unwrap();
        } else {
            let path = p.dir.path().join("relocated");
            fs::copy(&o.executable_path, &path).unwrap();
            o.executable_path = path;
        }
        resumed(&p, o).await;
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn resume_refuses_every_drift_before_mutation() {
    if !shell_available() {
        return;
    }
    let mut edits = mutations::mutations();
    assert_eq!(edits.len() + 1, 21);
    edits.extend(mutations::additional_mutations());
    assert_eq!(edits.len() + 1, 24);
    for row in 0..edits.len() + 1 {
        let p = pause().await;
        let mut o = p.options.clone();
        let label = if row < edits.len() {
            (edits[row].1)(&mut o);
            edits[row].0
        } else {
            "mount_removed"
        };
        eprintln!("RTK resume drift: {label}");
        let changed = Arc::new(RtkReducer::new(o).unwrap());
        let provider = Arc::new(FakeProvider::scripted(vec![done()]));
        let agent = build(
            p.store.clone(),
            provider.clone(),
            (row < edits.len()).then(|| changed.clone()),
            p.executor.clone(),
            p.dir.path(),
            false,
        );
        let before = persistence(&p).await;
        assert!(
            matches!(agent.resume(&p.run).await, Err(RuntimeError::PlanChanged)),
            "{label}"
        );
        assert_eq!(before, persistence(&p).await, "{label}");
        assert!(provider.requests().is_empty());
        assert_eq!(p.executor.executions.load(Ordering::Relaxed), 0);
        assert!(!p.dir.path().join("argv.log").exists());
        assert_eq!(changed.live_children() + p.ext.live_children(), 0);
        assert_eq!(fs::read_dir(&p.options.temp_root).unwrap().count(), 0);
        agent.close_extensions().await.unwrap();
        p.agent.close_extensions().await.unwrap();
    }
}
