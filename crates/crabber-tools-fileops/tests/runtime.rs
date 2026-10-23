//! Public runtime cancellation, mismatch, policy and panic recovery.
use crabber::{
    Agent, FakeProvider, PermissionDecision, StaticPolicy, core::ToolCallStatus,
    session::MemoryStore,
};
use crabber_tools_core::*;
use crabber_tools_fileops::{self as fileops, Options};
use crabber_tools_testkit as support;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use support::*;
fn options(path: &std::path::Path, hook: Option<Arc<dyn Fn() + Send + Sync>>) -> Arc<Options> {
    let limits = limits();
    Arc::new(Options {
        deny: crabber_tools_core::PathDenyPolicy::none(),
        root: WorkspaceRoot::open(path).unwrap(),
        capacity: Capacity::new(&limits).unwrap(),
        limits,
        pre_write: hook,
    })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_atomic_write_finishes_with_owned_guard() {
    let temp = tempfile::tempdir().unwrap();
    let (start_tx, mut start_rx) = tokio::sync::mpsc::unbounded_channel();
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let closure_gate = gate.clone();
    let hook = Arc::new(move || {
        start_tx.send(()).unwrap();
        let (lock, cv) = &*closure_gate;
        let mut release = lock.lock().unwrap();
        while !*release {
            release = cv.wait(release).unwrap();
        }
    });
    let o = options(temp.path(), Some(hook));
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("file_write", json!({"path":"target","content":"complete"})),
            done(),
        ])))
        .config(config(temp.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(fileops::write::definition(o.clone()))
        .build()
        .unwrap();
    let run = agent.prompt(None, "write").await.unwrap();
    let session = run.session_id().clone();
    tokio::time::timeout(Duration::from_secs(2), start_rx.recv())
        .await
        .unwrap();
    let interrupted_run = run.run_id().clone();
    run.interrupt();
    support::assert_stopped_uncertain(run, &store).await;
    assert_eq!(
        calls(&store, &session).await[0].status,
        ToolCallStatus::Running
    );
    let incomplete = agent.drain_execution_tasks(Duration::from_millis(10)).await;
    let resources = &incomplete
        .resources
        .iter()
        .find(|r| r.run_id == interrupted_run)
        .unwrap()
        .resources;
    assert!(resources.sealed);
    assert!(resources.pending >= 1);
    assert!(!temp.path().join("target").exists());
    // The write is already running off-thread: interruption cannot release its writer lock.
    let short = Limits {
        max_blocking_wait: Duration::from_millis(20),
        ..limits()
    };
    assert!(matches!(
        acquire_mutating(
            &RootLock::for_root(&o.root),
            &o.capacity,
            &short,
            &tokio_util::sync::CancellationToken::new()
        )
        .await,
        Err(AcquireError::Unavailable)
    ));
    {
        let (lock, cv) = &*gate;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
    wait_file(&temp.path().join("target")).await;
    let _guard = acquire_mutating(
        &RootLock::for_root(&o.root),
        &o.capacity,
        &limits(),
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(temp.path().join("target")).unwrap(),
        "complete"
    );
    assert!(std::fs::read_dir(temp.path()).unwrap().all(|e| {
        !e.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(fileops::TEMP_PREFIX)
    }));
    let complete = agent.drain_execution_tasks(Duration::from_secs(2)).await;
    let resources = &complete
        .resources
        .iter()
        .find(|r| r.run_id == interrupted_run)
        .unwrap()
        .resources;
    assert_eq!(resources.pending, 0);
    assert!(resources.joined >= 1);
    assert_eq!(
        resources.failures,
        [] as [crabber::extension::ResourceFailure; 0]
    );
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn mismatch_permissions_and_panic() {
    let temp = tempfile::tempdir().unwrap();
    let sibling = tempfile::tempdir().unwrap();
    let o = options(temp.path(), None);
    std::fs::write(temp.path().join("x"), "hello").unwrap();
    for (tool, args) in [
        (
            fileops::write::definition(o.clone()),
            json!({"path":"x","content":"bad"}),
        ),
        (fileops::read::definition(o.clone()), json!({"path":"x"})),
        (
            fileops::edit::definition(o.clone()),
            json!({"path":"x","anchor":"hello","replacement":"bad"}),
        ),
        (fileops::list::definition(o.clone()), json!({})),
    ] {
        assert_eq!(
            execute(sibling.path(), tool, args).await["error"]["category"],
            "workspace_mismatch"
        );
    }
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("file_write", json!({"path":"no","content":"bad"})),
            done(),
        ])))
        .config(config(temp.path()))
        .policy(Arc::new(
            StaticPolicy::new(PermissionDecision::Allow)
                .with_rule("file_write", PermissionDecision::Deny),
        ))
        .shared_tool(fileops::write::definition(o.clone()))
        .build()
        .unwrap();
    let run = agent.prompt(None, "deny").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    assert_eq!(
        calls(&store, &session).await[0].status,
        ToolCallStatus::Failed
    );
    assert!(!temp.path().join("no").exists());
    agent.close_extensions().await.unwrap();
    let panic = options(temp.path(), Some(Arc::new(|| panic!("injected"))));
    assert_eq!(
        execute(
            temp.path(),
            fileops::write::definition(panic),
            json!({"path":"x","content":"bad"})
        )
        .await["error"]["category"],
        "unknown"
    );
    assert_eq!(
        execute(
            temp.path(),
            fileops::write::definition(o),
            json!({"path":"x","content":"good"})
        )
        .await["outcome"],
        "succeeded"
    );
}
