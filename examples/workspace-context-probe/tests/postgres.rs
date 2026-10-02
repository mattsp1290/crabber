//! Fresh-process proofs against a real PostgreSQL store, public API only.
//!
//! Set `CRABBER_TEST_POSTGRES_URL` to a disposable database. Without it the
//! tests skip, unless `CRABBER_REQUIRE_POSTGRES=1` makes its absence a failure.
#![cfg(feature = "postgres")]

use crabber::{
    RuntimeError, SessionId,
    core::{CoreError, RunId, RunStatus},
    session::PostgresStore,
};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};
use workspace_context_probe::{Identity, Observations, config, host, identity, text, tool_call};

const CHILD: &str = "CRABBER_PROBE_CHILD";
const HANDOFF: &str = "CRABBER_PROBE_HANDOFF";

fn postgres_url() -> Option<String> {
    let url = std::env::var("CRABBER_TEST_POSTGRES_URL").ok();
    if url.is_none() {
        assert!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref() != Ok("1"),
            "CRABBER_TEST_POSTGRES_URL is required"
        );
        eprintln!("workspace context probe skipped: CRABBER_TEST_POSTGRES_URL unset");
    }
    url
}

struct Handoff(PathBuf);
impl Drop for Handoff {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Runs `fresh_host_child` in a new OS process: its own pool, registry and agent.
fn fresh_host(dir: &Path, mode: &str) {
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "fresh_host_child", "--nocapture"])
        .env(CHILD, mode)
        .env(HANDOFF, dir)
        .status()
        .unwrap();
    assert!(status.success(), "fresh host process failed in mode {mode}");
}

fn observed(dir: &Path, name: &str) -> (Vec<Identity>, Vec<Identity>) {
    serde_json::from_slice(&std::fs::read(dir.join(name)).unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn fresh_host_resumes_paused_run_with_the_persisted_workspace() {
    let Some(url) = postgres_url() else { return };
    PostgresStore::migrate(&url).await.unwrap();
    let workspace_id = format!("ws-{}", SessionId::new());
    let directory = format!("/srv/{workspace_id}");

    // This host admits the run and pauses before the tool executes.
    let seen = Arc::new(Observations::default());
    let agent = host(
        &seen,
        vec![tool_call()],
        config(&workspace_id, &directory),
        true,
    )
    .postgres(&url)
    .await
    .unwrap()
    .build()
    .unwrap();
    let run = agent.prompt(None, "first").await.unwrap();
    let (session, run_id) = (run.session_id().clone(), run.run_id().clone());
    assert_eq!(run.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(seen.tool(), [] as [Identity; 0]);
    drop(agent);

    let dir = Handoff(std::env::temp_dir().join(format!("crabber-probe-{session}")));
    std::fs::create_dir(&dir.0).unwrap();
    std::fs::write(
        dir.0.join("run.json"),
        serde_json::to_vec(&(&session, &run_id)).unwrap(),
    )
    .unwrap();

    // A freshly started host, configured with other defaults, resumes it.
    fresh_host(&dir.0, "resume");
    let (tool, assemble) = observed(&dir.0, "observed.json");
    let expected = identity(Some(&workspace_id), Some(&directory));
    assert_eq!((tool, assemble), (vec![expected.clone()], vec![expected]));

    // A freshly started host presenting a replacement root is rejected.
    std::fs::write(
        dir.0.join("identity.json"),
        serde_json::to_vec(&(&workspace_id, "/srv/replacement")).unwrap(),
    )
    .unwrap();
    fresh_host(&dir.0, "drift");
    let (tool, assemble) = observed(&dir.0, "rejected.json");
    assert_eq!((tool, assemble), (Vec::new(), Vec::new()));
}

// Invoked by the parent test in a genuinely fresh OS process, never recursively.
#[tokio::test(flavor = "multi_thread")]
async fn fresh_host_child() {
    let Ok(mode) = std::env::var(CHILD) else {
        return;
    };
    let dir = PathBuf::from(std::env::var_os(HANDOFF).unwrap());
    let url = std::env::var("CRABBER_TEST_POSTGRES_URL").unwrap();
    let (session, run_id): (SessionId, RunId) =
        serde_json::from_slice(&std::fs::read(dir.join("run.json")).unwrap()).unwrap();
    let seen = Arc::new(Observations::default());
    if mode == "resume" {
        let agent = host(
            &seen,
            vec![text("done")],
            config("fresh-host-default", "/fresh/host/cwd"),
            false,
        )
        .postgres(&url)
        .await
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(
            agent.resume(&run_id).await.unwrap().status,
            RunStatus::Completed
        );
        std::fs::write(
            dir.join("observed.json"),
            serde_json::to_vec(&(seen.tool(), seen.assemble())).unwrap(),
        )
        .unwrap();
    } else {
        let (workspace_id, directory): (String, String) =
            serde_json::from_slice(&std::fs::read(dir.join("identity.json")).unwrap()).unwrap();
        let agent = host(
            &seen,
            vec![tool_call(), text("never")],
            config(&workspace_id, &directory),
            false,
        )
        .postgres(&url)
        .await
        .unwrap()
        .build()
        .unwrap();
        assert!(matches!(
            agent.prompt(Some(session), "again").await,
            Err(RuntimeError::Store(CoreError::SessionIdentityMismatch))
        ));
        std::fs::write(
            dir.join("rejected.json"),
            serde_json::to_vec(&(seen.tool(), seen.assemble())).unwrap(),
        )
        .unwrap();
    }
}
