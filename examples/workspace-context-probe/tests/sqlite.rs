//! Fresh-process resume and identity-drift proofs on an isolated SQLite file.
#![cfg(feature = "sqlite")]

use crabber::{
    RuntimeError, SessionId,
    core::{CoreError, RunId, RunStatus},
    session::SqliteStore,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::process::Command;
use workspace_context_probe::{Identity, Observations, config, host, identity, text, tool_call};

const CHILD: &str = "CRABBER_SQLITE_PROBE_CHILD";
const HANDOFF: &str = "CRABBER_SQLITE_PROBE_HANDOFF";

/// Runs `fresh_host_child` in a new OS process: its own pool, registry and agent.
async fn fresh_host(dir: &Path, mode: &str) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "fresh_host_child", "--nocapture"])
        .env(CHILD, mode)
        .env(HANDOFF, dir)
        .env("CRABBER_SQLITE_CHILD_PATH", dir.join("session.sqlite"));
    let status = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        command.kill_on_drop(true).status(),
    )
    .await
    .expect("fresh host timed out")
    .unwrap();
    assert!(status.success(), "fresh host process failed in mode {mode}");
}

fn observed(dir: &Path, name: &str) -> (Vec<Identity>, Vec<Identity>, Vec<bool>) {
    // libtest exits successfully when `--exact fresh_host_child` matches no
    // test, so a missing handoff file means the child never ran.
    let bytes = std::fs::read(dir.join(name)).unwrap_or_else(|error| {
        panic!("fresh host wrote no {name} ({error}); does `--exact fresh_host_child` still match?")
    });
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn fresh_host_resumes_paused_run_with_the_persisted_workspace() {
    let dir = tempfile::Builder::new()
        .prefix("crabber-probe-")
        .tempdir()
        .unwrap();
    let path = dir.path().join("session.sqlite");
    SqliteStore::migrate(&path).await.unwrap();
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
    .sqlite(&path)
    .await
    .unwrap()
    .build()
    .unwrap();
    let run = agent.prompt(None, "first").await.unwrap();
    let (session, run_id) = (run.session_id().clone(), run.run_id().clone());
    assert_eq!(run.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(seen.tool(), [] as [Identity; 0]);
    drop(agent);

    std::fs::write(
        dir.path().join("run.json"),
        serde_json::to_vec(&(&session, &run_id)).unwrap(),
    )
    .unwrap();

    // A freshly started host, configured with other defaults, resumes it.
    fresh_host(dir.path(), "resume").await;
    let (tool, assemble, cancelled) = observed(dir.path(), "observed.json");
    let expected = identity(Some(&workspace_id), Some(&directory));
    assert_eq!((tool, assemble), (vec![expected.clone()], vec![expected]));
    assert_eq!(cancelled, [false]);

    // A freshly started host presenting a replacement root is rejected.
    std::fs::write(
        dir.path().join("identity.json"),
        serde_json::to_vec(&(&workspace_id, "/srv/replacement")).unwrap(),
    )
    .unwrap();
    fresh_host(dir.path(), "drift").await;
    let (tool, assemble, cancelled) = observed(dir.path(), "rejected.json");
    assert_eq!((tool, assemble), (Vec::new(), Vec::new()));
    assert_eq!(cancelled, [] as [bool; 0]);
}

// Invoked by the parent test in a genuinely fresh OS process, never recursively.
#[tokio::test(flavor = "multi_thread")]
async fn fresh_host_child() {
    let Ok(mode) = std::env::var(CHILD) else {
        return;
    };
    let dir = PathBuf::from(std::env::var_os(HANDOFF).unwrap());
    let path = PathBuf::from(std::env::var_os("CRABBER_SQLITE_CHILD_PATH").unwrap());
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
        .sqlite(&path)
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
            serde_json::to_vec(&(
                seen.tool(),
                seen.assemble(),
                seen.executions()
                    .iter()
                    .map(|entry| entry.cancelled_at_entry)
                    .collect::<Vec<_>>(),
            ))
            .unwrap(),
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
        .sqlite(&path)
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
            serde_json::to_vec(&(
                seen.tool(),
                seen.assemble(),
                seen.executions()
                    .iter()
                    .map(|entry| entry.cancelled_at_entry)
                    .collect::<Vec<_>>(),
            ))
            .unwrap(),
        )
        .unwrap();
    }
}
