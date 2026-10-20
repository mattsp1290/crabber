//! Real controller death across the atomic root-Join/native-claim commit.
use crate::Store;
use crabber_core::{ManualClock, RunId, SessionId, SessionOwnerId};
use std::{
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
type Key = (SessionId, RunId, u64, SessionOwnerId);
pub(crate) async fn child(store: &impl Store, clock: &ManualClock) {
    let pending = if std::env::var_os("CRABBER_EXPIRED_PARENT").is_some() {
        crate::run_control_contract::expired::unstarted::pending(store, clock).await
    } else if std::env::var_os("CRABBER_EXPIRED_UNKEYED").is_some() {
        crate::run_control_contract::expired::unkeyed::pending(store, clock).await
    } else {
        crate::run_control_contract::expired::pending(store, clock).await
    };
    let source = &pending.source;
    let key = (
        source.request().session_id.clone(),
        source.request().expected_fence.run_id.clone(),
        source.expected_issuance(),
        pending
            .joined
            .observed_grant()
            .owner_authority()
            .target()
            .id
            .clone(),
    );
    std::fs::write(
        std::env::var("CRABBER_EXPIRED_KEY").unwrap(),
        serde_json::to_vec(&key).unwrap(),
    )
    .unwrap();
    Box::pin(store.complete_preparation(pending.joined))
        .await
        .unwrap();
    panic!("controller failed to stop at commit boundary");
}
pub(crate) fn kill(test: &str, stage: &str, path: Option<&Path>, dir: &Path) -> Key {
    kill_kind(test, stage, path, dir, "paused")
}
pub(crate) fn kill_unkeyed(test: &str, stage: &str, path: Option<&Path>, dir: &Path) -> Key {
    kill_kind(test, stage, path, dir, "unkeyed")
}
pub(crate) fn kill_unstarted(test: &str, stage: &str, path: Option<&Path>, dir: &Path) -> Key {
    kill_kind(test, stage, path, dir, "unstarted")
}
fn kill_kind(test: &str, stage: &str, path: Option<&Path>, dir: &Path, kind: &str) -> Key {
    let marker = dir.join(format!("{stage}.marker"));
    let key = dir.join(format!("{stage}.key"));
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", test, "--nocapture"])
        .env("CRABBER_EXPIRED_CHILD", "1")
        .env("CRABBER_CONTROL_KILL_POINT", format!("expired-{stage}"))
        .env("CRABBER_CONTROL_MARKER", &marker)
        .env("CRABBER_EXPIRED_KEY", &key)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if kind == "unkeyed" {
        command.env("CRABBER_EXPIRED_UNKEYED", "1");
    } else if kind == "unstarted" {
        command.env("CRABBER_EXPIRED_PARENT", "1");
    }
    if let Some(path) = path {
        command.env("CRABBER_EXPIRED_DATABASE", path);
    }
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while !marker.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "controller exited before marker: {:?}",
            child.wait_with_output().unwrap()
        );
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("controller did not reach {stage} boundary");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    serde_json::from_slice(&std::fs::read(key).unwrap()).unwrap()
}
pub(crate) async fn verify(store: &impl Store, key: Key, committed: bool) {
    let (session, run, sequence, owner) = key;
    let receipt = store
        .lookup_expired_run_preparation(&session, &run, sequence)
        .await
        .unwrap();
    assert_eq!(receipt.is_some(), committed);
    let current = store.get_session_owner(&session, &owner).await.unwrap();
    assert_eq!(
        current.state,
        if committed {
            crate::session_owner::SessionOwnerState::Joined
        } else {
            crate::session_owner::SessionOwnerState::Active
        }
    );
}
