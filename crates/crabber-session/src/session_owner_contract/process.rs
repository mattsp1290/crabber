//! Physical controller death before commit and after commit, then cold replay.
use super::*;
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Operation {
    Reserve(ReserveSessionOwner),
    Mutate(MutateSessionOwner),
    Verify {
        reserve: ReserveSessionOwner,
        mutation: Option<MutateSessionOwner>,
        committed: bool,
    },
}
pub(crate) fn command() -> Option<Operation> {
    std::env::var("CRABBER_OWNER_CHILD")
        .ok()
        .map(|data| serde_json::from_str(&data).unwrap())
}
pub(crate) fn pause(point: &str) {
    if std::env::var("CRABBER_OWNER_KILL_POINT").ok().as_deref() == Some(point) {
        std::fs::write(std::env::var("CRABBER_OWNER_MARKER").unwrap(), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }
}
pub(crate) async fn execute(store: &impl Store, command: Operation) {
    match command {
        Operation::Reserve(request) => {
            store.reserve_session_owner(request, None).await.unwrap();
        }
        Operation::Mutate(request) => {
            store.mutate_session_owner(request, None).await.unwrap();
        }
        Operation::Verify {
            reserve,
            mutation,
            committed,
        } => {
            let session = &reserve.binding.session_id;
            let id = mutation.as_ref().map_or(&reserve.id, |request| &request.id);
            let receipt = store.lookup_session_owner(session, id).await.unwrap();
            if committed {
                let receipt = receipt.unwrap();
                let outcome = if let Some(mutation) = mutation.clone() {
                    store.mutate_session_owner(mutation, None).await.unwrap()
                } else {
                    store
                        .reserve_session_owner(reserve.clone(), None)
                        .await
                        .unwrap()
                };
                assert!(matches!(outcome,SessionOwnerOutcome::Replayed(saved) if saved==receipt));
                let begin = MutateSessionOwner {
                    id: SessionOwnerOperationId::new(),
                    session_id: session.clone(),
                    target: receipt.owner.target,
                    command: SessionOwnerCommand::Begin,
                };
                assert!(matches!(
                    store.mutate_session_owner(begin, None).await,
                    Err(SessionOwnerError::StaleOwner)
                ));
            } else {
                assert!(receipt.is_none());
            }
            // A pre-session reservation cannot manufacture ordinary session metadata.
            assert!(store.session_summary(session).await.is_err());
        }
    }
}
struct Guard(Child);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn spawn(
    helper: &str,
    operation: &Operation,
    path: Option<&Path>,
    point: Option<&str>,
    marker: &Path,
) -> Guard {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", helper, "--nocapture"])
        .env(
            "CRABBER_OWNER_CHILD",
            serde_json::to_string(operation).unwrap(),
        )
        .stdout(Stdio::null());
    if let Some(path) = path {
        cmd.env("CRABBER_SQLITE_CHILD_PATH", path);
    }
    if let Some(point) = point {
        cmd.env("CRABBER_OWNER_KILL_POINT", point)
            .env("CRABBER_OWNER_MARKER", marker);
    } else {
        cmd.env_remove("CRABBER_OWNER_KILL_POINT");
    }
    Guard(cmd.spawn().unwrap())
}
async fn marker(path: &Path) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owner child reached commit boundary");
}
async fn joined(child: &mut Guard) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success(), "owner child: {status}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
pub(crate) async fn run(store: &impl Store, helper: &str, path: Option<&Path>) {
    for mutate in [false, true] {
        for committed in [false, true] {
            let reserve = reservation(SessionId::new());
            let mutation = if mutate {
                let (receipt, _) = applied(
                    store
                        .reserve_session_owner(reserve.clone(), None)
                        .await
                        .unwrap(),
                );
                Some(mutation(
                    &receipt.owner,
                    SessionOwnerCommand::ReclaimUnstarted {
                        owner: "cold-controller".into(),
                        lease_ms: 100,
                    },
                ))
            } else {
                None
            };
            let command = mutation
                .clone()
                .map_or_else(|| Operation::Reserve(reserve.clone()), Operation::Mutate);
            let dir = tempfile::tempdir().unwrap();
            let path_marker = dir.path().join("owner-commit");
            let mut child = spawn(
                helper,
                &command,
                path,
                Some(if committed { "after" } else { "before" }),
                &path_marker,
            );
            marker(&path_marker).await;
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let mut verify = spawn(
                helper,
                &Operation::Verify {
                    reserve,
                    mutation,
                    committed,
                },
                path,
                None,
                &path_marker,
            );
            joined(&mut verify).await;
        }
    }
}
