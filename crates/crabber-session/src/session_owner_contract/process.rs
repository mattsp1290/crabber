//! Physical controller death before commit and after commit, then cold replay.
use super::*;
#[path = "process/resources.rs"]
mod resources;
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Operation {
    Resources(crate::session_owner::preparation::ReservePreparation),
    VerifyResources {
        request: crate::session_owner::preparation::ReservePreparation,
        committed: bool,
    },
    RequestCancel(crate::session_owner::RequestSessionOwnerCancel),
    ActiveUntilKilled(ReserveSessionOwner),
    VerifyCancel {
        request: crate::session_owner::RequestSessionOwnerCancel,
        committed: bool,
    },
    Reserve(ReserveSessionOwner),
    Preparation(crate::session_owner::preparation::ReservePreparation),
    VerifyPreparation {
        request: crate::session_owner::preparation::ReservePreparation,
        committed: bool,
    },
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
        Operation::Resources(request) => resources::retain(store, request).await,
        Operation::VerifyResources { request, committed } => {
            resources::verify(store, request, committed).await;
        }
        Operation::RequestCancel(request) => {
            store.request_session_owner_cancel(request).await.unwrap();
        }
        Operation::ActiveUntilKilled(request) => {
            let (receipt, authority) =
                applied(store.reserve_session_owner(request, None).await.unwrap());
            store
                .mutate_session_owner(
                    mutation(&receipt.owner, SessionOwnerCommand::Begin),
                    authority,
                )
                .await
                .unwrap();
            pause("active");
        }
        Operation::VerifyCancel { request, committed } => {
            let receipt = store
                .lookup_session_owner(&request.session_id, &request.id)
                .await
                .unwrap();
            assert_eq!(receipt.is_some(), committed);
            if let Some(receipt) = receipt {
                assert!(
                    matches!(store.request_session_owner_cancel(request.clone()).await.unwrap(),SessionOwnerOutcome::Replayed(saved) if saved==receipt)
                );
            }
            let owner = store
                .get_session_owner(&request.session_id, &request.target.id)
                .await
                .unwrap();
            assert_ne!(owner.state, SessionOwnerState::Joined);
            assert_eq!(owner.cancellation.is_some(), committed);
            assert!(
                owner
                    .cancellation
                    .is_none_or(|cancel| cancel.observed.is_none())
            );
            assert!(
                store
                    .get_session(&request.session_id)
                    .await
                    .unwrap()
                    .is_none()
            );
        }

        Operation::Reserve(request) => {
            store.reserve_session_owner(request, None).await.unwrap();
        }
        Operation::Preparation(request) => {
            store.reserve_preparation(request).await.unwrap();
        }
        Operation::VerifyPreparation { request, committed } => {
            verify_preparation(store, request, committed).await;
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
    run_preparation(store, helper, path).await;
    resources::run(helper, path).await;
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

async fn verify_preparation(
    store: &impl Store,
    request: crate::session_owner::preparation::ReservePreparation,
    committed: bool,
) {
    let session = &request.owner.binding.session_id;
    let original = store
        .lookup_preparation(session, &request.key)
        .await
        .unwrap();
    if committed {
        assert_eq!(original, Some(request.reconciliation().unwrap()));
        assert!(matches!(
            store.reserve_preparation(request.clone()).await.unwrap(),
            SessionOwnerOutcome::Replayed(_)
        ));
        let begin = MutateSessionOwner {
            id: request.reconciliation().unwrap().begin_id,
            session_id: session.clone(),
            target: original.unwrap().owner,
            command: SessionOwnerCommand::Begin,
        };
        assert_eq!(
            store.mutate_session_owner(begin, None).await.unwrap_err(),
            SessionOwnerError::StaleOwner
        );
    } else {
        assert!(original.is_none());
    }
    assert!(store.get_session(session).await.unwrap().is_none());
}
async fn run_preparation(_store: &impl Store, helper: &str, path: Option<&Path>) {
    use crate::session_owner::preparation::{
        PreparationKey, PreparationPurpose, ReservePreparation,
    };
    for committed in [false, true] {
        let request = ReservePreparation {
            owner: reservation(SessionId::new()),
            source: None,
            key: PreparationKey::Unkeyed(crabber_core::ControlId::new()),
            expected_revision: crabber_core::SessionRevision::INITIAL,
            purpose: PreparationPurpose::OrdinaryAdmission,
            intent: crabber_core::InputFingerprint::new("b".repeat(64)).unwrap(),
        }
        .bind_key(PreparationKey::Unkeyed(crabber_core::ControlId::new()))
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let marker_path = directory.path().join("preparation-commit");
        let mut child = spawn(
            helper,
            &Operation::Preparation(request.clone()),
            path,
            Some(if committed { "after" } else { "before" }),
            &marker_path,
        );
        marker(&marker_path).await;
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let mut cold = spawn(
            helper,
            &Operation::VerifyPreparation { request, committed },
            path,
            None,
            &marker_path,
        );
        joined(&mut cold).await;
    }
}

/// Both request commit boundaries survive process death, and a genuinely killed
/// Active worker never supplies signal acknowledgement or cleanup evidence.
pub(crate) async fn run_cancellation(store: &impl Store, helper: &str, path: Option<&Path>) {
    use crate::session_owner::cancellation_contract as contract;
    for committed in [false, true] {
        let (owner, _) = contract::reserve(store).await;
        let request = contract::request(&owner, None);
        let dir = tempfile::tempdir().unwrap();
        let marker_path = dir.path().join("cancel");
        let mut child = spawn(
            helper,
            &Operation::RequestCancel(request.clone()),
            path,
            Some(if committed { "after" } else { "before" }),
            &marker_path,
        );
        marker(&marker_path).await;
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let mut cold = spawn(
            helper,
            &Operation::VerifyCancel { request, committed },
            path,
            None,
            &marker_path,
        );
        joined(&mut cold).await;
    }
    let reserve = reservation(SessionId::new());
    let dir = tempfile::tempdir().unwrap();
    let marker_path = dir.path().join("active");
    let mut child = spawn(
        helper,
        &Operation::ActiveUntilKilled(reserve.clone()),
        path,
        Some("active"),
        &marker_path,
    );
    marker(&marker_path).await;
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let owner = store
        .get_session_owner(&reserve.binding.session_id, &reserve.binding.id)
        .await
        .unwrap();
    assert_eq!(owner.state, SessionOwnerState::Active);
    let request = contract::request(&owner, None);
    store
        .request_session_owner_cancel(request.clone())
        .await
        .unwrap();
    let mut cold = spawn(
        helper,
        &Operation::VerifyCancel {
            request,
            committed: true,
        },
        path,
        None,
        &marker_path,
    );
    joined(&mut cold).await;
}
