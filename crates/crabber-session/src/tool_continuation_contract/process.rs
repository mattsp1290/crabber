//! A killed writer and a fresh reader share only committed SQL state.
use super::{ManualClock, Store, fixture};
use crate::{
    ToolContinuationClaim, ToolContinuationClaimOutcome, ToolContinuationReceipt,
    ToolContinuationRecovery, ToolContinuationSuspension, ToolInteractionAnswer,
};
use crabber_core::{Clock, ControlId, InputFingerprint, SessionId};
use crabber_test_support::ChildDatabase;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};

#[derive(Serialize, Deserialize)]
enum Request {
    Suspend(Box<ToolContinuationSuspension>),
    Answer(Box<ToolInteractionAnswer>),
    Claim(ToolContinuationClaim),
    Recover(Box<ToolContinuationRecovery>),
}
#[derive(Serialize, Deserialize)]
struct Saved {
    session: SessionId,
    key: ControlId,
    digest: InputFingerprint,
    request: Request,
}

pub(crate) fn directory() -> Option<PathBuf> {
    std::env::var_os("CRABBER_TOOL_SQL_CHILD").map(PathBuf::from)
}
pub(crate) async fn write(store: &impl Store, clock: &ManualClock, directory: &Path) {
    let step: u8 = std::env::var("CRABBER_TOOL_SQL_STEP")
        .unwrap()
        .parse()
        .unwrap();
    let f = fixture::fixture(store).await;
    let session = f.admitted.session.id.clone();
    let (request, receipt) = if step == 0 {
        let receipt = f
            .execution
            .suspend_tool_continuation(f.suspension.clone(), f.authority)
            .await
            .unwrap();
        (Request::Suspend(Box::new(f.suspension)), receipt)
    } else {
        f.execution
            .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
            .await
            .unwrap();
        if step == 1 {
            let request = fixture::answer(store, &f, 0).await;
            let receipt = store
                .answer_tool_interaction(request.clone())
                .await
                .unwrap();
            (Request::Answer(Box::new(request)), receipt)
        } else {
            for index in 0..2 {
                store
                    .answer_tool_interaction(fixture::answer(store, &f, index).await)
                    .await
                    .unwrap();
            }
            let mut claim = fixture::claim(store, &f).await;
            claim.lease_ms = 1;
            let ToolContinuationClaimOutcome::Applied { receipt, .. } =
                store.claim_tool_continuation(claim.clone()).await.unwrap()
            else {
                panic!("fresh claim")
            };
            if step == 2 {
                (Request::Claim(claim), receipt)
            } else {
                clock.set(clock.now() + time::Duration::milliseconds(2));
                let request = ToolContinuationRecovery {
                    id: ControlId::new(),
                    expected_revision: receipt.revision,
                    original: claim,
                    attempt: f.suspension.next_attempt,
                    owner: "cold-recovery".into(),
                    lease_ms: 30000,
                };
                let ToolContinuationClaimOutcome::Applied { receipt, .. } = store
                    .recover_tool_continuation(request.clone())
                    .await
                    .unwrap()
                else {
                    panic!("fresh recovery")
                };
                (Request::Recover(Box::new(request)), receipt)
            }
        }
    };
    let saved = Saved {
        session,
        key: receipt.operation_id,
        digest: receipt.semantic_digest,
        request,
    };
    std::fs::write(
        directory.join("request"),
        serde_json::to_vec(&saved).unwrap(),
    )
    .unwrap();
    std::fs::write(directory.join("committed"), b"committed").unwrap();
    loop {
        std::thread::park();
    }
}
pub(crate) async fn verify(store: &impl Store, directory: &Path) {
    let saved: Saved =
        serde_json::from_slice(&std::fs::read(directory.join("request")).unwrap()).unwrap();
    let receipt = store
        .lookup_tool_continuation(&saved.session, &saved.key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.semantic_digest, saved.digest);
    let before = store.session_revision(&saved.session).await.unwrap();
    let actual: Option<ToolContinuationReceipt> = match saved.request {
        Request::Suspend(batch) => {
            assert_eq!(
                batch
                    .fingerprint(&receipt.session_id, &receipt.run_id)
                    .unwrap(),
                receipt.semantic_digest
            );
            assert_eq!(
                store
                    .get_run(&receipt.run_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                crabber_core::RunStatus::Paused
            );
            None
        }
        Request::Answer(answer) => Some(store.answer_tool_interaction(*answer).await.unwrap()),
        Request::Claim(claim) => {
            let ToolContinuationClaimOutcome::Replayed(actual) =
                store.claim_tool_continuation(claim).await.unwrap()
            else {
                panic!("cold replay must not dispatch")
            };
            Some(actual)
        }
        Request::Recover(request) => {
            let ToolContinuationClaimOutcome::Replayed(actual) =
                store.recover_tool_continuation(*request).await.unwrap()
            else {
                panic!("cold replay must not recover authority")
            };
            Some(actual)
        }
    };
    if let Some(actual) = actual {
        assert_eq!(actual, receipt);
    }
    assert_eq!(
        store.session_revision(&saved.session).await.unwrap(),
        before
    );
}
struct Guard(Child);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
pub(crate) async fn journey(helper: &str, database: ChildDatabase<'_>) {
    for step in 0..4 {
        let directory = tempfile::tempdir().unwrap();
        let mut writer = spawn(helper, database, directory.path(), step, true);
        crate::queue_contract::process::marker(&directory.path().join("committed")).await;
        writer.0.kill().unwrap();
        writer.0.wait().unwrap();
        let mut reader = spawn(helper, database, directory.path(), step, false);
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = reader.0.try_wait().unwrap() {
                    assert!(status.success(), "cold continuation reader failed");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cold reader timed out");
    }
}
fn spawn(
    helper: &str,
    database: ChildDatabase<'_>,
    directory: &Path,
    step: u8,
    write: bool,
) -> Guard {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", helper, "--nocapture"])
        .env("CRABBER_TOOL_SQL_CHILD", directory)
        .env("CRABBER_TOOL_SQL_STEP", step.to_string());
    if write {
        command.env("CRABBER_TOOL_SQL_WRITE", "1");
    } else {
        command.env_remove("CRABBER_TOOL_SQL_WRITE");
    }
    database.configure(&mut command, "CRABBER_TOOL_SQL_DATABASE");
    Guard(command.spawn().unwrap())
}
