//! Actual controller death while the native successor transaction is open/committed.
use super::*;
use crabber_core::{Clock, RunId, SessionId};
use std::{
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
#[derive(serde::Serialize, serde::Deserialize)]
struct Wire {
    session: SessionId,
    workspace: String,
    directory: String,
    run: RunId,
    token: String,
    claim: NativeClaimLocator,
    now: time::OffsetDateTime,
}
impl Wire {
    fn request(self) -> RegisterSuccessorExecutionDriver {
        RegisterSuccessorExecutionDriver {
            context: crabber_core::SessionContext::new(
                self.session,
                self.workspace,
                self.directory,
            ),
            fence: crabber_core::RunFence {
                run_id: self.run,
                claim_token: self.token,
            },
            claim: self.claim,
        }
    }
}
pub(crate) async fn child(store: &impl Store, clock: &ManualClock) {
    let admission = std::env::var("CRABBER_SUCCESSOR_FAMILY").unwrap() == "admission";
    let request = fixture::request(store, clock, admission).await;
    let wire = Wire {
        session: request.context.session_id().clone(),
        workspace: request.context.workspace_id().into(),
        directory: request.context.directory().into(),
        run: request.fence.run_id.clone(),
        token: request.fence.claim_token.clone(),
        claim: request.claim.clone(),
        now: clock.now(),
    };
    std::fs::write(
        std::env::var("CRABBER_SUCCESSOR_KEY").unwrap(),
        serde_json::to_vec(&wire).unwrap(),
    )
    .unwrap();
    store
        .register_successor_execution_driver(request)
        .await
        .unwrap();
    panic!("controller missed physical kill boundary");
}
pub(crate) fn kill(
    test: &str,
    stage: &str,
    path: Option<&Path>,
    directory: &Path,
    admission: bool,
) -> (RegisterSuccessorExecutionDriver, time::OffsetDateTime) {
    let marker = directory.join("marker");
    let key = directory.join("key");
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", test, "--nocapture"])
        .env("CRABBER_SUCCESSOR_CHILD", "1")
        .env("CRABBER_OWNER_KILL_POINT", format!("successor-{stage}"))
        .env("CRABBER_OWNER_MARKER", &marker)
        .env("CRABBER_SUCCESSOR_KEY", &key)
        .env(
            "CRABBER_SUCCESSOR_FAMILY",
            if admission { "admission" } else { "expired" },
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(path) = path {
        command.env("CRABBER_SUCCESSOR_DATABASE", path);
    }
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while !marker.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "child exited before boundary: {:?}",
            child.wait_with_output().unwrap()
        );
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("child missed {stage} boundary");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let wire: Wire = serde_json::from_slice(&std::fs::read(key).unwrap()).unwrap();
    let now = wire.now;
    (wire.request(), now)
}
pub(crate) async fn verify(
    store: &impl Store,
    request: RegisterSuccessorExecutionDriver,
    committed: bool,
) {
    let ids = request.identities().unwrap();
    let outcome = store
        .register_successor_execution_driver(request.clone())
        .await
        .unwrap();
    assert_eq!(
        matches!(outcome, SuccessorExecutionDriverOutcome::Replayed(_)),
        committed
    );
    let receipt = match outcome {
        SuccessorExecutionDriverOutcome::Applied { receipt, .. }
        | SuccessorExecutionDriverOutcome::Replayed(receipt) => receipt,
    };
    assert_eq!(receipt.worker.begin.owner.target.id, ids.owner);
    assert_eq!(
        store
            .get_session_owner(request.context.session_id(), &ids.owner)
            .await
            .unwrap()
            .state,
        crate::session_owner::SessionOwnerState::Active
    );
    assert!(
        matches!(store.register_successor_execution_driver(request).await.unwrap(),SuccessorExecutionDriverOutcome::Replayed(saved)if saved==receipt)
    );
}
