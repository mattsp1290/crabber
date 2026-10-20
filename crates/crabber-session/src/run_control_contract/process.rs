//! Physically killed controllers at the adapters' actual transaction boundaries.
use super::{accept, cancel_action, request, steer};
use crate::{
    RunControlOutcome, RunControlRequest, RunControlStatus, RunControlTarget, Store,
    WaitingCancellation,
};
use crabber_core::{RunFence, TurnId};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Operation {
    Accept(RunControlRequest),
    Observe {
        original: RunControlRequest,
        fence: RunFence,
        turn: Option<TurnId>,
    },
    Waiting {
        original: RunControlRequest,
        request: WaitingCancellation,
    },
    Verify {
        original: RunControlRequest,
        stage: String,
        committed: bool,
        cancel: bool,
        baseline_messages: usize,
    },
}
pub(crate) fn command() -> Option<Operation> {
    std::env::var("CRABBER_CONTROL_CHILD")
        .ok()
        .map(|data| serde_json::from_str(&data).unwrap())
}
/// Present only in native test builds. The parent kills this process while the
/// transaction is open, or after commit while its acknowledgement is withheld.
pub(crate) fn pause(point: &str) {
    if std::env::var("CRABBER_CONTROL_KILL_POINT").ok().as_deref() == Some(point) {
        std::fs::write(std::env::var("CRABBER_CONTROL_MARKER").unwrap(), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }
}
pub(crate) async fn execute(store: &dyn Store, command: Operation) {
    match command {
        Operation::Accept(request) => {
            store.control_run(request).await.unwrap();
        }
        Operation::Observe { fence, turn, .. } => {
            store
                .execution(fence)
                .await
                .unwrap()
                .observe_run_controls(turn.as_ref())
                .await
                .unwrap();
        }
        Operation::Waiting { request, .. } => {
            store.settle_waiting_cancellation(request).await.unwrap();
        }
        Operation::Verify {
            original,
            stage,
            committed,
            cancel,
            baseline_messages,
        } => {
            verify(
                store,
                original,
                &stage,
                committed,
                cancel,
                baseline_messages,
            )
            .await;
        }
    }
}
async fn verify(
    store: &dyn Store,
    original: RunControlRequest,
    stage: &str,
    committed: bool,
    cancel: bool,
    baseline_messages: usize,
) {
    let target = &original.target;
    let receipt = store
        .lookup_run_control(&target.session_id, &original.id)
        .await
        .unwrap();
    if matches!(stage, "accept" | "toolaccept") && !committed {
        assert!(receipt.is_none());
        assert_eq!(
            store
                .list_all_messages(&target.session_id)
                .await
                .unwrap()
                .len(),
            baseline_messages
        );
        assert_eq!(
            store.session_revision(&target.session_id).await.unwrap(),
            original.expected_revision
        );
        return;
    }
    let receipt = receipt.unwrap();
    let revision = store.session_revision(&target.session_id).await.unwrap();
    assert_eq!(
        store.control_run(original.clone()).await.unwrap(),
        RunControlOutcome::Replayed(receipt)
    );
    assert_eq!(
        store.session_revision(&target.session_id).await.unwrap(),
        revision
    );
    let page = store.run_controls(target).await.unwrap();
    let status = &page.controls[0].status;
    match (stage, committed, cancel) {
        ("boundary", true, true) => assert_eq!(status, &RunControlStatus::Observed),
        ("boundary", true, false) => {
            assert!(matches!(status, RunControlStatus::Incorporated { .. }));
        }
        ("waiting" | "toolaccept", true, _) => assert_eq!(
            status,
            &RunControlStatus::TerminalCommitted {
                status: crabber_core::RunStatus::Interrupted
            }
        ),
        _ => assert_eq!(status, &RunControlStatus::Accepted),
    }
    let messages = store.list_all_messages(&target.session_id).await.unwrap();
    let added = committed
        && ((stage == "boundary" && !cancel) || matches!(stage, "waiting" | "toolaccept"));
    assert_eq!(messages.len(), baseline_messages + usize::from(added));
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
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", helper, "--nocapture"])
        .env(
            "CRABBER_CONTROL_CHILD",
            serde_json::to_string(operation).unwrap(),
        )
        .stdout(Stdio::null());
    if let Some(path) = path {
        child.env("CRABBER_SQLITE_CHILD_PATH", path);
    }
    if let Some(point) = point {
        child
            .env("CRABBER_CONTROL_KILL_POINT", point)
            .env("CRABBER_CONTROL_MARKER", marker);
    } else {
        child.env_remove("CRABBER_CONTROL_KILL_POINT");
    }
    Guard(child.spawn().unwrap())
}
async fn marker(path: &Path) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("control child reached commit boundary");
}
async fn joined(child: &mut Guard) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success(), "control child: {status}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
pub(crate) async fn run(store: &impl Store, helper: &str, path: Option<&Path>) {
    for stage in ["accept", "boundary", "waiting", "toolaccept"] {
        for cancel in [false, true] {
            if matches!(stage, "waiting" | "toolaccept") && !cancel {
                continue;
            }
            for committed in [false, true] {
                let (operation, original) = setup(store, stage, cancel).await;
                let baseline_messages = store
                    .list_all_messages(&original.target.session_id)
                    .await
                    .unwrap()
                    .len();
                let dir = tempfile::tempdir().unwrap();
                let ready = dir.path().join("ready");
                let seam = if stage == "toolaccept" {
                    "accept"
                } else {
                    stage
                };
                let point = format!("{seam}_{}", if committed { "after" } else { "before" });
                let mut child = spawn(helper, &operation, path, Some(&point), &ready);
                marker(&ready).await;
                child.0.kill().unwrap();
                child.0.wait().unwrap();
                let mut cold = spawn(
                    helper,
                    &Operation::Verify {
                        original: original.clone(),
                        stage: stage.into(),
                        committed,
                        cancel,
                        baseline_messages,
                    },
                    path,
                    None,
                    &ready,
                );
                joined(&mut cold).await;
                if matches!(stage, "accept" | "toolaccept") && !committed {
                    accept(store, original).await;
                }
            }
        }
    }
}
async fn setup(store: &impl Store, stage: &str, cancel: bool) -> (Operation, RunControlRequest) {
    if stage == "toolaccept" {
        let f = crate::tool_continuation_contract::fixture::fixture(store).await;
        f.execution
            .suspend_tool_continuation(f.suspension, f.authority)
            .await
            .unwrap();
        let target = RunControlTarget::of(&f.admitted.run);
        let original = request(store, &target, cancel_action()).await;
        return (Operation::Accept(original.clone()), original);
    }
    if stage == "waiting" {
        let clock = crabber_core::ManualClock::new(time::OffsetDateTime::UNIX_EPOCH);
        let (target, original) = super::checkpoint::paused(store, &clock, true).await;
        let original = original.unwrap();
        let request = WaitingCancellation {
            target: target.clone(),
            original_cancel_id: original.id.clone(),
            expected_revision: store.session_revision(&target.session_id).await.unwrap(),
        };
        return (
            Operation::Waiting {
                original: original.clone(),
                request,
            },
            original,
        );
    }
    let admitted =
        crate::public_state_contract::admit(store, time::OffsetDateTime::UNIX_EPOCH).await;
    let target = RunControlTarget::of(&admitted.run);
    let original = if cancel {
        request(store, &target, cancel_action()).await
    } else {
        steer(store, &target, "physical kill retained input").await
    };
    if stage == "accept" {
        return (Operation::Accept(original.clone()), original);
    }
    accept(store, original.clone()).await;
    (
        Operation::Observe {
            original: original.clone(),
            fence: admitted.fence,
            turn: (!cancel).then(TurnId::new),
        },
        original,
    )
}
