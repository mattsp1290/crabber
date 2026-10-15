//! Test-only fresh-process attempt reconciliation, never a production fault API.
use super::{applied, reserve};
use crate::{
    AttemptCommand, AttemptError as Error, AttemptMutation, AttemptMutationOutcome as Outcome,
    AttemptState, Store,
};
use crabber_core::{ControlId, RunFence, SessionId};
use serde::{Deserialize, Serialize};
use std::{path::Path, process::Command, time::Duration};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) enum Mode {
    Reserve,
    Begin,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Fixture {
    pub fence: RunFence,
    pub session: SessionId,
    pub request: AttemptMutation,
    pub mode: Mode,
}
pub(crate) async fn pending(store: &impl Store, mode: Mode) -> Fixture {
    let admitted =
        crate::public_state_contract::admit(store, time::OffsetDateTime::now_utc()).await;
    let mut request = reserve(store.session_revision(&admitted.session.id).await.unwrap());
    if matches!(mode, Mode::Begin) {
        request.command = AttemptCommand::Begin {
            target: request.command.target().clone(),
        };
        request.expected_revision = request.expected_revision.next().unwrap();
    }
    Fixture {
        fence: admitted.fence,
        session: admitted.session.id,
        request,
        mode,
    }
}
pub(crate) async fn write(store: &impl Store, fixture: &Fixture) -> Result<Outcome, Error> {
    let execution = store.execution(fixture.fence.clone()).await.unwrap();
    let authority = if matches!(fixture.mode, Mode::Begin) {
        let reserve = prior_reserve(fixture);
        applied(execution.mutate_attempt(reserve, None).await.unwrap()).1
    } else {
        None
    };
    execution
        .mutate_attempt(fixture.request.clone(), authority)
        .await
}
fn prior_reserve(fixture: &Fixture) -> AttemptMutation {
    AttemptMutation {
        id: ControlId::new(),
        expected_revision: crabber_core::SessionRevision::new(
            fixture.request.expected_revision.get() - 1,
        )
        .unwrap(),
        command: AttemptCommand::Reserve {
            target: fixture.request.command.target().clone(),
        },
    }
}
pub(crate) async fn prepare_loss(store: &impl Store, mode: Mode, inject: impl Fn(u8)) -> Fixture {
    let fixture = pending(store, mode).await;
    let execution = store.execution(fixture.fence.clone()).await.unwrap();
    let authority = if matches!(mode, Mode::Begin) {
        applied(
            execution
                .mutate_attempt(prior_reserve(&fixture), None)
                .await
                .unwrap(),
        )
        .1
    } else {
        None
    };
    inject(2);
    assert_eq!(
        execution
            .mutate_attempt(fixture.request.clone(), authority)
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    inject(0);
    fixture
}
pub(crate) async fn verify_loss(store: &impl Store, fixture: Fixture) {
    let receipt = store
        .lookup_attempt_mutation(&fixture.session, &fixture.request.id)
        .await
        .unwrap()
        .unwrap();
    let before = store
        .execution_attempt(&fixture.fence.run_id, &receipt.attempt.target.id)
        .await
        .unwrap();
    let count = store
        .list_events(&fixture.session, None, 100)
        .await
        .unwrap()
        .len();
    assert_eq!(
        receipt.semantic_digest,
        fixture
            .request
            .fingerprint(&fixture.session, &fixture.fence.run_id)
            .unwrap()
    );
    assert_eq!(before.attempt, receipt.attempt);
    assert!(store.execution(fixture.fence.clone()).await.is_err());
    let replacement = store
        .claim_expired_run(&fixture.fence.run_id, "fresh-process")
        .await
        .unwrap();
    let execution = store.execution(replacement).await.unwrap();
    assert!(
        matches!(execution.mutate_attempt(fixture.request.clone(),None).await.unwrap(), Outcome::Replayed(r) if r == receipt)
    );
    assert_eq!(
        store
            .execution_attempt(&fixture.fence.run_id, &receipt.attempt.target.id)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        store
            .list_events(&fixture.session, None, 100)
            .await
            .unwrap()
            .len(),
        count
    );
    // Parent uses an epoch ManualClock. A fresh process uses wall time, so the
    // old run lease has expired; claiming it must not replay Started work.
    let replacement = execution;
    let reclaim = AttemptMutation {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        command: AttemptCommand::ReclaimUnstarted {
            target: receipt.attempt.target.clone(),
        },
    };
    if matches!(fixture.mode, Mode::Reserve) {
        let (claimed, authority) =
            applied(replacement.mutate_attempt(reclaim, None).await.unwrap());
        let begin = AttemptMutation {
            id: ControlId::new(),
            expected_revision: claimed.revision,
            command: AttemptCommand::Begin {
                target: claimed.attempt.target.clone(),
            },
        };
        let (started, _) = applied(replacement.mutate_attempt(begin, authority).await.unwrap());
        assert_eq!(started.attempt.state, AttemptState::Started);
    } else {
        assert_eq!(
            replacement.mutate_attempt(reclaim, None).await.unwrap_err(),
            Error::InvalidTransition
        );
        assert!(
            matches!(replacement.mutate_attempt(fixture.request,None).await.unwrap(),Outcome::Replayed(r) if r == receipt)
        );
        assert_eq!(
            store
                .list_events(&fixture.session, None, 100)
                .await
                .unwrap()
                .len(),
            count
        );
    }
}
pub(crate) fn fixture() -> Option<Fixture> {
    std::env::var("CRABBER_ATTEMPT_CHILD")
        .ok()
        .map(|v| serde_json::from_str(&v).unwrap())
}
pub(crate) struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Child {
    pub(crate) async fn wait(&mut self) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success(), "child failed: {status}");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fresh process timed out");
    }
    pub(crate) fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
    pub(crate) fn running(&mut self) -> bool {
        self.0.try_wait().unwrap().is_none()
    }
}
pub(crate) fn spawn(
    helper: &str,
    fixture: &Fixture,
    path: Option<&Path>,
    extra: &[(&str, String)],
) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", helper, "--nocapture"]).env(
        "CRABBER_ATTEMPT_CHILD",
        serde_json::to_string(fixture).unwrap(),
    );
    if let Some(path) = path {
        command.env("CRABBER_SQLITE_CHILD_PATH", path);
    }
    for (key, value) in extra {
        command.env(key, value);
    }
    Child(command.spawn().unwrap())
}

pub(crate) async fn uncommitted(store: &impl Store, fixture: &Fixture) {
    assert_eq!(
        store
            .lookup_attempt_mutation(&fixture.session, &fixture.request.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store.session_revision(&fixture.session).await.unwrap(),
        fixture.request.expected_revision
    );
    let snapshot = store
        .execution_attempt(&fixture.fence.run_id, &fixture.request.command.target().id)
        .await;
    let count = store
        .list_events(&fixture.session, None, 100)
        .await
        .unwrap()
        .len();
    if matches!(fixture.mode, Mode::Reserve) {
        assert_eq!(snapshot.unwrap_err(), Error::NotFound);
        assert_eq!(count, 0);
    } else {
        assert_eq!(snapshot.unwrap().attempt.state, AttemptState::Unstarted);
        assert_eq!(count, 1);
    }
}
pub(crate) async fn retry_crash(
    store: &impl Store,
    clock: &crabber_core::ManualClock,
    fixture: &Fixture,
) {
    if matches!(fixture.mode, Mode::Reserve) {
        let execution = store.execution(fixture.fence.clone()).await.unwrap();
        applied(
            execution
                .mutate_attempt(fixture.request.clone(), None)
                .await
                .unwrap(),
        );
        assert_eq!(
            store
                .list_events(&fixture.session, None, 100)
                .await
                .unwrap()
                .len(),
            1
        );
    } else {
        clock.set(
            store
                .get_run(&fixture.fence.run_id)
                .await
                .unwrap()
                .unwrap()
                .lease_until,
        );
        let fence = store
            .claim_expired_run(&fixture.fence.run_id, "crash-recovery")
            .await
            .unwrap();
        let execution = store.execution(fence).await.unwrap();
        let reclaim = AttemptMutation {
            id: ControlId::new(),
            expected_revision: fixture.request.expected_revision,
            command: AttemptCommand::ReclaimUnstarted {
                target: fixture.request.command.target().clone(),
            },
        };
        let (claimed, authority) = applied(execution.mutate_attempt(reclaim, None).await.unwrap());
        let begin = AttemptMutation {
            id: ControlId::new(),
            expected_revision: claimed.revision,
            command: AttemptCommand::Begin {
                target: claimed.attempt.target.clone(),
            },
        };
        applied(execution.mutate_attempt(begin, authority).await.unwrap());
        assert_eq!(
            store
                .lookup_attempt_mutation(&fixture.session, &fixture.request.id)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .list_events(&fixture.session, None, 100)
                .await
                .unwrap()
                .len(),
            3
        );
    }
}
