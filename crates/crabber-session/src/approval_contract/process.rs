//! Physical crash fixtures never serialize native attempt authority.
#[path = "process/child.rs"]
pub(crate) mod child;
#[path = "process/verify.rs"]
pub(crate) mod verify;
use super::fixture;
use crate::{
    ApprovalAnswer, ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalError,
    ApprovalReceipt, ApprovalReply, ApprovalSnapshot, ApprovalSuspension, AttemptAuthority,
    AttemptCommand, AttemptMutation, AttemptMutationOutcome, Store,
};
use crabber_core::{ControlId, ManualClock, RunFence, RunId, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) enum Mode {
    Suspend,
    Answer,
    Claim,
    Recover,
    Cancel,
    Expire,
}
impl Mode {
    pub(crate) const ALL: [Self; 6] = [
        Self::Suspend,
        Self::Answer,
        Self::Claim,
        Self::Recover,
        Self::Cancel,
        Self::Expire,
    ];
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Fixture {
    pub mode: Mode,
    pub session: SessionId,
    pub run: RunId,
    pub fence: RunFence,
    pub batch: ApprovalSuspension,
    pub answer: Option<ApprovalAnswer>,
    pub claim: Option<ApprovalContinuationClaim>,
    pub recovery: Option<crate::ApprovalContinuationRecovery>,
    pub termination: Option<crate::ApprovalTermination>,
    pub clock_at: time::OffsetDateTime,
    pub revision: SessionRevision,
    pub events: usize,
    pub messages: usize,
    pub before: Option<ApprovalSnapshot>,
}
impl Fixture {
    pub(crate) fn key(&self) -> &ControlId {
        match self.mode {
            Mode::Suspend => &self.batch.id,
            Mode::Answer => &self.answer.as_ref().unwrap().id,
            Mode::Claim => &self.claim.as_ref().unwrap().id,
            Mode::Recover => &self.recovery.as_ref().unwrap().id,
            Mode::Cancel | Mode::Expire => &self.termination.as_ref().unwrap().id,
        }
    }
}
pub(crate) async fn pending(store: &impl Store, mode: Mode, clock: &ManualClock) -> Fixture {
    clock.set(time::OffsetDateTime::UNIX_EPOCH);
    if matches!(mode, Mode::Recover) {
        return recovered(store, clock).await;
    }
    let (run, fence, batch, answer, claim) = if matches!(mode, Mode::Suspend) {
        let (run, fence, batch) = fixture::unstarted_batch(store, clock).await;
        (run, fence, batch, None, None)
    } else {
        let f = fixture::staged(store, clock).await;
        f.execution
            .suspend_for_approval(f.batch.clone(), f.owner.clone())
            .await
            .unwrap();
        let fence = RunFence {
            run_id: f.run.id.clone(),
            claim_token: f.run.claim_token.clone(),
        };
        let (answer, claim) = if matches!(mode, Mode::Answer) {
            (
                Some(fixture::answer(store, &f, 0, ApprovalReply::Grant).await),
                None,
            )
        } else if matches!(mode, Mode::Cancel | Mode::Expire) {
            (None, None)
        } else {
            for index in 0..2 {
                let answer = fixture::answer(store, &f, index, ApprovalReply::Grant).await;
                store.answer_approval(answer).await.unwrap();
            }
            (None, Some(fixture::claim(store, &f).await))
        };
        (f.run, fence, f.batch, answer, claim)
    };
    let termination = if matches!(mode, Mode::Cancel | Mode::Expire) {
        if matches!(mode, Mode::Expire) {
            clock.set(time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2));
        }
        let crate::CallApproval::Ask { id, description } = &batch.calls[0].decision else {
            panic!("question")
        };
        Some(crate::ApprovalTermination {
            id: ControlId::new(),
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            continuation: batch.continuation.clone(),
            expected_revision: store.session_revision(&run.session_id).await.unwrap(),
            resolver: crate::ApprovalResolverClaim {
                resolver_id: "admin".into(),
                authority: description.authority().clone(),
                authority_claim: "host-verified-admin".into(),
            },
            kind: if matches!(mode, Mode::Expire) {
                crate::ApprovalTerminationKind::Expire {
                    interaction_id: id.clone(),
                }
            } else {
                crate::ApprovalTerminationKind::Cancel
            },
        })
    } else {
        None
    };
    Fixture {
        revision: store.session_revision(&run.session_id).await.unwrap(),
        events: store
            .list_events(&run.session_id, None, 1000)
            .await
            .unwrap()
            .len(),
        messages: store
            .list_all_messages(&run.session_id)
            .await
            .unwrap()
            .len(),
        before: store
            .approval_snapshot(&run.session_id, &run.id)
            .await
            .unwrap(),
        session: run.session_id,
        run: run.id,
        fence,
        batch,
        answer,
        claim,
        recovery: None,
        termination,
        clock_at: crabber_core::Clock::now(clock),
        mode,
    }
}
pub(crate) async fn write(
    store: &impl Store,
    f: &Fixture,
    inject: impl Fn(u8),
    lost_ack: bool,
) -> Result<ApprovalReceipt, ApprovalError> {
    inject(0);
    let owner = if matches!(f.mode, Mode::Suspend) {
        Some(begin(store, f).await)
    } else {
        None
    };
    inject(if lost_ack { 2 } else { 0 });
    let result = match f.mode {
        Mode::Suspend => {
            store
                .execution(f.fence.clone())
                .await
                .unwrap()
                .suspend_for_approval(f.batch.clone(), owner.unwrap())
                .await
        }
        Mode::Cancel | Mode::Expire => {
            store
                .terminate_approval(f.termination.clone().unwrap())
                .await
        }
        Mode::Answer => store.answer_approval(f.answer.clone().unwrap()).await,
        Mode::Recover => match store
            .reclaim_approval_continuation(f.recovery.clone().unwrap())
            .await
        {
            Ok(
                ApprovalClaimOutcome::Applied { receipt, .. }
                | ApprovalClaimOutcome::Replayed(receipt),
            ) => Ok(receipt),
            Err(error) => Err(error),
        },
        Mode::Claim => match store
            .claim_approval_continuation(f.claim.clone().unwrap())
            .await
        {
            Ok(
                ApprovalClaimOutcome::Applied { receipt, .. }
                | ApprovalClaimOutcome::Replayed(receipt),
            ) => Ok(receipt),
            Err(error) => Err(error),
        },
    };
    inject(0);
    result
}
async fn begin(store: &impl Store, f: &Fixture) -> AttemptAuthority {
    let execution = store.execution(f.fence.clone()).await.unwrap();
    let reserve = AttemptMutation {
        id: ControlId::new(),
        expected_revision: f.revision,
        command: AttemptCommand::Reserve {
            target: f.batch.attempt.clone(),
        },
    };
    let AttemptMutationOutcome::Applied {
        receipt,
        authority: Some(owner),
    } = execution.mutate_attempt(reserve, None).await.unwrap()
    else {
        panic!("fresh native reserve");
    };
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: receipt.revision,
                command: AttemptCommand::Begin {
                    target: f.batch.attempt.clone(),
                },
            },
            Some(owner.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        store.session_revision(&f.session).await.unwrap(),
        f.batch.expected_revision
    );
    owner
}
pub(crate) fn clock() -> std::sync::Arc<ManualClock> {
    std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH))
}

pub(crate) fn fixture_clock(fixture: &Fixture) -> std::sync::Arc<ManualClock> {
    std::sync::Arc::new(ManualClock::new(fixture.clock_at))
}

async fn recovered(store: &impl Store, clock: &ManualClock) -> Fixture {
    let mode = Mode::Recover;
    let p = super::recovery::prepare(store, clock).await;
    clock.set(time::OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(31));
    Fixture {
        mode,
        session: p.original.session_id.clone(),
        run: p.original.run_id.clone(),
        fence: p.grant.fence,
        batch: p.fixture.batch,
        answer: None,
        claim: Some(p.original),
        recovery: Some(p.request),
        termination: None,
        clock_at: time::OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(31),
        revision: p.receipt.revision,
        events: store
            .list_events(&p.fixture.run.session_id, None, 1000)
            .await
            .unwrap()
            .len(),
        messages: store
            .list_all_messages(&p.fixture.run.session_id)
            .await
            .unwrap()
            .len(),
        before: store
            .approval_snapshot(&p.fixture.run.session_id, &p.fixture.run.id)
            .await
            .unwrap(),
    }
}
