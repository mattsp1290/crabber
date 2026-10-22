//! Coherent private changes cannot replace immutable accepted answers or release truth.
use super::fixture::{answer, claim, staged};
use crate::{
    ApprovalContinuationState, ApprovalError, ApprovalReply, AttemptCleanupResult, AttemptCommand,
    AttemptFinish, AttemptMutation, Store,
};
use crabber_core::{ContinuationId, ControlId, InteractionState, ManualClock};
use std::future::Future;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Case {
    MissingAnswer,
    ChangedAnswer,
    WaitingRollback,
    ReleasedNoBarrier,
    ChangedClaimReceipt,
    FakeTerminalCleanup,
}
pub(crate) const CASES: [Case; 6] = [
    Case::MissingAnswer,
    Case::ChangedAnswer,
    Case::WaitingRollback,
    Case::ReleasedNoBarrier,
    Case::ChangedClaimReceipt,
    Case::FakeTerminalCleanup,
];
pub(crate) struct Target {
    pub id: ContinuationId,
    pub claim_id: Option<ControlId>,
    pub case: Case,
}
pub(crate) fn corrupt(record: &mut crate::approval::ApprovalRecord, case: Case) {
    match case {
        Case::MissingAnswer => {
            record.questions[0].answer = None;
            record.questions[0].view.state = InteractionState::Pending;
        }
        Case::ChangedAnswer => record.questions[0]
            .answer
            .as_mut()
            .unwrap()
            .resolver
            .authority_claim
            .push_str(" changed"),
        Case::WaitingRollback => {
            record.state = ApprovalContinuationState::Waiting;
            for q in &mut record.questions {
                q.view.consumed = false;
            }
        }
        Case::ReleasedNoBarrier | Case::FakeTerminalCleanup => {
            record.state = ApprovalContinuationState::Released;
        }
        Case::ChangedClaimReceipt => (),
    }
    record.validate(&record.session_id, &record.run_id).unwrap();
}
pub(crate) async fn run<F, Fut, R>(store: &dyn Store, clock: &ManualClock, case: Case, save: F) -> R
where
    F: FnOnce(Target) -> Fut,
    Fut: Future<Output = R>,
{
    if matches!(case, Case::MissingAnswer | Case::ChangedAnswer) {
        let f = staged(store, clock).await;
        let suspended = f
            .execution
            .suspend_for_approval(f.batch.clone(), f.owner.clone())
            .await
            .unwrap();
        let accepted = answer(store, &f, 0, ApprovalReply::Grant).await;
        let receipt = store.answer_approval(accepted.clone()).await.unwrap();
        let restore = save(Target {
            id: f.batch.continuation.id.clone(),
            claim_id: None,
            case,
        })
        .await;
        rejected(store, &f).await;
        clock.set(crabber_core::Clock::now(clock) + time::Duration::seconds(2));
        assert_eq!(
            store
                .terminate_approval(super::termination::request(store, &f, true).await)
                .await
                .unwrap_err(),
            ApprovalError::UnknownStoreOutcome
        );

        assert_eq!(store.answer_approval(accepted).await.unwrap(), receipt);
        assert_eq!(
            f.execution
                .suspend_for_approval(f.batch.clone(), f.owner.clone())
                .await
                .unwrap(),
            suspended
        );
        return restore;
    }
    let p = super::recovery::prepare(store, clock).await;
    if case == Case::FakeTerminalCleanup {
        generic_cleanup(store, &p).await;
    }
    let restore = save(Target {
        id: p.original.continuation.id.clone(),
        claim_id: Some(p.original.id.clone()),
        case,
    })
    .await;
    rejected(store, &p.fixture).await;
    let execution = store.execution(p.grant.fence.clone()).await;
    if let Ok(execution) = execution {
        assert!(
            execution
                .mutate_attempt(
                    AttemptMutation {
                        id: ControlId::new(),
                        expected_revision: store
                            .session_revision(&p.original.session_id)
                            .await
                            .unwrap(),
                        command: AttemptCommand::Begin {
                            target: p.request.attempt.clone()
                        }
                    },
                    Some(p.grant.authority)
                )
                .await
                .is_err()
        );
    }
    if case == Case::ChangedClaimReceipt {
        assert!(store.claim_approval_continuation(p.original).await.is_err());
    } else {
        assert!(
            matches!(store.claim_approval_continuation(p.original).await.unwrap(),crate::ApprovalClaimOutcome::Replayed(saved) if saved==p.receipt)
        );
    }
    restore
}
async fn rejected(store: &dyn Store, f: &super::fixture::Fixture) {
    let run = store.get_run(&f.run.id).await.unwrap();
    let revision = store.session_revision(&f.run.session_id).await.unwrap();
    let events = store
        .list_events(&f.run.session_id, None, 1000)
        .await
        .unwrap();
    let messages = store.list_all_messages(&f.run.session_id).await.unwrap();
    assert_eq!(
        store
            .approval_snapshot(&f.run.session_id, &f.run.id)
            .await
            .unwrap_err(),
        ApprovalError::UnknownStoreOutcome
    );
    let request = crate::PublicSessionRequest {
        session_id: f.run.session_id.clone(),
        continuation: None,
        limits: crate::PublicSessionLimits {
            header_bytes: 65536,
            proof_bytes: 2 * 1024 * 1024,
            interactions: 64,
            history: crate::SnapshotLimits {
                messages: 64,
                tool_calls: 64,
                parts: 1024,
                text_bytes: 1024 * 1024,
                encoded_bytes: 2 * 1024 * 1024,
            },
        },
    };
    assert_eq!(
        store.public_session_snapshot(request).await.unwrap_err(),
        crate::PublicSessionError::UnknownStoreOutcome
    );
    assert!(
        store
            .answer_approval(answer(store, f, 0, ApprovalReply::Deny).await)
            .await
            .is_err()
    );
    assert!(
        store
            .claim_approval_continuation(claim(store, f).await)
            .await
            .is_err()
    );
    assert!(
        store
            .terminate_approval(super::termination::request(store, f, false).await)
            .await
            .is_err()
    );
    assert!(
        store
            .terminate_approval(super::termination::request(store, f, true).await)
            .await
            .is_err()
    );
    let summary = store.session_summary(&f.run.session_id).await.unwrap();
    assert!(
        store
            .admit_run(crate::management_contract::admission(&summary))
            .await
            .is_err()
    );
    assert_eq!(store.get_run(&f.run.id).await.unwrap(), run);
    assert_eq!(
        store.session_revision(&f.run.session_id).await.unwrap(),
        revision
    );
    assert_eq!(
        store
            .list_events(&f.run.session_id, None, 1000)
            .await
            .unwrap(),
        events
    );
    assert_eq!(
        store.list_all_messages(&f.run.session_id).await.unwrap(),
        messages
    );
}
async fn generic_cleanup(store: &dyn Store, p: &super::recovery::Prepared) {
    let execution = store.execution(p.grant.fence.clone()).await.unwrap();
    let target = p.request.attempt.clone();
    for command in [
        AttemptCommand::Begin {
            target: target.clone(),
        },
        AttemptCommand::Finish {
            target: target.clone(),
            outcome: AttemptFinish::Interrupted,
        },
        AttemptCommand::RecordCleanup {
            target,
            result: AttemptCleanupResult::Complete,
        },
    ] {
        execution
            .mutate_attempt(
                AttemptMutation {
                    id: ControlId::new(),
                    expected_revision: store
                        .session_revision(&p.original.session_id)
                        .await
                        .unwrap(),
                    command,
                },
                Some(p.grant.authority.clone()),
            )
            .await
            .unwrap();
    }
}

pub(crate) async fn retired(store: &dyn Store, clock: &ManualClock) {
    let p = super::recovery::prepare(store, clock).await;
    let execution = store.execution(p.grant.fence.clone()).await.unwrap();
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: p.receipt.revision,
                command: AttemptCommand::Begin {
                    target: p.request.attempt.clone(),
                },
            },
            Some(p.grant.authority),
        )
        .await
        .unwrap();
    store
        .mark_attempt_uncertain(crate::AttemptUncertainty {
            session_id: p.original.session_id.clone(),
            run_id: p.original.run_id.clone(),
            id: ControlId::new(),
            expected_revision: store
                .session_revision(&p.original.session_id)
                .await
                .unwrap(),
            target: p.request.attempt.clone(),
            cause: crate::AttemptUncertaintyCause::HostStopped {
                claim: crate::attempt_control_contract::claim(),
            },
        })
        .await
        .unwrap();
    store
        .retire_attempt(crate::AttemptRetirement {
            session_id: p.original.session_id.clone(),
            run_id: p.original.run_id.clone(),
            id: ControlId::new(),
            expected_revision: store
                .session_revision(&p.original.session_id)
                .await
                .unwrap(),
            target: p.request.attempt,
            outcome: crate::AttemptRetirementOutcome::Interrupted,
            joined: crate::attempt_control_contract::claim(),
            pending: crate::PendingCallDisposition::CancelNeverStarted,
        })
        .await
        .unwrap();
    let approval = store
        .approval_snapshot(&p.original.session_id, &p.original.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(approval.state, ApprovalContinuationState::Released);
    let request = crate::PublicSessionRequest {
        session_id: p.original.session_id.clone(),
        continuation: None,
        limits: crate::PublicSessionLimits {
            header_bytes: 65536,
            proof_bytes: 2 * 1024 * 1024,
            interactions: 64,
            history: crate::SnapshotLimits {
                messages: 64,
                tool_calls: 64,
                parts: 1024,
                text_bytes: 1024 * 1024,
                encoded_bytes: 2 * 1024 * 1024,
            },
        },
    };
    assert!(
        matches!(store.public_session_snapshot(request).await.unwrap(),crate::PublicSessionOutcome::Page(page) if page.is_terminal())
    );
    store
        .admit_run(crate::management_contract::admission(
            &store.session_summary(&p.original.session_id).await.unwrap(),
        ))
        .await
        .unwrap();
}
