use super::{fresh, retirement, uncertainty};
use crate::{
    AttemptRetirement, PhaseCommand, PhaseSettlement, PhaseStart, PhaseWriteAuthority, Store,
};
use crabber_core::{MessageId, PartId, PhaseId, ToolCallId, ToolResultStatus};
pub(crate) async fn ready(store: &impl Store) -> (AttemptRetirement, ToolCallId, PhaseId) {
    let (admitted, execution, owner, planned) = crate::phase_contract::fixture(store).await;
    let (claimed, worker) = crate::phase_contract::applied(
        execution
            .mutate_phase(planned, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let target = claimed.phase.target.clone();
    let (started, _) = crate::phase_contract::applied(
        execution
            .start_tool_phase(
                PhaseStart {
                    begin: crate::phase_contract::mutation(
                        claimed.revision,
                        PhaseCommand::Begin {
                            target: target.clone(),
                        },
                    ),
                    turn_id: None,
                    created_at: admitted.run.created_at,
                },
                worker.clone(),
            )
            .await
            .unwrap(),
    );
    let (settled, _) = crate::phase_contract::applied(
        execution
            .settle_tool_phase(
                PhaseSettlement {
                    completion: crate::phase_contract::mutation(
                        started.revision,
                        PhaseCommand::Complete {
                            target: target.clone(),
                            output: crate::phase_contract::output(),
                        },
                    ),
                    result_status: ToolResultStatus::Completed,
                    message_id: MessageId::new(),
                    part_id: PartId::new(),
                    turn_id: None,
                    created_at: admitted.run.created_at,
                    duration_ms: 0,
                },
                worker,
            )
            .await
            .unwrap(),
    );
    let request = uncertainty(&admitted, target.attempt, settled.revision);
    let marked = fresh(store.mark_attempt_uncertain(request.clone()).await.unwrap());
    (
        retirement(&request, marked.revision),
        target.call_id,
        target.id,
    )
}
pub(crate) async fn refused(store: &impl Store, request: AttemptRetirement) {
    let before = super::process::evidence(
        store,
        &crate::attempt_control::Control::Retire(request.clone()),
    )
    .await;
    assert_eq!(
        store.retire_attempt(request.clone()).await.unwrap_err(),
        crate::AttemptError::UnknownStoreOutcome
    );
    assert_eq!(
        super::process::evidence(
            store,
            &crate::attempt_control::Control::Retire(request.clone())
        )
        .await,
        before
    );
    assert_eq!(
        store
            .lookup_attempt_mutation(&request.session_id, &request.id)
            .await
            .unwrap(),
        None
    );
}

pub(crate) async fn claimed(
    store: &impl Store,
    clock: &crabber_core::ManualClock,
) -> (AttemptRetirement, crabber_core::ContinuationId) {
    let p = crate::approval_contract::recovery::prepare(store, clock).await;
    let execution = store.execution(p.grant.fence.clone()).await.unwrap();
    execution
        .mutate_attempt(
            crate::AttemptMutation {
                id: crabber_core::ControlId::new(),
                expected_revision: p.receipt.revision,
                command: crate::AttemptCommand::Begin {
                    target: p.request.attempt.clone(),
                },
            },
            Some(p.grant.authority.clone()),
        )
        .await
        .unwrap();
    let request = crate::AttemptUncertainty {
        session_id: p.original.session_id.clone(),
        run_id: p.original.run_id.clone(),
        id: crabber_core::ControlId::new(),
        expected_revision: store
            .session_revision(&p.original.session_id)
            .await
            .unwrap(),
        target: p.request.attempt,
        cause: crate::AttemptUncertaintyCause::HostStopped {
            claim: super::claim(),
        },
    };
    let marked = fresh(store.mark_attempt_uncertain(request.clone()).await.unwrap());
    (
        retirement(&request, marked.revision),
        p.original.continuation.id,
    )
}
pub(crate) fn forge_approval(record: &mut crate::approval::ApprovalRecord, case: usize) {
    match case {
        0 => record.batch.calls[2].decision = crate::CallApproval::Deny,
        1 => {
            record.questions[0].answer.as_mut().unwrap().reply = crate::ApprovalReply::Deny;
            record.questions[0].view.state = crabber_core::InteractionState::Denied;
        }
        _ => crate::approval_contract::origin::corrupt(record, [0, 1, 2, 4][case - 2]),
    }
    // This regression deliberately passes local record validation; only original
    // suspension/answer provenance can reject the coherently forged evidence.
    record.validate(&record.session_id, &record.run_id).unwrap();
}
