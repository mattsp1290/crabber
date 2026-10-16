//! A valid logical call can produce a larger canonical settlement event.
use super::{applied, fixture_for_admission_named, mutation};
use crate::{PhaseCommand, PhaseSettlement, PhaseWriteAuthority, Store};
use crabber_core::{EventKind, MessageId, PartId, ToolResultStatus};

pub(crate) async fn run(store: &impl Store) {
    let admitted =
        crate::public_state_contract::admit(store, time::OffsetDateTime::UNIX_EPOCH).await;
    let (admitted, execution, owner, claim) =
        fixture_for_admission_named(store, admitted, &"n".repeat(2_200_000)).await;
    let parent = claim.command.target().attempt.clone();
    let (claimed, worker) = applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let worker = worker.unwrap();
    let (begun, _) = applied(
        execution
            .mutate_phase(
                mutation(
                    claimed.revision,
                    PhaseCommand::Begin {
                        target: claimed.phase.target,
                    },
                ),
                Some(PhaseWriteAuthority::Phase(worker.clone())),
            )
            .await
            .unwrap(),
    );
    let settlement = PhaseSettlement {
        completion: mutation(
            begun.revision,
            PhaseCommand::Complete {
                target: begun.phase.target,
                output: super::output(),
            },
        ),
        result_status: ToolResultStatus::Completed,
        message_id: MessageId::new(),
        part_id: PartId::new(),
        turn_id: None,
        created_at: admitted.run.created_at,
        duration_ms: 0,
    };
    let (completed, _) = applied(
        execution
            .settle_tool_phase(settlement.clone(), Some(worker))
            .await
            .unwrap(),
    );
    let calls = super::settlement::read_calls(store, &admitted.session.id)
        .await
        .unwrap();
    assert_eq!(calls.len(), 1);
    assert!(serde_json::to_vec(&calls[0]).unwrap().len() < 4_194_304);
    // Read the actual canonical event, independently of mutable call/phase rows.
    let events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap();
    let settled = events
        .iter()
        .find(|event| event.kind == EventKind::ToolCallSettled)
        .unwrap();
    assert!(serde_json::to_vec(settled).unwrap().len() > 4_198_400);
    let uncertainty = crate::attempt_control_contract::uncertainty(
        &admitted,
        parent,
        store.session_revision(&admitted.session.id).await.unwrap(),
    );
    let marked = crate::attempt_control_contract::fresh(
        store
            .mark_attempt_uncertain(uncertainty.clone())
            .await
            .unwrap(),
    );
    let retire = crate::attempt_control_contract::retirement(&uncertainty, marked.revision);
    let retired =
        crate::attempt_control_contract::fresh(store.retire_attempt(retire.clone()).await.unwrap());
    assert_eq!(
        store.retire_attempt(retire).await.unwrap(),
        crate::AttemptControlOutcome::Replayed(retired)
    );
    assert_eq!(
        super::settlement::read_calls(store, &admitted.session.id)
            .await
            .unwrap(),
        calls
    );
    assert_eq!(
        store
            .tool_phase(&admitted.run.id, &completed.phase.target.id)
            .await
            .unwrap()
            .phase,
        completed.phase
    );
    assert!(
        matches!(execution.settle_tool_phase(settlement, None).await.unwrap(),
        crate::PhaseMutationOutcome::Replayed(receipt) if receipt == completed)
    );
}
