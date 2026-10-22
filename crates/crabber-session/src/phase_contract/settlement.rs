//! Complete-phase + protected result history is one native transaction.
#[path = "settlement/interruption.rs"]
mod interruption;
use super::{applied, fixture, mutation};
use crate::{
    PhaseAuthority, PhaseCommand, PhaseError, PhaseMutationOutcome, PhaseQuiescence,
    PhaseSettlement, PhaseState, PhaseWriteAuthority, Store,
};
use crabber_core::{
    ContentBlock, EventKind, MediaKind, MediaSource, MessageContent, MessageId, PartId,
    ToolCallStatus, ToolOutput,
};

async fn started(
    store: &dyn Store,
    failed: bool,
) -> (
    crate::AdmitOutcome,
    Box<dyn crate::ExecutionStore>,
    PhaseAuthority,
    PhaseSettlement,
) {
    let (admitted, execution, owner, claim) = fixture(store).await;
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
    let output = ToolOutput::new(
        vec![
            MessageContent::Text {
                text: "protected caption".into(),
            },
            MessageContent::Media {
                kind: MediaKind::Image,
                mime_type: "image/png".into(),
                source: MediaSource::File {
                    file_id: "opaque-file".into(),
                },
            },
            MessageContent::Text {
                text: "trailing text".into(),
            },
        ],
        failed,
    )
    .unwrap();
    let settlement = PhaseSettlement {
        result_status: if failed {
            crabber_core::ToolResultStatus::Failed
        } else {
            crabber_core::ToolResultStatus::Completed
        },
        completion: mutation(
            begun.revision,
            PhaseCommand::Complete {
                target: begun.phase.target,
                output,
            },
        ),
        message_id: MessageId::new(),
        part_id: PartId::new(),
        turn_id: None,
        created_at: admitted.run.created_at,
        duration_ms: 5,
    };
    (admitted, execution, worker, settlement)
}
pub(super) async fn run(store: &dyn Store) {
    for failed in [false, true] {
        lifecycle(store, failed).await;
    }
    late_collision(store).await;
    operation_domains_are_distinct(store).await;
    uncertain_cannot_be_completed(store).await;
    interruption::run(store).await;
}
async fn lifecycle(store: &dyn Store, failed: bool) {
    let (admitted, execution, owner, settlement) = started(store, failed).await;
    let messages = store
        .list_messages(&admitted.session.id, None)
        .await
        .unwrap();
    let (receipt, authority) = applied(
        execution
            .settle_tool_phase(settlement.clone(), Some(owner))
            .await
            .unwrap(),
    );
    assert!(authority.is_none());
    assert_eq!(
        receipt.phase.state,
        if failed {
            PhaseState::Failed
        } else {
            PhaseState::Completed
        }
    );
    assert_eq!(receipt.phase.quiescence, PhaseQuiescence::Complete);
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        receipt.revision
    );
    let (after, events) =
        committed_records(store, &admitted, &settlement, failed, messages.len()).await;
    let mut retry = settlement.clone();
    retry.message_id = MessageId::new();
    retry.part_id = PartId::new();
    retry.duration_ms += 10;
    assert!(
        matches!(execution.settle_tool_phase(retry, None).await.unwrap(), PhaseMutationOutcome::Replayed(saved) if saved == receipt)
    );
    assert_eq!(
        store
            .list_messages(&admitted.session.id, None)
            .await
            .unwrap(),
        after
    );
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 1000)
            .await
            .unwrap(),
        events
    );
    let mut changed = settlement.clone();
    changed.result_status = crabber_core::ToolResultStatus::Completed;
    if let PhaseCommand::Complete { output, .. } = &mut changed.completion.command {
        *output = ToolOutput::from_json(&serde_json::json!("different choice")).unwrap();
    }
    assert_eq!(
        execution
            .settle_tool_phase(changed, None)
            .await
            .unwrap_err(),
        PhaseError::SemanticConflict
    );
    assert_eq!(
        execution
            .mutate_phase(settlement.completion, None)
            .await
            .unwrap_err(),
        PhaseError::SemanticConflict
    );
}
async fn committed_records(
    store: &dyn Store,
    admitted: &crate::AdmitOutcome,
    settlement: &PhaseSettlement,
    failed: bool,
    initial_messages: usize,
) -> (Vec<crabber_core::Message>, Vec<crabber_core::EventRecord>) {
    let calls = read_calls(store, &admitted.session.id).await.unwrap();
    let call = calls
        .iter()
        .find(|call| call.id == settlement.completion.command.target().call_id)
        .unwrap();
    assert_eq!(
        call.status,
        if failed {
            ToolCallStatus::Failed
        } else {
            ToolCallStatus::Completed
        }
    );
    let content = &call.result.as_ref().unwrap().content;
    assert_eq!(content.len(), 3);
    assert!(
        matches!(&content[1], ContentBlock::MediaContent { source: MediaSource::File { file_id }, .. } if file_id == "opaque-file")
    );
    let after = store
        .list_messages(&admitted.session.id, None)
        .await
        .unwrap();
    assert_eq!(after.len(), initial_messages + 1);
    let part = &after.last().unwrap().parts[0];
    assert_eq!(part.id, settlement.part_id);
    assert!(
        matches!(&part.content, ContentBlock::ToolResult { call_id, content: saved, is_error } if call_id == &call.id && saved == content && *is_error == failed)
    );
    let events = store
        .list_events(&admitted.session.id, None, 1000)
        .await
        .unwrap();
    let event = events
        .iter()
        .find(|event| event.kind == EventKind::ToolCallSettled)
        .unwrap();
    assert_eq!(
        event.payload["content"],
        serde_json::to_value(content).unwrap()
    );
    assert_eq!(
        event.payload["message_id"],
        serde_json::json!(settlement.message_id)
    );
    assert_eq!(event.payload["is_error"], failed);
    (after, events)
}

async fn late_collision(store: &dyn Store) {
    let (admitted, execution, owner, mut settlement) = started(store, false).await;
    let messages = store
        .list_messages(&admitted.session.id, None)
        .await
        .unwrap();
    settlement.message_id = messages[0].id.clone();
    let revision = store.session_revision(&admitted.session.id).await.unwrap();
    let phase = store
        .tool_phase(&admitted.run.id, &settlement.completion.command.target().id)
        .await
        .unwrap();
    let calls = read_calls(store, &admitted.session.id).await.unwrap();
    let events = store
        .list_events(&admitted.session.id, None, 1000)
        .await
        .unwrap();
    assert_eq!(
        execution
            .settle_tool_phase(settlement.clone(), Some(owner))
            .await
            .unwrap_err(),
        PhaseError::UnknownStoreOutcome
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        revision
    );
    assert_eq!(
        store
            .tool_phase(&admitted.run.id, &phase.phase.target.id)
            .await
            .unwrap(),
        phase
    );
    assert_eq!(
        read_calls(store, &admitted.session.id).await.unwrap(),
        calls
    );
    assert_eq!(
        store
            .list_messages(&admitted.session.id, None)
            .await
            .unwrap(),
        messages
    );
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 1000)
            .await
            .unwrap(),
        events
    );
    assert!(
        store
            .lookup_phase_mutation(&admitted.session.id, &settlement.completion.id)
            .await
            .unwrap()
            .is_none()
    );
}
async fn operation_domains_are_distinct(store: &dyn Store) {
    let (admitted, execution, owner, settlement) = started(store, false).await;
    execution
        .mutate_phase(
            settlement.completion.clone(),
            Some(PhaseWriteAuthority::Phase(owner)),
        )
        .await
        .unwrap();
    assert_eq!(
        execution
            .settle_tool_phase(settlement, None)
            .await
            .unwrap_err(),
        PhaseError::SemanticConflict
    );
    assert!(
        read_calls(store, &admitted.session.id).await.unwrap()[0]
            .result
            .is_none()
    );
}
pub(super) async fn faults(store: &dyn Store, set_fault: &impl Fn(super::FaultPoint)) {
    for point in [
        super::FaultPoint::BeforeCommit,
        super::FaultPoint::AfterCommit,
    ] {
        let (admitted, execution, owner, settlement) = started(store, false).await;
        let before = store
            .list_messages(&admitted.session.id, None)
            .await
            .unwrap();
        let events = store
            .list_events(&admitted.session.id, None, 1000)
            .await
            .unwrap();
        set_fault(point);
        let result = execution
            .settle_tool_phase(settlement.clone(), Some(owner.clone()))
            .await;
        set_fault(super::FaultPoint::Disabled);
        assert_eq!(result.unwrap_err(), PhaseError::UnknownStoreOutcome);
        let receipt = store
            .lookup_phase_mutation(&admitted.session.id, &settlement.completion.id)
            .await
            .unwrap();
        match point {
            super::FaultPoint::BeforeCommit => {
                assert!(receipt.is_none());
                assert_eq!(
                    store
                        .list_messages(&admitted.session.id, None)
                        .await
                        .unwrap(),
                    before
                );
                assert_eq!(
                    store
                        .list_events(&admitted.session.id, None, 1000)
                        .await
                        .unwrap(),
                    events
                );
                assert_eq!(
                    store.session_revision(&admitted.session.id).await.unwrap(),
                    settlement.completion.expected_revision
                );
                assert_eq!(
                    store
                        .tool_phase(&admitted.run.id, &settlement.completion.command.target().id)
                        .await
                        .unwrap()
                        .phase
                        .state,
                    PhaseState::Started
                );
                assert!(
                    read_calls(store, &admitted.session.id).await.unwrap()[0]
                        .result
                        .is_none()
                );
                assert!(matches!(
                    execution
                        .settle_tool_phase(settlement, Some(owner))
                        .await
                        .unwrap(),
                    PhaseMutationOutcome::Applied {
                        authority: None,
                        ..
                    }
                ));
            }
            super::FaultPoint::AfterCommit => {
                let receipt = receipt.unwrap();
                assert_eq!(
                    store
                        .list_messages(&admitted.session.id, None)
                        .await
                        .unwrap()
                        .len(),
                    before.len() + 1
                );
                assert_eq!(
                    store
                        .list_events(&admitted.session.id, None, 1000)
                        .await
                        .unwrap()
                        .len(),
                    events.len() + 2
                );
                assert_eq!(
                    store.session_revision(&admitted.session.id).await.unwrap(),
                    receipt.revision
                );
                assert!(
                    matches!(execution.settle_tool_phase(settlement, None).await.unwrap(), PhaseMutationOutcome::Replayed(saved) if saved == receipt)
                );
            }
            super::FaultPoint::Disabled => unreachable!(),
        }
    }
}

pub(super) async fn read_calls(
    store: &dyn Store,
    session: &crabber_core::SessionId,
) -> Result<Vec<crabber_core::ToolCallRecord>, crate::StoreError> {
    let outcome = store
        .snapshot(crate::SnapshotRequest {
            session_id: session.clone(),
            continuation: None,
            limits: crate::SnapshotLimits {
                messages: 32,
                tool_calls: 32,
                parts: 128,
                text_bytes: 1_048_576,
                encoded_bytes: 4_194_304,
            },
        })
        .await?;
    let crate::SnapshotOutcome::Page(page) = outcome else {
        panic!("small fixture must fit one snapshot page");
    };
    assert!(page.continuation.is_none());
    Ok(page.tool_calls)
}

async fn uncertain_cannot_be_completed(store: &dyn Store) {
    let (admitted, execution, owner, mut settlement) = started(store, false).await;
    let (uncertain, _) = applied(
        execution
            .mutate_phase(
                mutation(
                    settlement.completion.expected_revision,
                    PhaseCommand::MarkStoppedUncertain {
                        target: settlement.completion.command.target().clone(),
                        claim: super::resolver(),
                    },
                ),
                None,
            )
            .await
            .unwrap(),
    );
    let messages = store
        .list_messages(&admitted.session.id, None)
        .await
        .unwrap();
    settlement.completion.id = crabber_core::ControlId::new();
    settlement.completion.expected_revision = uncertain.revision;
    assert_eq!(
        execution
            .settle_tool_phase(settlement, Some(owner))
            .await
            .unwrap_err(),
        PhaseError::StaleOwner
    );
    assert_eq!(
        store
            .list_messages(&admitted.session.id, None)
            .await
            .unwrap(),
        messages
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        uncertain.revision
    );
    assert_eq!(
        store
            .tool_phase(&admitted.run.id, &uncertain.phase.target.id)
            .await
            .unwrap()
            .phase
            .state,
        PhaseState::Uncertain
    );
    assert_eq!(
        store
            .list_unfinished_tool_calls(&admitted.run.id)
            .await
            .unwrap()
            .len(),
        1
    );
}
