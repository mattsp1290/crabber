//! A real closed native call prefix for physical imported-history purge checks.
use crate::{ForkOutcome, ForkReceipt, ForkSession, PhaseCommand, PhaseWriteAuthority, Store};
use crabber_core::{
    ContentBlock, EventKind, EventRecord, Message, MessageId, Part, PartId, PartKind, Role,
};

pub(super) async fn imported(store: &impl Store) -> (ForkSession, ForkReceipt) {
    let mut prepared = crate::typed_admission_contract::fixture(
        &crabber_core::SessionId::new(),
        time::OffsetDateTime::UNIX_EPOCH,
        "deletion-fork",
    );
    prepared.keyed.request.session_id = None;
    let admitted = store
        .admit_bound_run(crate::BoundAdmitRequest {
            request: prepared.keyed.request,
            binding: prepared.binding,
        })
        .await
        .unwrap();
    let (admitted, execution, owner, mut claim) =
        crate::phase_contract::fixture_for_admission(store, admitted).await;
    let call_id = claim.command.target().call_id.clone();
    let message = Message {
        id: MessageId::new(),
        session_id: admitted.session.id.clone(),
        run_id: Some(admitted.run.id.clone()),
        role: Role::Assistant,
        parent_id: None,
        parts: Vec::new(),
        created_at: admitted.run.created_at,
    };
    commit_assistant(execution.as_ref(), message, call_id).await;
    claim.expected_revision = store.session_revision(&admitted.session.id).await.unwrap();
    let (claimed, worker) = crate::phase_contract::applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let worker = worker.unwrap();
    let (begun, _) = crate::phase_contract::applied(
        execution
            .mutate_phase(
                crate::phase_contract::mutation(
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
    let (done, _) = crate::phase_contract::applied(
        execution
            .settle_tool_phase(
                crate::PhaseSettlement {
                    completion: crate::phase_contract::mutation(
                        begun.revision,
                        PhaseCommand::Complete {
                            target: begun.phase.target,
                            output: crate::phase_contract::output(),
                        },
                    ),
                    result_status: crabber_core::ToolResultStatus::Completed,
                    message_id: MessageId::new(),
                    part_id: PartId::new(),
                    turn_id: None,
                    created_at: admitted.run.created_at,
                    duration_ms: 0,
                },
                Some(worker),
            )
            .await
            .unwrap(),
    );
    assert_eq!(done.phase.quiescence, crate::PhaseQuiescence::Complete);
    let summary = store.session_summary(&admitted.session.id).await.unwrap();
    let messages = store.list_all_messages(&admitted.session.id).await.unwrap();
    let request = fork_request(summary, messages.last().unwrap().id.clone());
    let outcome = store.fork_session(request.clone()).await.unwrap();
    let ForkOutcome::Applied(receipt) = outcome else {
        panic!("safe native prefix: {outcome:?}")
    };
    (request, receipt)
}
async fn commit_assistant(
    execution: &dyn crate::ExecutionStore,
    mut message: Message,
    call: crabber_core::ToolCallId,
) {
    message.parts.push(Part {
        id: PartId::new(),
        message_id: message.id.clone(),
        ordinal: 0,
        kind: PartKind::FunctionToolCall,
        content: ContentBlock::ToolCall {
            call_id: call,
            name: "phase-owned".into(),
            arguments: serde_json::json!({}),
        },
    });
    let event = EventRecord {
        cursor: None,
        session_id: message.session_id.clone(),
        run_id: message.run_id.clone().unwrap(),
        turn_id: None,
        kind: EventKind::MessageCommitted,
        payload: serde_json::json!({"message_id":message.id,"role":Role::Assistant}),
        correlation: None,
        live_only: false,
        created_at: message.created_at,
    };
    execution.append_message(message).await.unwrap();
    execution.append_event(event).await.unwrap();
}

fn fork_request(summary: crate::SessionSummary, last: MessageId) -> ForkSession {
    ForkSession {
        id: crabber_core::ControlId::new(),
        source_session_id: summary.session_id,
        workspace_id: summary.workspace_id,
        directory: summary.directory,
        expected_source_revision: summary.revision,
        boundary: crate::ForkBoundary::AfterMessage { message_id: last },
        target_session_id: crabber_core::SessionId::new(),
        title: "physical purge fixture".into(),
        defaults: crate::SessionDefaults::default(),
        limits: crate::SnapshotLimits {
            messages: 64,
            tool_calls: 64,
            parts: 256,
            text_bytes: 1 << 20,
            encoded_bytes: 4 << 20,
        },
        max_proof_bytes: 32 << 20,
    }
}
