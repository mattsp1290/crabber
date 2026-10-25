//! Actual checkpoint + frontend + ordinary pending call in one frozen batch.
use super::super::*;
use crabber_core::*;
pub(super) async fn staged(
    store: &impl Store,
    admitted: crate::AdmitOutcome,
    context: SessionContext,
    descriptor: FrontendTool,
) -> (
    SessionContext,
    crate::ToolContinuationSuspension,
    PreparedOperation,
) {
    let mut f = crate::tool_continuation_contract::fixture::fixture_on(store, admitted).await;
    let request = stage(&f, store, descriptor).await;
    let frozen = crate::FrozenFrontendCall {
        target: crate::PhaseTarget {
            id: PhaseId::new(),
            generation: PhaseGeneration::INITIAL,
            call_id: request.call.id.clone(),
            attempt: f.suspension.next_attempt.clone(),
        },
        call_binding: crate::phase::call_binding(&request.call).unwrap(),
        descriptor: request.descriptor.clone(),
        descriptor_fingerprint: request.descriptor_fingerprint.clone(),
        arguments: request.call.arguments.clone(),
    };
    f.suspension.calls.push(crate::PreparedApproval {
        target: frozen.target.clone(),
        call_binding: frozen.call_binding.clone(),
        decision: crate::CallApproval::NotRequired,
    });
    f.suspension.frontend.push(frozen);
    let ordinary = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: f.admitted.run.id.clone(),
        name: "ordinary".into(),
        arguments: serde_json::json!({}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    f.execution
        .create_tool_call(
            ordinary.clone(),
            EventRecord {
                cursor: None,
                session_id: f.admitted.session.id.clone(),
                run_id: f.admitted.run.id.clone(),
                turn_id: None,
                kind: EventKind::ToolCallPending,
                payload: serde_json::json!({}),
                correlation: None,
                live_only: false,
                created_at: f.admitted.run.created_at,
            },
        )
        .await
        .unwrap();
    f.suspension.calls.push(crate::PreparedApproval {
        target: crate::PhaseTarget {
            id: PhaseId::new(),
            generation: PhaseGeneration::INITIAL,
            call_id: ordinary.id.clone(),
            attempt: f.suspension.next_attempt.clone(),
        },
        call_binding: crate::phase::call_binding(&ordinary).unwrap(),
        decision: crate::CallApproval::NotRequired,
    });
    f.suspension.expected_revision = store
        .session_revision(&f.admitted.session.id)
        .await
        .unwrap();
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    for index in 0..2 {
        store
            .answer_tool_interaction(
                crate::tool_continuation_contract::fixture::answer(store, &f, index).await,
            )
            .await
            .unwrap();
    }
    let claim = crate::frontend_contract::fixture::external_claim(
        &request,
        &f.suspension,
        store
            .session_revision(&f.admitted.session.id)
            .await
            .unwrap(),
    );
    (
        context,
        f.suspension,
        PreparedOperation::FrontendClaim(claim),
    )
}
async fn stage(
    f: &crate::tool_continuation_contract::fixture::Fixture,
    store: &dyn Store,
    descriptor: FrontendTool,
) -> crate::FrontendCallStaging {
    let call = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: f.admitted.run.id.clone(),
        name: descriptor.name.clone(),
        arguments: serde_json::json!({"location":"here"}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    let message = MessageId::new();
    f.execution
        .append_message(Message {
            id: message.clone(),
            session_id: f.admitted.session.id.clone(),
            run_id: Some(f.admitted.run.id.clone()),
            role: Role::Assistant,
            parent_id: None,
            parts: vec![Part {
                id: PartId::new(),
                message_id: message.clone(),
                ordinal: 0,
                kind: PartKind::FunctionToolCall,
                content: ContentBlock::ToolCall {
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: call.arguments.clone(),
                },
            }],
            created_at: f.admitted.run.created_at,
        })
        .await
        .unwrap();
    let request = crate::FrontendCallStaging {
        id: ControlId::new(),
        expected_revision: store
            .session_revision(&f.admitted.session.id)
            .await
            .unwrap(),
        session_id: f.admitted.session.id.clone(),
        run_id: f.admitted.run.id.clone(),
        parent: f.suspension.attempt.clone(),
        call,
        descriptor_fingerprint: crate::frontend_descriptor_fingerprint(&descriptor).unwrap(),
        descriptor,
        message_id: message,
        source_turn_id: f.suspension.source_turn_id.clone(),
        created_at: f.admitted.run.created_at,
    };
    f.execution
        .stage_frontend_call(request.clone(), f.authority.attempt.clone())
        .await
        .unwrap();
    request
}
