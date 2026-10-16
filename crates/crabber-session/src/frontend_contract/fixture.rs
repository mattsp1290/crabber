use crate::*;
use crabber_core::{
    ContentBlock, ControlId, FrontendTool, Message, MessageId, Part, PartId, PartKind,
    PhaseGeneration, PhaseId, Role, SessionId, ToolCallId, ToolCallRecord, ToolCallStatus,
};
use time::OffsetDateTime;
pub(crate) struct Fixture {
    pub execution: Box<dyn ExecutionStore>,
    pub authority: AttemptAuthority,
    pub request: FrontendCallStaging,
}
#[allow(clippy::too_many_lines)] // Original admission, native Begin, and assistant declaration form one fixture.
pub(crate) async fn fixture(store: &impl Store) -> Fixture {
    let now = OffsetDateTime::UNIX_EPOCH;
    let mut prepared = crate::typed_admission_contract::fixture(&SessionId::new(), now, "frontend");
    let descriptor = FrontendTool {
        name: "frontend".into(),
        description: "External only".into(),
        input_parameters: serde_json::json!({"type":"object","required":["location"],"properties":{"location":{"type":"string"}}}),
        result_contract: Some(
            serde_json::json!({"type":"object","required":["content","is_error"],"properties":{"content":{"type":"array","minItems":1},"is_error":{"type":"boolean"}}}),
        ),
    };
    let mut wire = serde_json::to_value(prepared.binding.input_envelope.as_ref().unwrap()).unwrap();
    wire["frontend_tools"] = serde_json::json!([descriptor]);
    prepared = crate::typed_admission_contract::prepare(
        prepared,
        serde_json::from_value(wire).unwrap(),
        now,
    );
    prepared.keyed.request.session_id = None;
    let admitted = store
        .admit_bound_run(BoundAdmitRequest {
            request: prepared.keyed.request,
            binding: prepared.binding,
        })
        .await
        .unwrap();
    let (admitted, execution, authority, planned) =
        crate::phase_contract::fixture_for_admission(store, admitted).await;
    let call = ToolCallRecord {
        id: ToolCallId::from(format!("front-\"é\"-{}", ToolCallId::new().0)),
        run_id: admitted.run.id.clone(),
        name: "frontend".into(),
        arguments: serde_json::json!({"location":"here"}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    let message = MessageId::new();
    execution
        .append_message(Message {
            id: message.clone(),
            session_id: admitted.session.id.clone(),
            run_id: Some(admitted.run.id.clone()),
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
            created_at: now,
        })
        .await
        .unwrap();
    let request = FrontendCallStaging {
        id: ControlId::new(),
        expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
        session_id: admitted.session.id,
        run_id: admitted.run.id,
        parent: planned.command.target().attempt.clone(),
        call,
        descriptor_fingerprint: crate::frontend_descriptor_fingerprint(&descriptor).unwrap(),
        descriptor,
        message_id: message,
        source_turn_id: None,
        created_at: now,
    };
    Fixture {
        execution,
        authority,
        request,
    }
}
pub(crate) async fn suspension(
    store: &impl Store,
    request: &FrontendCallStaging,
) -> ToolContinuationSuspension {
    let next_attempt = crate::AttemptTarget {
        id: crabber_core::AttemptId::new(),
        generation: request.parent.generation.next().unwrap(),
    };
    let frontend = crate::FrozenFrontendCall {
        target: crate::PhaseTarget {
            id: PhaseId::new(),
            generation: PhaseGeneration::INITIAL,
            call_id: request.call.id.clone(),
            attempt: next_attempt.clone(),
        },
        call_binding: crate::phase::call_binding(&request.call).unwrap(),
        descriptor: request.descriptor.clone(),
        descriptor_fingerprint: request.descriptor_fingerprint.clone(),
        arguments: request.call.arguments.clone(),
    };
    let run = store.get_run(&request.run_id).await.unwrap().unwrap();
    let calls = store
        .list_unfinished_tool_calls(&run.id)
        .await
        .unwrap()
        .iter()
        .map(|call| crate::PreparedApproval {
            target: if call.id == request.call.id {
                frontend.target.clone()
            } else {
                crate::PhaseTarget {
                    id: PhaseId::new(),
                    generation: PhaseGeneration::INITIAL,
                    call_id: call.id.clone(),
                    attempt: next_attempt.clone(),
                }
            },
            call_binding: crate::phase::call_binding(call).unwrap(),
            decision: crate::CallApproval::NotRequired,
        })
        .collect();
    crate::ToolContinuationSuspension {
        id: ControlId::new(),
        expected_revision: store.session_revision(&request.session_id).await.unwrap(),
        attempt: request.parent.clone(),
        next_attempt: next_attempt.clone(),
        continuation: crate::ContinuationTarget {
            id: crabber_core::ContinuationId::new(),
            generation: crabber_core::ContinuationGeneration::INITIAL,
        },
        expected_plan: crabber_core::InputFingerprint::new(run.plan_fingerprint).unwrap(),
        frontend: vec![frontend.clone()],
        phases: Vec::new(),
        calls,
        usage: run.usage,
        turns_used: 1,
        source_turn_id: None,
        source_created_at: request.created_at,
    }
}
pub(crate) fn external_claim(
    request: &FrontendCallStaging,
    batch: &crate::ToolContinuationSuspension,
    revision: crabber_core::SessionRevision,
) -> crate::FrontendContinuationClaim {
    crate::FrontendContinuationClaim {
        claim: crate::ToolContinuationClaim {
            id: ControlId::new(),
            expected_revision: revision,
            session_id: request.session_id.clone(),
            run_id: request.run_id.clone(),
            continuation: batch.continuation.clone(),
            expected_plan: batch.expected_plan.clone(),
            owner: "external-result-worker".into(),
            lease_ms: 1,
        },
        resolver: crate::FrontendResolverClaim {
            resolver_id: "host".into(),
            authority_claim: "verified session owner".into(),
        },
        results: vec![
            crabber_core::FrontendToolResult::new(
                request.call.id.clone(),
                crabber_core::FrontendResultStatus::Completed,
                crabber_core::ToolOutput::from_json(&serde_json::json!("external response"))
                    .unwrap(),
            )
            .unwrap(),
        ],
    }
}
pub(crate) async fn paused(
    store: &impl Store,
) -> (Fixture, ToolContinuationSuspension, ToolContinuationReceipt) {
    let f = fixture(store).await;
    f.execution
        .stage_frontend_call(f.request.clone(), f.authority.clone())
        .await
        .unwrap();
    let batch = suspension(store, &f.request).await;
    let receipt = f
        .execution
        .suspend_tool_continuation(
            batch.clone(),
            ToolSuspensionAuthority {
                attempt: f.authority.clone(),
                phases: Vec::new(),
            },
        )
        .await
        .unwrap();
    (f, batch, receipt)
}
