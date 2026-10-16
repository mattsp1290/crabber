use super::*;
use crate::{BoundAdmitRequest, PhaseCommand, PhaseMutation, PhaseWriteAuthority, Store};
use crabber_core::{
    ContentBlock, EventKind, FrontendTool, Message, MessageId, Part, PartId, PartKind,
    PhaseGeneration, PhaseId, Role, ToolCallRecord, ToolCallStatus,
};
use time::OffsetDateTime;
async fn fixture() -> (
    super::super::MemoryStore,
    Box<dyn crate::ExecutionStore>,
    crate::AttemptAuthority,
    FrontendCallStaging,
) {
    fixture_for_store(super::super::MemoryStore::new()).await
}
async fn fixture_for_store(
    store: super::super::MemoryStore,
) -> (
    super::super::MemoryStore,
    Box<dyn crate::ExecutionStore>,
    crate::AttemptAuthority,
    FrontendCallStaging,
) {
    let now = OffsetDateTime::now_utc();
    let mut prepared = crate::typed_admission_contract::fixture(&SessionId::new(), now, "frontend");
    let descriptor = FrontendTool {
        name: "frontend".into(),
        description: "External only".into(),
        input_parameters: serde_json::json!({"type":"object","required":["location"],"properties":{"location":{"type":"string"}}}),
        result_contract: None,
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
        crate::phase_contract::fixture_for_admission(&store, admitted).await;
    let call = ToolCallRecord {
        id: ToolCallId::new(),
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
    (store, execution, authority, request)
}
#[tokio::test]
async fn staging_binds_original_descriptor_and_assistant_call_without_native_dispatch() {
    let (store, execution, authority, request) = fixture().await;
    let before = store
        .list_events(&request.session_id, None, 100)
        .await
        .unwrap();
    let saved = execution
        .stage_frontend_call(request.clone(), authority.clone())
        .await
        .unwrap();
    assert_eq!(saved.revision, request.expected_revision.next().unwrap());
    assert_eq!(
        store
            .list_events(&request.session_id, None, 100)
            .await
            .unwrap()
            .len(),
        before.len() + 1
    );
    let origin = store
        .frontend_call_origin(&request.session_id, &request.run_id, &request.call.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        origin.descriptor_fingerprint,
        request.descriptor_fingerprint
    );
    assert_eq!(
        execution
            .stage_frontend_call(request.clone(), authority.clone())
            .await
            .unwrap(),
        saved
    );
    assert_eq!(
        store
            .lookup_frontend_staging(&request.session_id, &request.id)
            .await
            .unwrap(),
        Some(saved.clone())
    );
    assert_eq!(
        store
            .lookup_attempt_mutation(&request.session_id, &request.id)
            .await
            .unwrap_err(),
        crate::AttemptError::SemanticConflict
    );
    let phase = PhaseMutation {
        id: ControlId::new(),
        expected_revision: saved.revision,
        command: PhaseCommand::Claim {
            target: crate::PhaseTarget {
                id: PhaseId::new(),
                generation: PhaseGeneration::INITIAL,
                call_id: request.call.id.clone(),
                attempt: request.parent.clone(),
            },
        },
    };
    assert_eq!(
        execution
            .mutate_phase(phase, Some(PhaseWriteAuthority::Attempt(authority)))
            .await
            .unwrap_err(),
        crate::PhaseError::InvalidTransition
    );
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        saved.revision
    );
    assert!(
        store
            .state
            .lock()
            .unwrap()
            .phases
            .values()
            .all(|phase| phase.snapshot.target.call_id != request.call.id)
    );
}
#[tokio::test]
async fn generic_and_legacy_tool_paths_cannot_dispatch_frontend_calls() {
    let (store, execution, authority, request) = fixture().await;
    let mut ordinary = request.materialize().unwrap();
    ordinary.payload =
        serde_json::json!({"call_id":request.call.id,"name":request.call.name,"status":"pending"});
    assert!(matches!(
        execution
            .create_tool_call(request.call.clone(), ordinary)
            .await,
        Err(crate::StoreError::Validation(_))
    ));
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        request.expected_revision
    );
    let saved = execution
        .stage_frontend_call(request.clone(), authority)
        .await
        .unwrap();
    let mut running = request.materialize().unwrap();
    running.kind = EventKind::ToolCallRunning;
    running.payload = serde_json::json!({"call_id":request.call.id});
    assert!(matches!(
        execution.claim_tool_call(&request.call.id, running).await,
        Err(crate::StoreError::Conflict)
    ));
    assert!(matches!(
        execution.append_event(request.materialize().unwrap()).await,
        Err(crate::StoreError::Validation(_))
    ));
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        saved.revision
    );
}
#[tokio::test]
async fn erased_or_changed_private_frontend_origin_fails_closed_but_metadata_replays() {
    let (store, execution, authority, request) = fixture().await;
    let saved = execution
        .stage_frontend_call(request.clone(), authority.clone())
        .await
        .unwrap();
    let original = store.state.lock().unwrap().frontend_origins[&request.call.id].clone();
    store
        .state
        .lock()
        .unwrap()
        .frontend_origins
        .get_mut(&request.call.id)
        .unwrap()
        .request
        .descriptor
        .description = "changed".into();
    assert_eq!(
        store
            .frontend_call_origin(&request.session_id, &request.run_id, &request.call.id)
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    store
        .state
        .lock()
        .unwrap()
        .frontend_origins
        .insert(request.call.id.clone(), original);
    store
        .state
        .lock()
        .unwrap()
        .frontend_origins
        .remove(&request.call.id);
    assert_eq!(
        store
            .frontend_call_origin(&request.session_id, &request.run_id, &request.call.id)
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    assert_eq!(
        execution
            .stage_frontend_call(request.clone(), authority)
            .await
            .unwrap(),
        saved
    );
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        saved.revision
    );
    assert_eq!(
        store
            .list_events(&request.session_id, None, 100)
            .await
            .unwrap()
            .last()
            .unwrap()
            .kind,
        EventKind::ToolCallPending
    );
}
#[tokio::test]
async fn staging_commit_fault_rolls_back_or_replays_without_mutation() {
    let (store, execution, authority, request) = fixture().await;
    store
        .tool_continuation_fault
        .store(1, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        execution
            .stage_frontend_call(request.clone(), authority.clone())
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    assert!(
        store
            .lookup_frontend_staging(&request.session_id, &request.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        request.expected_revision
    );
    store
        .tool_continuation_fault
        .store(2, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        execution
            .stage_frontend_call(request.clone(), authority.clone())
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    let saved = store
        .lookup_frontend_staging(&request.session_id, &request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        execution
            .stage_frontend_call(request.clone(), authority)
            .await
            .unwrap(),
        saved
    );
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        saved.revision
    );
}

async fn paused_fixture(
    store: super::super::MemoryStore,
) -> (
    super::super::MemoryStore,
    FrontendCallStaging,
    crate::ToolContinuationSuspension,
    crate::ToolContinuationReceipt,
) {
    let (store, execution, authority, request) = fixture_for_store(store).await;
    execution
        .stage_frontend_call(request.clone(), authority.clone())
        .await
        .unwrap();
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
    let run = store.state.lock().unwrap().runs[&request.run_id].clone();
    let calls = store
        .state
        .lock()
        .unwrap()
        .calls
        .values()
        .filter(|call| call.run_id == run.id)
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
    let batch = crate::ToolContinuationSuspension {
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
    };
    let paused = execution
        .suspend_tool_continuation(
            batch.clone(),
            crate::ToolSuspensionAuthority {
                attempt: authority,
                phases: Vec::new(),
            },
        )
        .await
        .unwrap();
    (store, request, batch, paused)
}
#[tokio::test]
async fn frontend_pause_requires_complete_results_and_replays_without_authority() {
    let (store, request, batch, paused) = paused_fixture(super::super::MemoryStore::new()).await;
    let frontend = batch.frontend[0].clone();
    let view = store
        .tool_continuation_snapshot(&request.session_id, &request.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.frontend_calls.len(), 1);
    assert!(!view.frontend_calls[0].consumed);
    assert_eq!(view.revision, paused.revision);
    let mut claim = external_claim(&request, &batch, paused.revision);
    claim.claim.lease_ms = 30000;
    assert_eq!(
        store
            .claim_tool_continuation(claim.claim.clone())
            .await
            .unwrap_err(),
        Error::InvalidTransition
    );
    let mut wrong = claim.clone();
    wrong.results = vec![
        crabber_core::FrontendToolResult::new(
            ToolCallId::new(),
            crabber_core::FrontendResultStatus::Completed,
            crabber_core::ToolOutput::new(Vec::new(), false).unwrap(),
        )
        .unwrap(),
    ];
    assert_eq!(
        store.claim_frontend_continuation(wrong).await.unwrap_err(),
        Error::SemanticConflict
    );
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        paused.revision
    );
    let crate::ToolContinuationClaimOutcome::Applied { receipt, grant } = store
        .claim_frontend_continuation(claim.clone())
        .await
        .unwrap()
    else {
        panic!("fresh authority missing")
    };
    assert!(grant.phases.is_empty());
    assert_eq!(grant.frontend.len(), 1);
    assert_eq!(grant.frontend[0].target, frontend.target);
    assert_eq!(
        store
            .tool_phase(&request.run_id, &frontend.target.id)
            .await
            .unwrap()
            .phase
            .state,
        crate::PhaseState::Claimed
    );
    assert!(
        matches!(store.claim_frontend_continuation(claim.clone()).await.unwrap(), crate::ToolContinuationClaimOutcome::Replayed(saved) if saved == receipt)
    );
    claim.resolver.authority_claim = "changed original assertion".into();
    assert_eq!(
        store.claim_frontend_continuation(claim).await.unwrap_err(),
        Error::SemanticConflict
    );
    assert!(
        store
            .tool_continuation_snapshot(&request.session_id, &request.run_id)
            .await
            .unwrap()
            .unwrap()
            .frontend_calls[0]
            .consumed
    );
}
fn external_claim(
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
#[tokio::test]
async fn frontend_recovery_rotates_only_positively_unstarted_authority() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(OffsetDateTime::now_utc()));
    let (store, request, batch, paused) =
        paused_fixture(super::super::MemoryStore::with_clock(clock.clone())).await;
    let claim = external_claim(&request, &batch, paused.revision);
    let crate::ToolContinuationClaimOutcome::Applied {
        receipt,
        grant: old,
    } = store
        .claim_frontend_continuation(claim.clone())
        .await
        .unwrap()
    else {
        panic!("fresh claim")
    };
    let recovery = crate::FrontendContinuationRecovery {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        original: claim.clone(),
        attempt: batch.next_attempt.clone(),
        owner: "new-result-worker".into(),
        lease_ms: 30000,
    };
    assert_eq!(
        store
            .recover_frontend_continuation(recovery.clone())
            .await
            .unwrap_err(),
        Error::LiveOwner
    );
    let old_execution = store.execution(old.fence.clone()).await.unwrap();
    clock.set(crabber_core::Clock::now(clock.as_ref()) + time::Duration::seconds(1));
    let crate::ToolContinuationClaimOutcome::Applied {
        receipt: recovered,
        grant,
    } = store
        .recover_frontend_continuation(recovery.clone())
        .await
        .unwrap()
    else {
        panic!("recovered authority")
    };
    assert_eq!(grant.frontend[0].target, old.frontend[0].target);
    assert_eq!(grant.frontend[0].result, old.frontend[0].result);
    let begin = crate::AttemptMutation {
        id: ControlId::new(),
        expected_revision: recovered.revision,
        command: crate::AttemptCommand::Begin {
            target: batch.next_attempt,
        },
    };
    assert_eq!(
        old_execution
            .mutate_attempt(begin.clone(), Some(old.authority))
            .await
            .unwrap_err(),
        crate::AttemptError::StaleOwner
    );
    let execution = store.execution(grant.fence).await.unwrap();
    execution
        .mutate_attempt(begin, Some(grant.authority))
        .await
        .unwrap();
    assert!(
        matches!(store.recover_frontend_continuation(recovery.clone()).await.unwrap(), crate::ToolContinuationClaimOutcome::Replayed(saved) if saved == recovered)
    );
    let mut changed = recovery;
    changed.id = ControlId::new();
    changed.expected_revision = store.session_revision(&request.session_id).await.unwrap();
    assert_eq!(
        store
            .recover_frontend_continuation(changed)
            .await
            .unwrap_err(),
        Error::CleanupBlocked
    );
    assert!(
        matches!(store.claim_frontend_continuation(claim).await.unwrap(), crate::ToolContinuationClaimOutcome::Replayed(saved) if saved == receipt)
    );
}
#[tokio::test]
async fn frontend_claim_faults_and_erased_external_results_preserve_original_metadata() {
    let (store, request, batch, paused) = paused_fixture(super::super::MemoryStore::new()).await;
    let claim = external_claim(&request, &batch, paused.revision);
    store
        .tool_continuation_fault
        .store(1, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        store
            .claim_frontend_continuation(claim.clone())
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    assert!(
        store
            .lookup_tool_continuation(&request.session_id, &claim.claim.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        paused.revision
    );
    store
        .tool_continuation_fault
        .store(2, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        store
            .claim_frontend_continuation(claim.clone())
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    let receipt = store
        .lookup_tool_continuation(&request.session_id, &claim.claim.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(store.claim_frontend_continuation(claim.clone()).await.unwrap(), crate::ToolContinuationClaimOutcome::Replayed(saved) if saved == receipt)
    );
    {
        let mut state = store.state.lock().unwrap();
        let record = state
            .tool_continuations
            .get_mut(&batch.continuation.id)
            .unwrap();
        record.claim = None;
        record.frontend_claim = None;
        record.state = crate::ToolContinuationState::Waiting;
    }
    assert_eq!(
        store
            .tool_continuation_snapshot(&request.session_id, &request.run_id)
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    assert!(
        matches!(store.claim_frontend_continuation(claim).await.unwrap(), crate::ToolContinuationClaimOutcome::Replayed(saved) if saved == receipt)
    );
}

#[tokio::test]
async fn memory_frontend_fault_contract() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    crate::frontend_contract::run_fault_contract(&store, &clock, |fault| {
        store
            .tool_continuation_fault
            .store(fault, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
}
