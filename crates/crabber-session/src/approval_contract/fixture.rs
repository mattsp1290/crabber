use crate::{
    ApprovalAnswer, ApprovalContinuationClaim, ApprovalResolverClaim, ApprovalSuspension,
    AttemptAuthority, AttemptCommand, AttemptMutation, AttemptMutationOutcome, BoundAdmitRequest,
    CallApproval, ContinuationTarget, ExecutionStore, PreparedApproval, Store,
};
use crabber_core::{
    AuthorityRequirement, Clock, ContinuationGeneration, ContinuationId, ControlId, EventKind,
    EventRecord, InputFingerprint, InteractionDescription, InteractionId, InteractionKind,
    ManualClock, PhaseGeneration, PhaseId, Run, SessionId, ToolCallId, ToolCallRecord,
    ToolCallStatus, Usage,
};
use serde_json::json;

pub(crate) struct Fixture {
    pub run: Run,
    pub execution: Box<dyn ExecutionStore>,
    pub owner: AttemptAuthority,
    pub batch: ApprovalSuspension,
}
pub(crate) async fn staged(store: &dyn Store, clock: &ManualClock) -> Fixture {
    let mut prepared =
        crate::typed_admission_contract::fixture(&SessionId::new(), clock.now(), "approval");
    prepared.keyed.request.session_id = None;
    let admitted = store
        .admit_bound_run(BoundAdmitRequest {
            request: prepared.keyed.request,
            binding: prepared.binding,
        })
        .await
        .unwrap();
    staged_on(store, clock, admitted).await
}
/// Authentic admission supplied by a preparation fixture, never a synthetic Run.
pub(crate) async fn staged_on(
    store: &dyn Store,
    clock: &ManualClock,
    admitted: crate::AdmitOutcome,
) -> Fixture {
    let execution = store.execution(admitted.fence).await.unwrap();
    if store
        .admission_execution_state(&admitted.run.id)
        .await
        .unwrap()
        == Some(crate::AdmissionExecutionState::Unstarted)
    {
        execution.begin_admission_execution().await.unwrap();
    }
    let reserve = crate::attempt_contract::reserve(
        store.session_revision(&admitted.session.id).await.unwrap(),
    );
    let AttemptMutationOutcome::Applied {
        receipt,
        authority: Some(owner),
    } = execution.mutate_attempt(reserve, None).await.unwrap()
    else {
        panic!("fresh reserve")
    };
    let attempt = receipt.attempt.target;
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: receipt.revision,
                command: AttemptCommand::Begin {
                    target: attempt.clone(),
                },
            },
            Some(owner.clone()),
        )
        .await
        .unwrap();
    let next_attempt = crate::AttemptTarget {
        id: crabber_core::AttemptId::new(),
        generation: attempt.generation.next().unwrap(),
    };
    let calls = prepared_calls(execution.as_ref(), &admitted.run, &next_attempt, clock).await;
    let batch = ApprovalSuspension {
        id: ControlId::new(),
        expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
        attempt,
        expected_plan: InputFingerprint::new(admitted.run.plan_fingerprint.clone()).unwrap(),
        continuation: ContinuationTarget {
            id: ContinuationId::new(),
            generation: ContinuationGeneration::INITIAL,
        },
        next_attempt,
        calls,
        usage: Usage::default(),
        turns_used: Some(1),
        source_turn_id: None,
        source_created_at: None,
    };
    Fixture {
        run: admitted.run,
        execution,
        owner,
        batch,
    }
}
pub(crate) async fn answer(
    store: &dyn Store,
    f: &Fixture,
    index: usize,
    reply: crate::ApprovalReply,
) -> ApprovalAnswer {
    let CallApproval::Ask { id, description } = &f.batch.calls[index].decision else {
        panic!("question")
    };
    ApprovalAnswer {
        id: ControlId::new(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        session_id: f.run.session_id.clone(),
        run_id: f.run.id.clone(),
        continuation: f.batch.continuation.clone(),
        interaction_id: id.clone(),
        resolver: ApprovalResolverClaim {
            resolver_id: "human".into(),
            authority: description.authority().clone(),
            authority_claim: "host-verified".into(),
        },
        reply,
    }
}
pub(crate) async fn claim(store: &impl Store, f: &Fixture) -> ApprovalContinuationClaim {
    ApprovalContinuationClaim {
        id: ControlId::new(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        session_id: f.run.session_id.clone(),
        run_id: f.run.id.clone(),
        continuation: f.batch.continuation.clone(),
        expected_plan: f.batch.expected_plan.clone(),
        owner: "new-host".into(),
        lease_ms: 30_000,
    }
}

async fn prepared_calls(
    execution: &dyn ExecutionStore,
    run: &Run,
    next_attempt: &crate::AttemptTarget,
    clock: &ManualClock,
) -> Vec<PreparedApproval> {
    let mut calls = Vec::new();
    for index in 0..3 {
        let call = ToolCallRecord {
            id: ToolCallId::from(format!("provider:/λ/{}:{index}", run.id)),
            run_id: run.id.clone(),
            name: "prepared".into(),
            arguments: json!({"exact":index}),
            status: ToolCallStatus::Pending,
            retry_safe: false,
            result: None,
        };
        let bound = crate::phase::call_binding(&call).unwrap();
        execution
            .create_tool_call(
                call.clone(),
                EventRecord {
                    cursor: None,
                    session_id: run.session_id.clone(),
                    run_id: run.id.clone(),
                    turn_id: None,
                    kind: EventKind::ToolCallPending,
                    payload: json!({"call_id":call.id}),
                    correlation: None,
                    live_only: false,
                    created_at: clock.now(),
                },
            )
            .await
            .unwrap();
        calls.push(PreparedApproval {
            target: crate::PhaseTarget {
                id: PhaseId::new(),
                generation: PhaseGeneration::INITIAL,
                call_id: call.id,
                attempt: next_attempt.clone(),
            },
            call_binding: bound,
            decision: if index == 2 {
                CallApproval::Allow
            } else {
                CallApproval::Ask {
                    id: InteractionId::new(),
                    description: InteractionDescription::new(
                        "approve",
                        InteractionKind::Approval,
                        "PRIVATE_PROMPT",
                        json!({"type":"boolean"}),
                        AuthorityRequirement::new("host-approval", "v1").unwrap(),
                        Some(1000),
                    )
                    .unwrap(),
                }
            },
        });
    }

    calls
}

#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
/// A child writer will acquire and begin this first native attempt itself.
pub(crate) async fn unstarted_batch(
    store: &impl Store,
    clock: &ManualClock,
) -> (Run, crabber_core::RunFence, ApprovalSuspension) {
    let mut prepared = crate::typed_admission_contract::fixture(
        &SessionId::new(),
        clock.now(),
        "approval-process",
    );
    prepared.keyed.request.session_id = None;
    let admitted = store
        .admit_bound_run(BoundAdmitRequest {
            request: prepared.keyed.request,
            binding: prepared.binding,
        })
        .await
        .unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let attempt = crate::AttemptTarget {
        id: crabber_core::AttemptId::new(),
        generation: crabber_core::AttemptGeneration::INITIAL,
    };
    let next_attempt = crate::AttemptTarget {
        id: crabber_core::AttemptId::new(),
        generation: attempt.generation.next().unwrap(),
    };
    let calls = prepared_calls(execution.as_ref(), &admitted.run, &next_attempt, clock).await;
    let revision = store.session_revision(&admitted.session.id).await.unwrap();
    let batch = ApprovalSuspension {
        id: ControlId::new(),
        expected_revision: revision.next().unwrap().next().unwrap(),
        attempt,
        expected_plan: InputFingerprint::new(admitted.run.plan_fingerprint.clone()).unwrap(),
        continuation: ContinuationTarget {
            id: ContinuationId::new(),
            generation: ContinuationGeneration::INITIAL,
        },
        next_attempt,
        calls,
        usage: Usage::default(),
        turns_used: Some(1),
        source_turn_id: None,
        source_created_at: None,
    };
    (admitted.run, admitted.fence, batch)
}
