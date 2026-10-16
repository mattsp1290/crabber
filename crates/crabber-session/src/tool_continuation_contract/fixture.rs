use crate::{
    AdmitOutcome, ApprovalResolverClaim, AttemptTarget, BoundAdmitRequest, CallApproval,
    ContinuationTarget, ExecutionStore, FrozenToolPhase, PhaseCommand, PhaseStart, PhaseTarget,
    PhaseWriteAuthority, PreparedApproval, RequiredToolInteraction, Store, ToolContinuationClaim,
    ToolContinuationSuspension, ToolInteractionAnswer, ToolInteractionReply,
    ToolSuspensionAuthority,
};
use crabber_core::{
    AttemptId, AuthorityRequirement, ContinuationGeneration, ContinuationId, ControlId,
    InputFingerprint, InteractionDescription, InteractionId, InteractionKind, PhaseId,
    ResumableToolIdentity, ToolCheckpoint,
};
use time::OffsetDateTime;
pub(crate) struct Fixture {
    pub admitted: AdmitOutcome,
    pub execution: Box<dyn ExecutionStore>,
    pub suspension: ToolContinuationSuspension,
    pub authority: ToolSuspensionAuthority,
}
#[allow(clippy::too_many_lines)] // One complete native lifecycle fixture/journey.
pub(crate) async fn fixture(store: &impl Store) -> Fixture {
    let mut prepared = crate::typed_admission_contract::fixture(
        &crabber_core::SessionId::new(),
        OffsetDateTime::UNIX_EPOCH,
        "tool-continuation",
    );
    prepared.keyed.request.session_id = None;
    let admitted = store
        .admit_bound_run(BoundAdmitRequest {
            request: prepared.keyed.request,
            binding: prepared.binding,
        })
        .await
        .unwrap();
    let (admitted, execution, attempt, planned) =
        crate::phase_contract::fixture_for_admission(store, admitted).await;
    let (claimed, owner) = crate::phase_contract::applied(
        execution
            .mutate_phase(planned, Some(PhaseWriteAuthority::Attempt(attempt.clone())))
            .await
            .unwrap(),
    );
    let owner = owner.unwrap();
    let (started, _) = crate::phase_contract::applied(
        execution
            .start_tool_phase(
                PhaseStart {
                    begin: crate::phase_contract::mutation(
                        claimed.revision,
                        PhaseCommand::Begin {
                            target: claimed.phase.target,
                        },
                    ),
                    turn_id: None,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                },
                Some(owner.clone()),
            )
            .await
            .unwrap(),
    );
    // Audit selectors must ignore arbitrary neighboring public payloads without
    // converting their escaped NULs through PostgreSQL JSON text operators.
    let public = execution
        .mutate_public_state(crate::PublicStateMutation {
            id: ControlId::new(),
            expected_revision: started.revision,
            contents: crabber_core::PublicStateContents::new(
                serde_json::json!({"nul\u{0}": "neighbor\u{0}", "kind": "continuation_changed"}),
                Vec::new(),
            )
            .unwrap(),
        })
        .await
        .unwrap();
    let next_attempt = AttemptTarget {
        id: AttemptId::new(),
        generation: started.phase.target.attempt.generation.next().unwrap(),
    };
    let next = PhaseTarget {
        id: PhaseId::new(),
        generation: started.phase.target.generation.next().unwrap(),
        call_id: started.phase.target.call_id.clone(),
        attempt: next_attempt.clone(),
    };
    let question = |name: &str| RequiredToolInteraction {
        id: InteractionId::new(),
        description: InteractionDescription::new(
            name,
            InteractionKind::UserInput,
            "Choose",
            serde_json::json!({"type":"string"}),
            AuthorityRequirement::new("operator", "1").unwrap(),
            None,
        )
        .unwrap(),
    };
    let suspension = ToolContinuationSuspension {
        id: ControlId::new(),
        expected_revision: public.revision,
        attempt: started.phase.target.attempt.clone(),
        next_attempt,
        continuation: ContinuationTarget {
            id: ContinuationId::new(),
            generation: ContinuationGeneration::INITIAL,
        },
        expected_plan: InputFingerprint::new(admitted.run.plan_fingerprint.clone()).unwrap(),
        phases: vec![FrozenToolPhase {
            previous: started.phase.target,
            next: next.clone(),
            call_binding: started.phase.call_binding.clone(),
            identity: ResumableToolIdentity {
                component_id: "fixture".into(),
                component_version: "1".into(),
                config_fingerprint: InputFingerprint::new("c".repeat(64)).unwrap(),
                checkpoint_schema_id: "fixture".into(),
                checkpoint_schema_version: "1".into(),
            },
            checkpoint: ToolCheckpoint::new(
                "fixture",
                "1",
                serde_json::json!({"private":"never a result"}),
            )
            .unwrap(),
            interactions: vec![question("first"), question("second")],
        }],
        calls: vec![PreparedApproval {
            target: next,
            call_binding: started.phase.call_binding,
            decision: CallApproval::NotRequired,
        }],
        usage: crabber_core::Usage {
            input_tokens: 8,
            output_tokens: 3,
        },
        turns_used: 1,
        source_turn_id: Some(crabber_core::TurnId::new()),
        source_created_at: OffsetDateTime::UNIX_EPOCH,
    };
    Fixture {
        admitted,
        execution,
        suspension,
        authority: ToolSuspensionAuthority {
            attempt,
            phases: vec![owner],
        },
    }
}
pub(crate) async fn answer(store: &impl Store, f: &Fixture, index: usize) -> ToolInteractionAnswer {
    ToolInteractionAnswer {
        id: ControlId::new(),
        expected_revision: store
            .session_revision(&f.admitted.session.id)
            .await
            .unwrap(),
        session_id: f.admitted.session.id.clone(),
        run_id: f.admitted.run.id.clone(),
        continuation: f.suspension.continuation.clone(),
        interaction_id: f.suspension.phases[0].interactions[index].id.clone(),
        resolver: ApprovalResolverClaim {
            resolver_id: "operator-a".into(),
            authority: f.suspension.phases[0].interactions[index]
                .description
                .authority()
                .clone(),
            authority_claim: "authenticated by host".into(),
        },
        reply: ToolInteractionReply::Answer {
            value: serde_json::json!(format!("answer-{index}")),
        },
    }
}
pub(crate) async fn claim(store: &impl Store, f: &Fixture) -> ToolContinuationClaim {
    ToolContinuationClaim {
        id: ControlId::new(),
        expected_revision: store
            .session_revision(&f.admitted.session.id)
            .await
            .unwrap(),
        session_id: f.admitted.session.id.clone(),
        run_id: f.admitted.run.id.clone(),
        continuation: f.suspension.continuation.clone(),
        expected_plan: f.suspension.expected_plan.clone(),
        owner: "new-native-worker".into(),
        lease_ms: 30000,
    }
}
