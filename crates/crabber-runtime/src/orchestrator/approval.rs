//! Freeze permissions for the complete prepared batch before any call runs.
use super::{
    EventPublished, ExecutionStore, GuardContext, GuardDecision, Orchestrator, PermissionDecision,
    RecordedCall, RunPlan, RunSession, RuntimeError, Value, ensure_lease, read_recorded_call,
};
use crabber_core::{
    AuthorityRequirement, ContinuationGeneration, ContinuationId, ControlId,
    InteractionDescription, InteractionId, InteractionKind, PhaseGeneration, PhaseId, RunId,
    ToolCallRecord, TurnId, Usage,
};
use crabber_session::{
    ApprovalError, ApprovalSuspension, AttemptError, AttemptTarget, CallApproval,
    ContinuationTarget, GrantedApproval, GrantedDecision, PhaseTarget, PreparedApproval,
};
use std::sync::atomic::AtomicBool;

impl Orchestrator {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn authorize_tools(
        &self,
        execution: &dyn ExecutionStore,
        session: &RunSession,
        run_id: &RunId,
        plan: &RunPlan,
        calls: &[ToolCallRecord],
        usage: &Usage,
        turns_used: usize,
        turn: Option<TurnId>,
        lease_lost: &AtomicBool,
    ) -> Result<Vec<GrantedApproval>, RuntimeError> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }
        if calls.len() > crabber_session::MAX_APPROVAL_CALLS {
            return Err(RuntimeError::InvalidConfiguration(
                "prepared tool batch exceeds durable limit",
            ));
        }
        let owner = self
            .owned_attempt
            .as_ref()
            .ok_or(AttemptError::InvalidTransition)?;
        let decisions = calls
            .iter()
            .map(|call| self.permission(plan, session, run_id, call))
            .collect::<Result<Vec<_>, _>>()?;
        let waiting = decisions
            .iter()
            .any(|decision| matches!(decision, CallApproval::Ask { .. }));
        let target_attempt = if waiting {
            AttemptTarget {
                id: crabber_core::AttemptId::new(),
                generation: owner
                    .target
                    .generation
                    .next()
                    .map_err(|_| AttemptError::GenerationExhausted)?,
            }
        } else {
            owner.target.clone()
        };
        let prepared = calls
            .iter()
            .zip(decisions)
            .map(|(call, decision)| {
                PreparedApproval::new(
                    PhaseTarget {
                        id: PhaseId::new(),
                        generation: PhaseGeneration::INITIAL,
                        call_id: call.id.clone(),
                        attempt: target_attempt.clone(),
                    },
                    call,
                    decision,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        if waiting {
            ensure_lease(lease_lost)?;
            self.suspend_approvals(
                execution,
                run_id,
                plan,
                prepared,
                target_attempt,
                usage,
                turns_used,
                turn,
            )
            .await?;
            return Err(RuntimeError::Paused);
        }
        Ok(prepared
            .into_iter()
            .map(|call| GrantedApproval {
                target: call.target,
                call_binding: call.call_binding,
                decision: match call.decision {
                    CallApproval::Allow => GrantedDecision::Allow,
                    CallApproval::Deny => GrantedDecision::Deny,
                    CallApproval::NotRequired => GrantedDecision::NotRequired,
                    CallApproval::Ask { .. } => unreachable!("pending batch was suspended"),
                },
            })
            .collect())
    }
    fn permission(
        &self,
        plan: &RunPlan,
        session: &RunSession,
        run_id: &RunId,
        call: &ToolCallRecord,
    ) -> Result<CallApproval, RuntimeError> {
        if call.status != crabber_core::ToolCallStatus::Pending || call.result.is_some() {
            return Err(RuntimeError::ToolOutcomeUnknown {
                run_id: run_id.clone(),
                call_id: call.id.clone(),
            });
        }
        if call.run_id != *run_id {
            return Err(super::StoreError::NotFound.into());
        }
        let Some(tool) = plan.tools.iter().find(|tool| tool.info.name == call.name) else {
            return Ok(CallApproval::NotRequired);
        };
        let RecordedCall::Execute(arguments) =
            read_recorded_call(call.arguments.clone(), &call.name, true)
        else {
            return Ok(CallApproval::NotRequired);
        };
        let guards: Vec<_> = plan
            .guards
            .iter()
            .map(|guard| {
                guard.check_with_context(GuardContext {
                    tool: &tool.info,
                    arguments: &arguments,
                    call_id: &call.id,
                    session_id: &session.id,
                    run_id,
                })
            })
            .collect();
        if guards.contains(&GuardDecision::Deny)
            || plan
                .restrictions
                .iter()
                .any(|set| !set.iter().any(|name| name == &tool.info.name))
        {
            return Ok(CallApproval::Deny);
        }
        let decision = if guards.contains(&GuardDecision::Ask) {
            PermissionDecision::Ask
        } else if guards.contains(&GuardDecision::Allow) {
            PermissionDecision::Allow
        } else {
            self.policy.decide(&tool.info, &arguments)
        };
        Ok(match decision {
            PermissionDecision::Allow => CallApproval::Allow,
            PermissionDecision::Deny => CallApproval::Deny,
            PermissionDecision::Ask => CallApproval::Ask {
                id: InteractionId::new(),
                description: description(&tool.info.name)?,
            },
        })
    }
    #[allow(clippy::too_many_arguments)]
    async fn suspend_approvals(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        plan: &RunPlan,
        calls: Vec<PreparedApproval>,
        next_attempt: AttemptTarget,
        usage: &Usage,
        turns_used: usize,
        turn: Option<TurnId>,
    ) -> Result<(), RuntimeError> {
        let owner = self
            .owned_attempt
            .as_ref()
            .ok_or(AttemptError::InvalidTransition)?;
        if !self
            .execution_tasks
            .join_run_resources(run_id, super::attempt::RESOURCE_JOIN_BOUND)
            .await
        {
            return Err(RuntimeError::AttemptCleanupIncomplete {
                run_id: run_id.clone(),
            });
        }
        let run = self
            .store
            .get_run(run_id)
            .await?
            .ok_or(super::StoreError::NotFound)?;
        let generation = self
            .store
            .approval_snapshot(&run.session_id, &run.id)
            .await?
            .map(|previous| {
                previous
                    .continuation
                    .generation
                    .next()
                    .map_err(|_| ApprovalError::GenerationConflict)
            })
            .transpose()?
            .unwrap_or(ContinuationGeneration::INITIAL);
        let continuation = ContinuationTarget {
            id: ContinuationId::new(),
            generation,
        };
        for _ in 0..4 {
            let now = self.clock.now();
            let batch = ApprovalSuspension {
                id: ControlId::new(),
                expected_revision: self.store.session_revision(&run.session_id).await?,
                attempt: owner.target.clone(),
                expected_plan: crabber_core::InputFingerprint::new(plan.fingerprint.to_string())
                    .map_err(|_| ApprovalError::InvalidRequest)?,
                continuation: continuation.clone(),
                next_attempt: next_attempt.clone(),
                calls: calls.clone(),
                usage: usage.clone(),
                turns_used: Some(
                    u64::try_from(turns_used).map_err(|_| ApprovalError::InvalidRequest)?,
                ),
                source_turn_id: turn.clone(),
                source_created_at: Some(now),
            };
            let event = batch.conclusion(&run, now)?.materialize(&run)?;
            let key = batch.id.clone();
            match execution
                .suspend_for_approval(batch, owner.authority.clone())
                .await
            {
                Ok(_) => {
                    self.observer.emit(&event);
                    plan.dispatcher
                        .notify::<EventPublished>(
                            serde_json::to_value(&event).unwrap_or(Value::Null),
                        )
                        .await;
                    return Ok(());
                }
                Err(ApprovalError::RevisionConflict) => (),
                Err(reason) => {
                    return Err(RuntimeError::ApprovalBoundaryUnknown {
                        run_id: run_id.clone(),
                        operation_id: key,
                        reason,
                    });
                }
            }
        }
        Err(ApprovalError::RevisionConflict.into())
    }
}
fn description(name: &str) -> Result<InteractionDescription, RuntimeError> {
    let authority = AuthorityRequirement::new("host-approval", "v1")
        .map_err(|_| ApprovalError::InvalidRequest)?;
    InteractionDescription::new(
        "tool-approval",
        InteractionKind::Approval,
        format!("Approve the prepared {name} call."),
        serde_json::json!({"type":"boolean"}),
        authority,
        None,
    )
    .map_err(|_| ApprovalError::InvalidRequest.into())
}
