use super::{
    AttemptTarget, Batch, CallApproval, ContinuationGeneration, ContinuationId, ContinuationTarget,
    ControlId, Error, GrantedApproval, GrantedDecision, InputFingerprint, Orchestrator,
    PhaseGeneration, PhaseId, PhaseTarget, PreparedApproval, RuntimeError,
    ToolContinuationSuspension, ToolSuspensionAuthority,
};
impl Orchestrator {
    pub(super) async fn freeze_tool_suspensions(
        &self,
        scope: &Batch<'_>,
        approvals: Vec<GrantedApproval>,
    ) -> Result<(ToolContinuationSuspension, ToolSuspensionAuthority), RuntimeError> {
        let pending = self
            .tool_suspensions
            .lock()
            .expect("tool suspensions poisoned")
            .clone();
        let owner = self
            .owned_attempt
            .as_ref()
            .ok_or(crabber_session::AttemptError::InvalidTransition)?;
        let (next_attempt, continuation) = self.next_tool_targets(scope).await?;
        let unfinished = self.store.list_unfinished_tool_calls(scope.run).await?;
        let mut calls = Vec::new();
        let mut phases = Vec::new();
        let mut frontend = Vec::new();
        let mut authorities = Vec::new();
        let frontend_declarations = super::super::frontend::declarations(scope.plan)?;
        for approval in approvals {
            let Some(call) = unfinished
                .iter()
                .find(|call| call.id == approval.target.call_id)
            else {
                continue;
            };
            let suspension = pending.iter().find(|s| s.record.id == call.id);
            let target = PhaseTarget {
                id: PhaseId::new(),
                generation: if let Some(s) = suspension {
                    s.owner
                        .target
                        .generation
                        .next()
                        .map_err(|_| Error::GenerationConflict)?
                } else {
                    PhaseGeneration::INITIAL
                },
                call_id: call.id.clone(),
                attempt: next_attempt.clone(),
            };
            if let Some(s) = suspension {
                phases.push(s.freeze(target.clone()));
                authorities.push(s.owner.authority.clone());
            }
            if frontend_declarations
                .iter()
                .any(|tool| tool.name == call.name)
            {
                if suspension.is_some() || approval.decision != GrantedDecision::NotRequired {
                    return Err(Error::SemanticConflict.into());
                }
                frontend.push(
                    self.freeze_frontend_call(scope, call, target.clone())
                        .await?,
                );
            }
            calls.push(PreparedApproval::new(
                target,
                call,
                match approval.decision {
                    GrantedDecision::Allow => CallApproval::Allow,
                    GrantedDecision::Deny => CallApproval::Deny,
                    GrantedDecision::NotRequired => CallApproval::NotRequired,
                },
            )?);
        }
        if phases.len() != pending.len() || calls.len() != unfinished.len() {
            return Err(self.fail_tool_suspensions(scope).await);
        }
        let batch = ToolContinuationSuspension {
            id: ControlId::new(),
            expected_revision: self.store.session_revision(&scope.session.id).await?,
            attempt: owner.target.clone(),
            next_attempt,
            continuation,
            expected_plan: InputFingerprint::new(scope.plan.fingerprint.to_string())?,
            phases,
            frontend,
            calls,
            usage: scope.usage.clone(),
            turns_used: u64::try_from(scope.turns_used).map_err(|_| Error::InvalidRequest)?,
            source_turn_id: scope.turn.clone(),
            source_created_at: self.clock.now(),
        };
        Ok((
            batch,
            ToolSuspensionAuthority {
                attempt: owner.authority.clone(),
                phases: authorities,
            },
        ))
    }
    async fn next_tool_targets(
        &self,
        scope: &Batch<'_>,
    ) -> Result<(AttemptTarget, ContinuationTarget), RuntimeError> {
        let owner = self
            .owned_attempt
            .as_ref()
            .ok_or(crabber_session::AttemptError::InvalidTransition)?;
        let next_attempt = AttemptTarget {
            id: crabber_core::AttemptId::new(),
            generation: owner
                .target
                .generation
                .next()
                .map_err(|_| Error::GenerationConflict)?,
        };
        let previous = self
            .store
            .tool_continuation_snapshot(&scope.session.id, scope.run)
            .await?;
        let continuation = ContinuationTarget {
            id: ContinuationId::new(),
            generation: previous
                .map(|old| {
                    old.continuation
                        .generation
                        .next()
                        .map_err(|_| Error::GenerationConflict)
                })
                .transpose()?
                .unwrap_or(ContinuationGeneration::INITIAL),
        };
        Ok((next_attempt, continuation))
    }
}
