//! Native authority is acquired before execution middleware and never on replay.
use super::{ExecutionStore, Orchestrator, RuntimeError};
use crabber_core::{
    ControlId, EventRecord, InputFingerprint, RunId, SessionId, ToolCallRecord, ToolOutput,
    ToolResultStatus, TurnId,
};
use crabber_session::{
    GrantedApproval, PhaseAuthority, PhaseCommand, PhaseError, PhaseMutation, PhaseMutationOutcome,
    PhaseOperation, PhaseQuiescence, PhaseQuiescenceResult, PhaseReceipt, PhaseResolverClaim,
    PhaseSettlement, PhaseStart, PhaseState, PhaseTarget, PhaseWriteAuthority,
};

const REVISION_RETRIES: usize = 128;
pub(super) struct OwnedPhase {
    pub target: PhaseTarget,
    authority: PhaseAuthority,
    call_binding: InputFingerprint,
}
struct ReceiptScope<'a> {
    session: &'a SessionId,
    run: &'a RunId,
    target: &'a PhaseTarget,
    call_binding: &'a InputFingerprint,
}
impl ReceiptScope<'_> {
    fn matches(
        &self,
        receipt: &PhaseReceipt,
        key: &ControlId,
        digest: &InputFingerprint,
        operation: PhaseOperation,
        state: PhaseState,
        quiescence: PhaseQuiescence,
    ) -> bool {
        receipt.operation_id == *key
            && receipt.semantic_digest == *digest
            && receipt.operation == operation
            && receipt.phase.session_id == *self.session
            && receipt.phase.run_id == *self.run
            && receipt.phase.target == *self.target
            && receipt.phase.call_binding == *self.call_binding
            && receipt.phase.state == state
            && receipt.phase.quiescence == quiescence
    }
}
fn unknown(
    run: &RunId,
    target: &PhaseTarget,
    key: Option<ControlId>,
    reason: PhaseError,
) -> RuntimeError {
    RuntimeError::PhaseOutcomeUnknown {
        run_id: run.clone(),
        phase: Box::new(target.clone()),
        operation_id: key,
        reason,
    }
}
impl Orchestrator {
    pub(super) async fn begin_tool_phase(
        &self,
        execution: &dyn ExecutionStore,
        session: &SessionId,
        run: &RunId,
        call: &ToolCallRecord,
        authorization: &GrantedApproval,
        turn: Option<TurnId>,
    ) -> Result<(OwnedPhase, EventRecord), RuntimeError> {
        let owner = self
            .owned_attempt
            .as_ref()
            .ok_or(crabber_session::AttemptError::InvalidTransition)?;
        let target = &authorization.target;
        if target.attempt != owner.target || target.call_id != call.id || call.run_id != *run {
            return Err(crabber_session::ApprovalError::SemanticConflict.into());
        }
        let scope = ReceiptScope {
            session,
            run,
            target,
            call_binding: &authorization.call_binding,
        };
        let authority = self
            .claim_tool_phase(execution, &scope, &owner.authority)
            .await?;
        let stored = self
            .store
            .get_run(run)
            .await
            .map_err(|_| unknown(run, target, None, PhaseError::UnknownStoreOutcome))?
            .ok_or_else(|| unknown(run, target, None, PhaseError::NotFound))?;
        for _ in 0..REVISION_RETRIES {
            let start =
                PhaseStart {
                    begin: PhaseMutation {
                        id: ControlId::new(),
                        expected_revision: self.store.session_revision(session).await.map_err(
                            |_| unknown(run, target, None, PhaseError::UnknownStoreOutcome),
                        )?,
                        command: PhaseCommand::Begin {
                            target: target.clone(),
                        },
                    },
                    turn_id: turn.clone(),
                    created_at: self.clock.now(),
                };
            let event = start
                .materialize(&stored, call)
                .map_err(|reason| unknown(run, target, None, reason))?;
            let key = start.begin.id.clone();
            let digest = start
                .fingerprint(session, run)
                .map_err(|reason| unknown(run, target, Some(key.clone()), reason))?;
            match execution
                .start_tool_phase(start, Some(authority.clone()))
                .await
            {
                Ok(PhaseMutationOutcome::Applied { receipt, .. })
                    if scope.matches(
                        &receipt,
                        &key,
                        &digest,
                        PhaseOperation::Begin,
                        PhaseState::Started,
                        PhaseQuiescence::Pending,
                    ) =>
                {
                    return Ok((
                        OwnedPhase {
                            target: target.clone(),
                            authority,
                            call_binding: authorization.call_binding.clone(),
                        },
                        event,
                    ));
                }
                Err(PhaseError::RevisionConflict) => (),
                Err(reason) => return Err(unknown(run, target, Some(key), reason)),
                Ok(_) => {
                    return Err(unknown(
                        run,
                        target,
                        Some(key),
                        PhaseError::UnknownStoreOutcome,
                    ));
                }
            }
        }
        Err(unknown(run, target, None, PhaseError::RevisionConflict))
    }
    async fn claim_tool_phase(
        &self,
        execution: &dyn ExecutionStore,
        scope: &ReceiptScope<'_>,
        attempt_authority: &crabber_session::AttemptAuthority,
    ) -> Result<PhaseAuthority, RuntimeError> {
        let ReceiptScope {
            session,
            run,
            target,
            ..
        } = *scope;
        for _ in 0..REVISION_RETRIES {
            let request = PhaseMutation {
                id: ControlId::new(),
                expected_revision: self.store.session_revision(session).await?,
                command: PhaseCommand::Claim {
                    target: target.clone(),
                },
            };
            let key = request.id.clone();
            let digest = request
                .fingerprint(session, run)
                .map_err(|reason| unknown(run, target, Some(key.clone()), reason))?;
            match execution
                .mutate_phase(
                    request,
                    Some(PhaseWriteAuthority::Attempt(attempt_authority.clone())),
                )
                .await
            {
                Ok(PhaseMutationOutcome::Applied {
                    receipt,
                    authority: Some(value),
                }) if scope.matches(
                    &receipt,
                    &key,
                    &digest,
                    PhaseOperation::Claim,
                    PhaseState::Claimed,
                    PhaseQuiescence::Pending,
                ) =>
                {
                    return Ok(value);
                }
                Err(PhaseError::RevisionConflict) => (),
                Err(reason) => return Err(unknown(run, target, Some(key), reason)),
                Ok(_) => {
                    return Err(unknown(
                        run,
                        target,
                        Some(key),
                        PhaseError::UnknownStoreOutcome,
                    ));
                }
            }
        }
        Err(unknown(run, target, None, PhaseError::RevisionConflict))
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn settle_owned_phase(
        &self,
        execution: &dyn ExecutionStore,
        session: &SessionId,
        run: &RunId,
        call: &ToolCallRecord,
        owner: &OwnedPhase,
        output: &ToolOutput,
        status: ToolResultStatus,
        turn: Option<TurnId>,
        duration_ms: u64,
    ) -> Result<EventRecord, RuntimeError> {
        let stored = self
            .store
            .get_run(run)
            .await
            .map_err(|_| unknown(run, &owner.target, None, PhaseError::UnknownStoreOutcome))?
            .ok_or_else(|| unknown(run, &owner.target, None, PhaseError::NotFound))?;
        let scope = ReceiptScope {
            session,
            run,
            target: &owner.target,
            call_binding: &owner.call_binding,
        };
        for _ in 0..REVISION_RETRIES {
            let settlement = PhaseSettlement {
                completion: PhaseMutation {
                    id: ControlId::new(),
                    expected_revision: self.store.session_revision(session).await.map_err(
                        |_| unknown(run, &owner.target, None, PhaseError::UnknownStoreOutcome),
                    )?,
                    command: PhaseCommand::Complete {
                        target: owner.target.clone(),
                        output: output.clone(),
                    },
                },
                result_status: status,
                message_id: crabber_core::MessageId::new(),
                part_id: crabber_core::PartId::new(),
                turn_id: turn.clone(),
                created_at: self.clock.now(),
                duration_ms,
            };
            let event = settlement
                .materialize(&stored, call)
                .map_err(|reason| unknown(run, &owner.target, None, reason))?
                .event;
            let key = settlement.completion.id.clone();
            let digest = settlement
                .fingerprint(session, run)
                .map_err(|reason| unknown(run, &owner.target, Some(key.clone()), reason))?;
            match execution
                .settle_tool_phase(settlement, Some(owner.authority.clone()))
                .await
            {
                Ok(PhaseMutationOutcome::Applied { receipt, .. })
                    if scope.matches(
                        &receipt,
                        &key,
                        &digest,
                        PhaseOperation::Complete,
                        if output.is_error() {
                            PhaseState::Failed
                        } else {
                            PhaseState::Completed
                        },
                        PhaseQuiescence::Complete,
                    ) =>
                {
                    return Ok(event);
                }
                Err(PhaseError::RevisionConflict) => (),
                Err(reason) => return Err(unknown(run, &owner.target, Some(key), reason)),
                Ok(_) => {
                    return Err(unknown(
                        run,
                        &owner.target,
                        Some(key),
                        PhaseError::UnknownStoreOutcome,
                    ));
                }
            }
        }
        Err(unknown(
            run,
            &owner.target,
            None,
            PhaseError::RevisionConflict,
        ))
    }
    pub(super) async fn stopped_tool_phase(
        &self,
        execution: &dyn ExecutionStore,
        session: &SessionId,
        run: &RunId,
        owner: &OwnedPhase,
        complete: bool,
    ) -> RuntimeError {
        let resolver = PhaseResolverClaim { resolver_id: "crabber-native-phase-driver".into(),
            authority_claim: "the owned phase future ended without an acknowledged result; cleanup state is recorded separately".into() };
        for command in [
            PhaseCommand::MarkStoppedUncertain {
                target: owner.target.clone(),
                claim: resolver.clone(),
            },
            PhaseCommand::RecordQuiescence {
                target: owner.target.clone(),
                result: if complete {
                    PhaseQuiescenceResult::Complete
                } else {
                    PhaseQuiescenceResult::Incomplete
                },
                host_claim: Some(resolver),
            },
        ] {
            let mut acknowledged = false;
            for _ in 0..REVISION_RETRIES {
                let Ok(revision) = self.store.session_revision(session).await else {
                    return unknown(run, &owner.target, None, PhaseError::UnknownStoreOutcome);
                };
                let request = PhaseMutation {
                    id: ControlId::new(),
                    expected_revision: revision,
                    command: command.clone(),
                };
                let key = request.id.clone();
                match execution.mutate_phase(request, None).await {
                    Ok(_) => {
                        acknowledged = true;
                        break;
                    }
                    Err(PhaseError::RevisionConflict) => (),
                    Err(reason) => return unknown(run, &owner.target, Some(key), reason),
                }
            }
            if !acknowledged {
                return unknown(run, &owner.target, None, PhaseError::RevisionConflict);
            }
        }
        unknown(run, &owner.target, None, PhaseError::Uncertain)
    }
}
