//! One-shot dispatch authority for a native worker. Durable metadata is never a permit.
use super::{ExecutionStore, Orchestrator, RuntimeError};
use crabber_core::{AttemptGeneration, AttemptId, ControlId, RunId, SessionId};
use crabber_session::{
    AttemptAuthority, AttemptCleanup, AttemptCommand, AttemptError, AttemptMutation,
    AttemptMutationOutcome, AttemptState, AttemptTarget,
};

pub(super) const RESOURCE_JOIN_BOUND: std::time::Duration = std::time::Duration::from_secs(1);
pub(super) const DRIVER_COMPONENT: &str = "crabber.native-attempt-driver";
pub(super) const DRIVER_VERSION: &str = "1:resource-join-bound-ms=1000";

pub(super) struct OwnedAttempt {
    pub target: AttemptTarget,
    pub authority: AttemptAuthority,
}

impl Orchestrator {
    /// Only an acknowledged fresh Begin allows the caller to enter business work.
    pub(super) async fn begin_owned_attempt(
        &self,
        execution: &dyn ExecutionStore,
        session: &SessionId,
        run: &RunId,
        resume: bool,
    ) -> Result<OwnedAttempt, RuntimeError> {
        let previous = self.store.latest_execution_attempt(run).await?;
        let command = match previous {
            None => AttemptCommand::ReserveBound {
                target: AttemptTarget {
                    id: AttemptId::new(),
                    generation: AttemptGeneration::INITIAL,
                },
            },
            Some(previous) if resume => match previous.attempt.state {
                AttemptState::Unstarted => AttemptCommand::ReclaimUnstarted {
                    target: previous.attempt.target,
                },
                AttemptState::Suspended if previous.attempt.cleanup == AttemptCleanup::Complete => {
                    AttemptCommand::Reserve {
                        target: AttemptTarget {
                            id: AttemptId::new(),
                            generation: previous
                                .attempt
                                .target
                                .generation
                                .next()
                                .map_err(|_| AttemptError::GenerationExhausted)?,
                        },
                    }
                }
                _ => return Err(AttemptError::Busy.into()),
            },
            _ => return Err(AttemptError::InvalidTransition.into()),
        };
        let (receipt, authority) = self
            .apply_attempt(execution, session, command, None)
            .await?;
        let authority = authority.ok_or(AttemptError::UnknownStoreOutcome)?;
        let target = receipt.attempt.target;
        self.apply_attempt(
            execution,
            session,
            AttemptCommand::Begin {
                target: target.clone(),
            },
            Some(authority.clone()),
        )
        .await?;
        Ok(OwnedAttempt { target, authority })
    }

    async fn apply_attempt(
        &self,
        execution: &dyn ExecutionStore,
        session: &SessionId,
        command: AttemptCommand,
        authority: Option<AttemptAuthority>,
    ) -> Result<(crabber_session::AttemptReceipt, Option<AttemptAuthority>), RuntimeError> {
        // A definite revision conflict commits nothing. Every other result,
        // including replay or a lost acknowledgment, stops dispatch.
        for _ in 0..4 {
            let request = AttemptMutation {
                id: ControlId::new(),
                expected_revision: self.store.session_revision(session).await?,
                command: command.clone(),
            };
            match execution.mutate_attempt(request, authority.clone()).await {
                Ok(AttemptMutationOutcome::Applied { receipt, authority }) => {
                    return Ok((receipt, authority));
                }
                Err(AttemptError::RevisionConflict) => (),
                Ok(AttemptMutationOutcome::Replayed(_)) => {
                    return Err(AttemptError::UnknownStoreOutcome.into());
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(AttemptError::RevisionConflict.into())
    }
}

impl Orchestrator {
    pub(super) async fn conclude_owned_attempt(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        boundary: crabber_session::RunBoundary,
        turn: Option<crabber_core::TurnId>,
        started_at: time::OffsetDateTime,
    ) -> Result<crabber_core::EventRecord, RuntimeError> {
        let owner = self
            .owned_attempt
            .as_ref()
            .ok_or(AttemptError::InvalidTransition)?;
        if !self
            .execution_tasks
            .join_run_resources(run_id, RESOURCE_JOIN_BOUND)
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
        let outcome = match &boundary {
            crabber_session::RunBoundary::Suspended { .. } => {
                crabber_session::AttemptFinish::Suspended
            }
            crabber_session::RunBoundary::Terminal { status, .. } => match status {
                crabber_core::RunStatus::Completed => crabber_session::AttemptFinish::Completed,
                crabber_core::RunStatus::Failed => crabber_session::AttemptFinish::Failed,
                crabber_core::RunStatus::Interrupted => crabber_session::AttemptFinish::Interrupted,
                _ => return Err(AttemptError::InvalidRequest.into()),
            },
        };
        for _ in 0..4 {
            let created_at = self.clock.now();
            let conclusion = crabber_session::AttemptConclusion {
                finish: AttemptMutation {
                    id: ControlId::new(),
                    expected_revision: self.store.session_revision(&run.session_id).await?,
                    command: AttemptCommand::Finish {
                        target: owner.target.clone(),
                        outcome,
                    },
                },
                boundary: boundary.clone(),
                created_at,
                turn_id: turn.clone(),
                elapsed: (created_at - started_at).try_into().unwrap_or_default(),
            };
            let event = conclusion.materialize(&run)?;
            match execution
                .conclude_attempt(conclusion, Some(owner.authority.clone()))
                .await
            {
                Ok(AttemptMutationOutcome::Applied { .. }) => return Ok(event),
                Err(AttemptError::RevisionConflict) => (),
                Err(AttemptError::PendingInput) => return Err(AttemptError::PendingInput.into()),
                Ok(AttemptMutationOutcome::Replayed(_)) => {
                    return Err(RuntimeError::AttemptBoundaryUnknown {
                        run_id: run_id.clone(),
                        reason: AttemptError::UnknownStoreOutcome,
                    });
                }
                Err(reason) => {
                    return Err(RuntimeError::AttemptBoundaryUnknown {
                        run_id: run_id.clone(),
                        reason,
                    });
                }
            }
        }
        Err(AttemptError::RevisionConflict.into())
    }
}
