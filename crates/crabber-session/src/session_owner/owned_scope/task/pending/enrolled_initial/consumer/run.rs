//! One fixed turn; every native action borrows the same enrolled original.
mod administrative;
mod turn;
use super::super::{core, protocol};
use super::{Arc, Error, FreshBegun, PreparationCommitError, RetainedInitial, Step};
use crate::native_enrollment::initial_consumer::{
    CallResult, ConsumerInput, ConsumerOutput, Original,
};
use crate::session_owner::audit::RevisionState;
use crabber_core::{RunFence, SessionRevision, TurnId};

struct Run<'a> {
    retained: &'a Arc<RetainedInitial>,
    birth: Arc<protocol::EnrolledInitialData>,
    revision: SessionRevision,
    turn: TurnId,
    started: std::time::Instant,
    effect_entered: bool,
}
impl Run<'_> {
    fn fence(&self) -> Result<&RunFence, PreparationCommitError> {
        use crate::session_owner::preparation::PreparationCompletion;
        match &self.birth.completion {
            PreparationCompletion::Admitted(v) => Ok(&v.fence),
            PreparationCompletion::Keyed(crate::KeyedAdmitOutcome::Started {
                admitted, ..
            }) => Ok(&admitted.fence),
            _ => Err(Error::UnknownStoreOutcome.into()),
        }
    }
    fn accepted(&mut self, record: &CallResult) -> Result<(), PreparationCommitError> {
        self.revision = match &record.original {
            Original::Register(v) => present(
                v.begin
                    .publication
                    .as_ref()
                    .ok_or(Error::UnknownStoreOutcome)?
                    .after,
            )?,
            Original::Activate(v) => present(
                v.audit
                    .publication
                    .as_ref()
                    .ok_or(Error::UnknownStoreOutcome)?
                    .after,
            )?,
            Original::KeyedBegin(v) => v.revision,
            Original::ReserveInitial(v) | Original::BeginInitial(v) => v.receipt.revision,
            Original::AssistantMessage(v) => v.revision,
            Original::Lifecycle(v) => v.revision,
            Original::Conclude(v) => v.attempt.receipt.revision,
        };
        Ok(())
    }
}
fn present(state: RevisionState) -> Result<SessionRevision, PreparationCommitError> {
    match state {
        RevisionState::Present(v) => Ok(v),
        RevisionState::Absent(_) => Err(Error::UnknownStoreOutcome.into()),
    }
}

pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) async fn run(
    retained: &Arc<RetainedInitial>,
) -> Result<core::TaskOutput<()>, super::super::super::PendingSuccessorError> {
    let birth = retained
        .lock()
        .enrolled
        .clone()
        .ok_or(Error::InvalidTransition)?;
    // Revision before Register is never used for a mutation. Register's real
    // publication supplies the first CAS value, including all concurrent writes.
    let mut run = Run {
        retained,
        birth,
        revision: SessionRevision::INITIAL,
        turn: TurnId::new(),
        started: std::time::Instant::now(),
        effect_entered: false,
    };
    run.register_and_activate().await?;
    run.keyed_begin().await?;
    let (begun, mut proof) = run.begin().await?;
    if retained.stopped() {
        run.conclude(&begun, crabber_core::RunStatus::Interrupted)
            .await?;
        return Ok(core::TaskOutput::Stopped);
    }
    let outcome = run.turn(&mut proof).await;
    let status = match outcome {
        Ok(()) if !retained.stopped() => crabber_core::RunStatus::Completed,
        Ok(()) | Err(turn::TurnFailure::Stopped) => crabber_core::RunStatus::Interrupted,
        Err(turn::TurnFailure::Model(super::resources::ModelFailure::Semantic)) => {
            crabber_core::RunStatus::Failed
        }
        Err(turn::TurnFailure::Model(super::resources::ModelFailure::Observation)) => {
            return Ok(core::TaskOutput::BusinessFailed(
                Error::UnknownStoreOutcome.into(),
            ));
        }
        Err(turn::TurnFailure::Native(error)) => {
            return if run.effect_entered {
                Ok(core::TaskOutput::BusinessFailed(error.into()))
            } else {
                Err(error.into())
            };
        }
    };
    match run.conclude(&begun, status).await {
        Ok(()) if run.effect_entered => Ok(core::TaskOutput::Business(())),
        Ok(()) => Ok(core::TaskOutput::Stopped),
        Err(error) if run.effect_entered => Ok(core::TaskOutput::BusinessFailed(error.into())),
        Err(error) => Err(error.into()),
    }
}
