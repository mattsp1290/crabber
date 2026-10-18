//! Fresh acknowledgements are the only route to local dispatch or model Begin.
use super::{
    Arc, ConsumerInput, ConsumerOutput, Error, FreshBegun, PreparationCommitError, Run, Step,
};
use crate::native_enrollment::initial_consumer::CallKind;
use crate::session_owner::{
    driver::{ExecutionDriverOutcome, RegisterExecutionDriver},
    owned_scope::{OwnedDispatchPermit, OwnedScopeActivation},
};
use crate::{AttemptCommand, AttemptMutation, AttemptMutationOutcome, AttemptState, AttemptTarget};
use crabber_core::{AttemptGeneration, AttemptId, ControlId};

impl Run<'_> {
    pub(super) async fn register_and_activate(&mut self) -> Result<(), PreparationCommitError> {
        let mut step = Step::new(self.retained, CallKind::Register)?;
        let context = &self.retained.source.context;
        step.proof
            .measure(&(
                context.session_id(),
                context.workspace_id(),
                context.directory(),
                self.fence()?,
            ))
            .map_err(|_| Error::Capacity)?;
        let request = RegisterExecutionDriver {
            context: context.clone(),
            fence: self.fence()?.clone(),
        };
        let (output, record, _) = step.invoke(&ConsumerInput::Register(&request)).await?;
        self.accepted(&record)?;
        let ConsumerOutput::Registered(outcome) = output else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        let ExecutionDriverOutcome::Applied { grant, .. } = *outcome else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        let grant: Arc<crate::session_owner::driver::ExecutionDriverGrant> = Arc::from(grant);
        *self
            .retained
            .calls
            .driver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&grant));
        let mut step = Step::new(self.retained, CallKind::Activate)?;
        step.proof
            .measure(grant.receipt())
            .map_err(|_| Error::Capacity)?;
        let request = OwnedScopeActivation::new(grant.scope_source()?)?;
        let (output, record, mut proof) = step
            .invoke(&ConsumerInput::Activate {
                request: &request,
                grant: &grant,
            })
            .await?;
        self.accepted(&record)?;
        let ConsumerOutput::Activated(outcome) = output else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        if !matches!(
            *outcome,
            crate::session_owner::SessionOwnerOutcome::Applied {
                authority: None,
                ..
            }
        ) {
            return Err(Error::UnknownStoreOutcome.into());
        }
        // This seam runs only after the actual original native future returned,
        // was explicitly disposed, and its full ACK passed this call's Proof.
        proof.measure(&request).map_err(|_| Error::Capacity)?;
        let permit = OwnedDispatchPermit::applied(request.clone());
        let control = self
            .retained
            .lock()
            .control
            .clone()
            .ok_or(Error::InvalidTransition)?;
        if !control.dispatch_consumer(permit, &request)? {
            return Err(Error::CancellationPending.into());
        }
        Ok(())
    }
    pub(super) async fn keyed_begin(&mut self) -> Result<(), PreparationCommitError> {
        if !matches!(
            &self.birth.completion,
            crate::session_owner::preparation::PreparationCompletion::Keyed(_)
        ) {
            return Ok(());
        }
        let step = Step::new(self.retained, CallKind::KeyedBegin)?;
        let (output, record, _) = step
            .invoke(&ConsumerInput::KeyedBegin {
                fence: self.fence()?,
            })
            .await?;
        if !matches!(output, ConsumerOutput::KeyedBegan { .. }) {
            return Err(Error::UnknownStoreOutcome.into());
        }
        self.accepted(&record)
    }
    pub(super) async fn begin(
        &mut self,
    ) -> Result<(Arc<FreshBegun>, super::super::proof::OperationProof), PreparationCommitError>
    {
        let step = Step::new(self.retained, CallKind::ReserveInitial)?;
        let request = AttemptMutation {
            id: ControlId::new(),
            expected_revision: self.revision,
            command: AttemptCommand::ReserveBound {
                target: AttemptTarget {
                    id: AttemptId::new(),
                    generation: AttemptGeneration::INITIAL,
                },
            },
        };
        let (output, record, _) = step
            .invoke(&ConsumerInput::ReserveInitial(&request))
            .await?;
        self.accepted(&record)?;
        let ConsumerOutput::Attempt(outcome) = output else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        let AttemptMutationOutcome::Applied {
            authority: Some(authority),
            receipt,
        } = *outcome
        else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        let authority = Arc::new(authority);
        *self
            .retained
            .calls
            .authority
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&authority));
        let mut step = Step::new(self.retained, CallKind::BeginInitial)?;
        step.proof
            .measure(&receipt.attempt.target)
            .map_err(|_| Error::Capacity)?;
        let request = AttemptMutation {
            id: ControlId::new(),
            expected_revision: self.revision,
            command: AttemptCommand::Begin {
                target: receipt.attempt.target.clone(),
            },
        };
        let (output, record, proof) = step
            .invoke(&ConsumerInput::BeginInitial {
                request: &request,
                authority: &authority,
            })
            .await?;
        self.accepted(&record)?;
        let ConsumerOutput::Attempt(outcome) = output else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        let AttemptMutationOutcome::Applied {
            receipt,
            authority: None,
        } = *outcome
        else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        if receipt.attempt.state != AttemptState::Started {
            return Err(Error::UnknownStoreOutcome.into());
        }
        let begun = Arc::new(FreshBegun { authority, receipt });
        *self
            .retained
            .calls
            .begun
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&begun));
        Ok((begun, proof))
    }
}
