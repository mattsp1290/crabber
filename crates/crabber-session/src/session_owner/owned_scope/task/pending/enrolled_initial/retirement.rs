//! Same-original local cleanup observation, available only on joined Seal calls.
use super::{
    Error, InitialResourceAssociation, OwnedMainDisposition, PreparationCommitError, Proof,
    RetainedInitial,
};
use crate::native_enrollment::pending_initial::Correlation;
use crate::session_owner::{
    driver::ExecutionDriverGrant,
    owned_scope::{JoinedOwnedScope, OwnedScopeActivation},
    preparation::{JoinedPreparation, PreparationCompletion},
};
use crabber_core::{ControlId, SessionContext};

/// Borrowed physical observations from the existing original-task retainer.
/// No native record, receipt or caller-supplied disposition constructs this.
pub(crate) struct InitialRetirementWitness<'a> {
    retained: &'a RetainedInitial,
    disposition: OwnedMainDisposition,
    association: &'a InitialResourceAssociation<'a>,
    expected: &'a Correlation,
}

/// Genuine retained originals. Native source authentication remains mandatory.
pub(crate) struct InitialRetirementView<'a> {
    pub(crate) calls: &'a [ControlId],
    pub(crate) disposition: OwnedMainDisposition,
    pub(crate) activation: Option<&'a OwnedScopeActivation>,
    pub(crate) grant: Option<&'a ExecutionDriverGrant>,
}
impl<'a> InitialRetirementWitness<'a> {
    pub(super) fn new(
        retained: &'a RetainedInitial,
        disposition: OwnedMainDisposition,
        association: &'a InitialResourceAssociation<'a>,
        expected: &'a Correlation,
    ) -> Self {
        Self {
            retained,
            disposition,
            association,
            expected,
        }
    }

    pub(crate) fn verify(
        &self,
        original: &JoinedPreparation,
        context: &SessionContext,
        expected: &Correlation,
        proof: &mut Proof,
    ) -> Result<InitialRetirementView<'_>, PreparationCommitError> {
        {
            let state = self.retained.lock();
            if !state.joined
                || state.identity.as_deref() != Some(&expected.identity)
                || self.expected != expected
                || state
                    .ingress
                    .as_ref()
                    .is_none_or(|ingress| !ingress.is_closed())
            {
                return Err(Error::InvalidTransition.into());
            }
        }
        let calls = self
            .association
            .verify_close(original, context, expected, proof)?;
        proof
            .measure(&self.disposition)
            .map_err(|_| Error::Capacity)?;
        let activation = self.retained.calls.activation.get();
        if let Some(activation) = activation {
            proof.measure(activation).map_err(|_| Error::Capacity)?;
        }
        let grant = self.retained.calls.driver.get().map(std::sync::Arc::as_ref);
        if let Some(grant) = grant {
            let request = grant.original_request();
            proof
                .measure(&(
                    grant.receipt(),
                    grant.source_identity(),
                    request.context.session_id(),
                    request.context.workspace_id(),
                    request.context.directory(),
                    &request.fence,
                ))
                .map_err(|_| Error::Capacity)?;
            crate::native_enrollment::caller::measure_owner_authority(grant.authority(), proof)
                .map_err(|_| Error::Capacity)?;
            if activation
                .is_some_and(|activation| grant.scope_source().as_ref() != Ok(&activation.source))
            {
                return Err(Error::IdentityMismatch.into());
            }
        } else if activation.is_some() {
            return Err(Error::InvalidTransition.into());
        }
        Ok(InitialRetirementView {
            calls,
            disposition: self.disposition,
            activation,
            grant,
        })
    }

    /// Tie the native Register original to the actual completion retained by
    /// this epoch, including its private fence. Native storage independently
    /// authenticates that assignment and all original Register publications.
    pub(crate) fn verify_register(
        &self,
        register: &crate::native_enrollment::initial_consumer::RegisterOriginal,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        self.verify(
            &self.retained.source.original,
            &self.retained.source.context,
            self.expected,
            proof,
        )?;
        let state = self.retained.lock();
        let birth = state.enrolled.as_deref().ok_or(Error::InvalidTransition)?;
        let fence = match &birth.completion {
            PreparationCompletion::Admitted(value) => &value.fence,
            PreparationCompletion::Keyed(crate::KeyedAdmitOutcome::Started {
                admitted, ..
            }) => &admitted.fence,
            _ => return Err(Error::InvalidTransition.into()),
        };
        let context = &self.retained.source.context;
        proof
            .measure(&(
                register,
                &birth.assignment,
                fence,
                context.session_id(),
                context.workspace_id(),
                context.directory(),
            ))
            .map_err(|_| Error::Capacity)?;
        let expected_digest = crate::session_owner::digest(&(
            "crabber.execution-driver.initial.request.v1",
            context.session_id(),
            context.workspace_id(),
            context.directory(),
            &fence.run_id,
            &fence.claim_token,
        ))?;
        if birth.correlation() != self.expected
            || birth.assignment.run_id != fence.run_id
            || register.request.session != *context.session_id()
            || register.request.workspace != context.workspace_id()
            || register.request.directory != context.directory()
            || register.request.fence != *fence
            || register.receipt.run_id != fence.run_id
            || register.receipt.request_digest != expected_digest
            || register
                .request
                .fingerprint()
                .map_err(|error| crate::native_enrollment::initial_consumer::failure(&error))?
                != expected_digest
        {
            return Err(Error::IdentityMismatch.into());
        }
        Ok(())
    }

    pub(crate) fn joined_scope(
        &self,
        activation: &OwnedScopeActivation,
        proof: &mut Proof,
    ) -> Result<JoinedOwnedScope, PreparationCommitError> {
        JoinedOwnedScope::from_initial_retirement(self, activation, proof)
    }

    pub(crate) fn verified_activation(
        &self,
        activation: &OwnedScopeActivation,
        proof: &mut Proof,
    ) -> Result<(&OwnedScopeActivation, OwnedMainDisposition), PreparationCommitError> {
        let view = self.verify(
            &self.retained.source.original,
            &self.retained.source.context,
            self.expected,
            proof,
        )?;
        let original = view.activation.ok_or(Error::InvalidTransition)?;
        if original != activation {
            return Err(Error::IdentityMismatch.into());
        }
        Ok((original, view.disposition))
    }
}
