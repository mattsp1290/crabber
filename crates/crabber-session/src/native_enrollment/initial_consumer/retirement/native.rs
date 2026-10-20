//! The sole native mutation loan for an actually joined unactivated Driver.
use super::{Correlation, Record, RegisterOriginal, unactivated_request};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerError as Error,
    owned_scope::{InitialRetirementView, InitialRetirementWitness},
    preparation::{JoinedPreparation, PreparationCommitError},
};
use crabber_core::SessionContext;

pub(crate) struct VerifiedUnactivatedRetirement<'a> {
    register: &'a RegisterOriginal,
    request: MutateSessionOwner,
    _witness: &'a InitialRetirementWitness<'a>,
}
pub(crate) fn verify_unactivated_retirement<'a>(
    witness: &'a InitialRetirementWitness<'a>,
    source: (&JoinedPreparation, &SessionContext, &Correlation),
    register: &'a RegisterOriginal,
    current: &Record,
    proof: &mut Proof,
) -> Result<VerifiedUnactivatedRetirement<'a>, PreparationCommitError> {
    let (original, context, expected) = source;
    witness.verify_register(register, proof)?;
    let _local: InitialRetirementView<'_> = witness.verify(original, context, expected, proof)?;
    proof
        .measure(&(register, current))
        .map_err(|_| Error::Capacity)?;
    register.reserve.validate()?;
    register.begin.validate()?;
    crate::session_owner::audit::verify_current(
        &register.begin.record,
        &register.reserve,
        &register.begin,
    )?;
    if register.request.session != expected.key.session_id
        || register.receipt.session_id != expected.key.session_id
        || current != &register.begin.record
        || current.snapshot.state != crate::session_owner::SessionOwnerState::Active
        || current.owned_scope.is_some()
        || current.owned_join.is_some()
        || current.cancellation.is_some()
        || current.resolver.is_some()
        || current.retained_resources.is_some()
    {
        return Err(Error::StaleOwner.into());
    }
    let request = unactivated_request(expected, register, proof)
        .map_err(|error| super::super::failure(&error))?;
    Ok(VerifiedUnactivatedRetirement {
        register,
        request,
        _witness: witness,
    })
}
impl VerifiedUnactivatedRetirement<'_> {
    pub(crate) fn request(&self) -> &MutateSessionOwner {
        &self.request
    }
    /// Consume the original private witness only while mutating the same exact
    /// unactivated original. No authority or activation is reconstructed.
    pub(crate) fn into_request(self, current: &Record) -> Result<MutateSessionOwner, Error> {
        current.validate()?;
        if current != &self.register.begin.record
            || current.owned_scope.is_some()
            || current.owned_join.is_some()
            || current.snapshot.state != crate::session_owner::SessionOwnerState::Active
            || self.request.session_id != current.snapshot.binding.session_id
            || self.request.target != current.snapshot.target
        {
            return Err(Error::StaleOwner);
        }
        Ok(self.request)
    }
}
