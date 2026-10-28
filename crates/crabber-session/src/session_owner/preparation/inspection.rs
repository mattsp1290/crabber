//! Pure original unkeyed admission inspection, never replacement authority.
use super::{
    JoinedPreparation, PreparationCommitError, PreparationKey, PreparationPurpose,
    PreparedOperation,
};
use crate::public_session::v2::proof::{Proof, ReadError};
use crate::session_owner::SessionOwnerError as Error;

pub(crate) const MAX_PROOF: usize = 32 << 20;
pub(crate) fn original<'a>(
    joined: &'a JoinedPreparation,
    proof: &mut Proof,
) -> Result<&'a crate::BoundAdmitRequest, PreparationCommitError> {
    let grant = joined.observed_grant();
    let reservation = grant.original_reservation();
    let PreparedOperation::Admit(bound) = joined.observed_operation() else {
        return Err(Error::Unsupported.into());
    };
    if reservation.resource_contract.is_none()
        || joined.observed_registration().is_none()
        || !matches!(reservation.purpose, PreparationPurpose::OrdinaryAdmission)
        || !matches!(reservation.key, PreparationKey::Unkeyed(_))
    {
        return Err(Error::Unsupported.into());
    }
    // Stream borrowed carriers before original-intent hashing or any cloning.
    proof
        .measure(&(
            reservation,
            joined.observed_registration(),
            &bound.request,
            &bound.binding,
        ))
        .map_err(|error| {
            proof.admission_failure(&error, PreparationCommitError::Owner(Error::Capacity))
        })?;
    joined.verify_original_intent()?;
    Ok(bound)
}
pub(crate) fn read_error(proof: &Proof, error: ReadError<Error>) -> PreparationCommitError {
    match error {
        ReadError::Domain(error) => error.into(),
        ReadError::Budget(error) => {
            let fallback = match &error {
                crate::public_session::v2::proof::Failure::Limited(_) => Error::Capacity,
                crate::public_session::v2::proof::Failure::Error(_) => Error::UnknownStoreOutcome,
            };
            proof.admission_failure(&error, PreparationCommitError::Owner(fallback))
        }
    }
}

#[cfg(test)]
#[path = "inspection_contract.rs"]
pub(crate) mod contract;
