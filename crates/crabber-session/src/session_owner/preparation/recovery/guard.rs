//! Authenticate original recovery intent and exact own preparation CAS.
use super::super::{
    PreparationCommitError, PreparationKey, PreparationSource, proof, source_validation,
};
use super::AdmissionRecoveryPreparation;
use crabber_core::SessionRevision;
pub(crate) fn completion_source(
    source: &AdmissionRecoveryPreparation,
    proof: &proof::VerifiedPreparation<'_>,
    actual: SessionRevision,
) -> Result<crate::native_parent::CompletionSource, PreparationCommitError> {
    source_validation::validate(
        &PreparationSource::AdmissionRecovery(source.clone()),
        proof.grant.original_reservation(),
    )?;
    let request = source.request();
    let key = PreparationKey::AdmissionRecovery {
        key: request.key.clone(),
        issuance: source.expected_issuance(),
    };
    if actual
        != proof.control_revision(
            &key,
            &request.session_id,
            &request.expected_fence.run_id,
            source.expected_revision(),
        )?
    {
        return Err(crate::session_owner::SessionOwnerError::RevisionConflict.into());
    }
    Ok(crate::native_parent::CompletionSource::from_preparation(
        proof,
        crate::native_parent::CompletionOperation::AdmissionRecovery,
        source.fingerprint()?,
    )?)
}
