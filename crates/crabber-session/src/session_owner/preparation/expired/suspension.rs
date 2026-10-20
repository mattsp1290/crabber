use super::Error;
use crabber_core::InputFingerprint;
pub(crate) fn suspension_digest(
    proof: &crate::run_control::SuspensionProof,
    run: &crabber_core::Run,
) -> Result<InputFingerprint, Error> {
    crate::session_owner::digest(&(
        "crabber.preparation.expired-suspension.v1",
        proof
            .conclusion
            .fingerprint(&run.session_id, &run.id)
            .map_err(|_| Error::UnknownStoreOutcome)?,
        &proof.receipt,
    ))
}
