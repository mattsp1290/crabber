//! Capture the exact Tool/Frontend rotating issuer under the caller's proof.
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::proof::VerifiedPreparation;
use crate::{ToolContinuationError as Error, native_parent::CompletionSource};
use crabber_core::Run;
pub(super) fn save(
    state: &mut crate::memory::State,
    old: &Run,
    next: &Run,
    completion: Option<&CompletionSource>,
    preparation: Option<&VerifiedPreparation<'_>>,
    proof: &mut Proof,
) -> Result<crate::native_enrollment::caller::initial::RecoveryPublication, Error> {
    let captured = crate::memory::native_parent::record_run_completed_publication_with_proof(
        state,
        old,
        next,
        completion,
        proof,
        crate::native_enrollment::caller::initial::Capture::Original,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    if let Some(preparation) = preparation {
        preparation
            .record_continuation_claim(captured.sequence)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    captured.updated.ok_or(Error::UnknownStoreOutcome)
}
