//! The actual rotating issuer is captured before the later status publication.
use crate::session_owner::preparation::proof::VerifiedPreparation;
use crate::{ApprovalError as Error, native_parent::CompletionSource};
use crabber_core::Run;
pub(super) fn save(
    state: &mut crate::memory::State,
    old: &Run,
    next: &Run,
    completion: Option<&CompletionSource>,
    preparation: Option<&VerifiedPreparation<'_>>,
) -> Result<(), Error> {
    let captured =
        crate::memory::native_parent::record_run_completed_captured(state, old, next, completion)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    if let Some(preparation) = preparation {
        preparation
            .record_continuation_claim(captured)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    Ok(())
}
