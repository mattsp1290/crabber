//! Queue fresh eligibility preserves the actual publication/current-R fence.
use super::{Error, State};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::{PreparationKey, PreparationPurpose, queue_admission};
pub(super) fn revision(
    state: &State,
    original: &queue_admission::AdmissionSources<'_>,
    activation: Option<crate::session_owner::audit::RevisionState>,
    proof: &mut Proof,
) -> Result<(), Error> {
    let origin = original
        .audits
        .reserve
        .record
        .preparation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    if !matches!(origin.purpose, PreparationPurpose::Queue { .. }) {
        return Ok(());
    }
    let PreparationKey::Queue(id) = &origin.key else {
        return Err(Error::UnknownStoreOutcome);
    };
    let prefix =
        crate::memory::queue::protocol::admission_prefix(state, &original.session.id, id, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?
            .ok_or(Error::UnknownStoreOutcome)?;
    let revision =
        queue_admission::publication_revision(prefix, original).map_err(|error| match error {
            crate::session_owner::preparation::PreparationCommitError::Owner(error) => error,
            _ => Error::UnknownStoreOutcome,
        })?;
    let expected = activation.unwrap_or(crate::session_owner::audit::RevisionState::Present(
        revision,
    ));
    if super::revision(state, &original.session.id) != expected {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}
