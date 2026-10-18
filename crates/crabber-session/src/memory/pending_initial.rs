//! Shared negative routing, source protection, and tombstone-only removal.
use super::State;
use crate::native_enrollment::pending_initial::SourceKey;
use crate::public_session::v2::proof::{Failure, Proof};
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{JoinedPreparation, PreparationCommitError},
};
use crabber_core::SessionId;
fn failure(error: &Failure) -> PreparationCommitError {
    match error {
        Failure::Limited(_) => Error::Capacity.into(),
        Failure::Error(_) => Error::UnknownStoreOutcome.into(),
    }
}
pub(super) fn routes(
    state: &State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    let present = state.pending_initial.routes(session, proof)?;
    // Independent immutable originals retain the exact winning epoch even if
    // every current projection is coherently replaced or removed.
    for (key, original) in state
        .pending_initial_protocols
        .iter()
        .filter(|(key, slot)| &key.session_id == session || &slot.key.session_id == session)
    {
        proof.measure(&(key, original))?;
        if key != &original.key
            || !state.pending_initial.source_claimed(key, proof)?
            || state.pending_initial.slot(key) != Some(original)
        {
            return Err(crate::public_session::v2::proof::unknown(
                "changed Pending original source slot",
            ));
        }
    }
    for key in state
        .pending_initial
        .keys()
        .filter(|key| &key.session_id == session)
    {
        proof.measure(key)?;
        if !state.pending_initial_protocols.contains_key(key) {
            return Err(crate::public_session::v2::proof::unknown(
                "missing Pending original cohort",
            ));
        }
    }
    Ok(present)
}
pub(super) fn raw_preparation(
    state: &State,
    original: &JoinedPreparation,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    if state.pending_initial_protocols.is_empty() && state.pending_initial.is_empty() {
        return Ok(());
    }
    if !matches!(
        original.observed_operation(),
        crate::session_owner::preparation::PreparedOperation::Admit(_)
            | crate::session_owner::preparation::PreparedOperation::AdmitKeyed(_)
    ) {
        return Ok(());
    }
    let key = SourceKey::expected(original, proof)?;
    routes(state, &key.session_id, proof).map_err(|error| failure(&error))?;
    if state
        .pending_initial
        .source_claimed(&key, proof)
        .map_err(|error| failure(&error))?
    {
        return Err(Error::Unsupported.into());
    }
    Ok(())
}
pub(super) fn contains_scope(state: &State, session: &SessionId) -> bool {
    state.pending_initial.contains_scope(session)
        || state
            .pending_initial_protocols
            .iter()
            .any(|(key, slot)| &key.session_id == session || &slot.key.session_id == session)
}
pub(super) fn purge(
    state: &mut State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(), Failure> {
    // An unrelated legacy scope has no Pending rows to erase.
    if !contains_scope(state, session) {
        routes(state, session, proof)?;
        return Ok(());
    }
    // Physical removal is legal only under the actual same-session tombstone.
    let tombstone = state.tombstones.get(session).ok_or_else(|| {
        crate::public_session::v2::proof::unknown("Pending purge requires tombstone")
    })?;
    proof.measure(tombstone)?;
    tombstone
        .verify(session)
        .map_err(crate::public_session::v2::proof::unknown)?;
    routes(state, session, proof)?;
    state.pending_initial.purge(session, proof)?;
    state
        .pending_initial_protocols
        .retain(|key, _| &key.session_id != session);
    Ok(())
}

native_pending_initial_items! {
    #[cfg(test)]
    use super::MemoryStore;
    #[path = "pending_initial/native.rs"]
    mod native;
    #[cfg(test)]
    use crate::session_owner::owned_scope::NativeInitialOperation;
#[cfg(test)]
#[path = "pending_initial/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "pending_initial/budget_tests.rs"]
mod budget_tests;

#[cfg(test)]
#[path = "pending_initial/pre_run_tests.rs"]
mod pre_run_tests;
}
