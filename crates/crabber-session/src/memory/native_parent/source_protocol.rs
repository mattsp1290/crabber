//! Protected source membership is independent of mutable issuance projections.
use crate::memory::State;
use crate::native_parent::source_protocol::{JournalHead, JournalSource};
use crate::native_parent::{Error, RunIssuance};

/// Immutable Session identity era is installed with the actual first row.
pub(in crate::memory) fn session_birth(
    state: &mut State,
    session: &crabber_core::SessionId,
) -> Result<(), crate::session_owner::SessionOwnerError> {
    if state.session_owners.record_native_session_birth(session)? {
        state.native_session_protocols.insert(session.clone());
    }
    Ok(())
}

/// A protected Run cannot fall back to legacy behavior when its mutable latest
/// projection disappears, including writes that would emit no new issuance.
pub(super) fn verify_previous(
    state: &State,
    previous: &crate::native_parent::RunState,
    latest: Option<&RunIssuance>,
) -> Result<(), Error> {
    crate::native_parent::source_protocol::validate_scope(&previous.session_id, &previous.run_id)?;
    let scope = (previous.session_id.clone(), previous.run_id.clone());
    if !state.native_run_protocols.contains(&scope) {
        if state
            .native_session_protocols
            .contains(&previous.session_id)
        {
            return Err(Error);
        }
        return if state
            .native_run_source_heads
            .keys()
            .any(|(_, id)| id == &previous.run_id)
            || state
                .native_run_sources
                .keys()
                .any(|(_, id, _)| id == &previous.run_id)
        {
            Err(Error)
        } else {
            Ok(())
        };
    }
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
    history(state, &previous.session_id, &previous.run_id, &mut proof).map_err(|_| Error)?;
    let head = state.native_run_source_heads.get(&scope).ok_or(Error)?;
    head.validate()?;
    let latest = latest.ok_or(Error)?;
    if head.session_id != previous.session_id
        || head.run_id != previous.run_id
        || head.sequence != latest.sequence
        || latest.state != *previous
    {
        return Err(Error);
    }
    let selected = state
        .native_run_sources
        .get(&(scope.0.clone(), scope.1.clone(), head.sequence))
        .ok_or(Error)?;
    selected.verify(latest)?;
    let original = state
        .native_run_sources
        .get(&(scope.0, scope.1, 1))
        .ok_or(Error)?;
    original.validate()?;
    if selected.chain_digest != head.chain_digest
        || original.chain_digest != head.origin_digest
        || original.session_id != previous.session_id
        || original.run_id != previous.run_id
        || original.sequence != 1
    {
        return Err(Error);
    }
    Ok(())
}

/// All validation precedes the first mutation; publishing the prepared entries
/// cannot fail halfway through updating the protected maps.
pub(super) fn append(state: &mut State, issuer: &RunIssuance, creation: bool) -> Result<(), Error> {
    if crate::snapshot::encoded_bytes(issuer, 2048).is_none() {
        return Err(Error);
    }
    issuer.validate()?;
    let scope = (issuer.state.session_id.clone(), issuer.state.run_id.clone());
    let key = (scope.0.clone(), scope.1.clone(), issuer.sequence);
    let protocol = state.native_run_protocols.contains(&scope);
    if state
        .native_run_protocols
        .iter()
        .any(|(session, run)| run == &scope.1 && session != &scope.0)
        || state
            .native_run_source_heads
            .keys()
            .any(|(session, run)| run == &scope.1 && session != &scope.0)
        || state
            .native_run_sources
            .keys()
            .any(|(session, run, _)| run == &scope.1 && session != &scope.0)
    {
        return Err(Error);
    }
    if creation {
        if protocol
            || state.native_run_source_heads.contains_key(&scope)
            || state
                .native_run_sources
                .keys()
                .any(|(_, run, _)| run == &scope.1)
        {
            return Err(Error);
        }
    } else if !protocol {
        // Retained legacy state is usable but never promoted by a later write.
        return if state.native_run_source_heads.contains_key(&scope)
            || state
                .native_run_sources
                .keys()
                .any(|(_, run, _)| run == &scope.1)
        {
            Err(Error)
        } else {
            Ok(())
        };
    }
    if state.native_run_sources.contains_key(&key) {
        return Err(Error);
    }
    let previous = state.native_run_source_heads.get(&scope);
    if creation != previous.is_none() {
        return Err(Error);
    }
    if let Some(head) = previous {
        head.validate()?;
        let previous_key = (scope.0.clone(), scope.1.clone(), head.sequence);
        let source = state.native_run_sources.get(&previous_key).ok_or(Error)?;
        let original = state.native_run_issuances.get(&previous_key).ok_or(Error)?;
        source.verify(original)?;
        if source.chain_digest != head.chain_digest {
            return Err(Error);
        }
    }
    let source = JournalSource::new(previous, issuer)?;
    let head = JournalHead::advance(previous, &source)?;
    state.native_run_protocols.insert(scope.clone());
    state.native_run_sources.insert(key, source);
    state.native_run_source_heads.insert(scope, head);
    Ok(())
}

#[path = "source_protocol/original.rs"]
mod original;
#[path = "source_protocol/read.rs"]
mod read;
pub(in crate::memory) use read::{current, history, retained_current, routes};
#[cfg(test)]
#[path = "source_protocol/tests.rs"]
mod tests;
pub(in crate::memory) use original::read as original;

#[cfg(test)]
#[path = "source_protocol/begin_tests.rs"]
mod begin_tests;
