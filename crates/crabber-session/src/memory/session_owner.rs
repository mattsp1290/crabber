//! Session owner controls under the ordinary Memory mutation gate.
use super::{MemoryStore, State, bump_public_revision};
use crate::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerAuthority, SessionOwnerError as Error,
    SessionOwnerOutcome, SessionOwnerParentAuthority,
};
use crate::{AttemptCleanup, AttemptState, PhaseState};
use crabber_core::RunStatus;
use time::OffsetDateTime;
impl MemoryStore {
    pub(super) fn reserve_owner(
        &self,
        request: &ReserveSessionOwner,
        parent: Option<&SessionOwnerParentAuthority>,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let fault = fault(self);
        let outcome = self.transact(|state| {
            if let Some(saved) = state
                .session_owners
                .receipt(&request.binding.session_id, &request.id)?
            {
                if saved.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(SessionOwnerOutcome::Replayed(saved));
            }
            if let Some(session) = state.sessions.get(&request.binding.session_id) {
                session
                    .ensure_identity(&request.binding.workspace_id, &request.binding.directory)
                    .map_err(|_| Error::IdentityMismatch)?;
                if request.binding.parent.is_none()
                    && state
                        .management
                        .get(&session.id)
                        .is_some_and(|metadata| metadata.archived)
                {
                    return Err(Error::InvalidTransition);
                }
            }
            let now = self.clock.now();
            let parent = parent_proof(state, request, parent, now)?;
            let outcome = state
                .session_owners
                .reserve_authenticated(request, parent, now)?;
            if state.sessions.contains_key(&request.binding.session_id) {
                bump_public_revision(state, &request.binding.session_id)
                    .map_err(|_| Error::UnknownStoreOutcome)?;
            }
            before(fault)?;
            Ok(outcome)
        })?;
        after(outcome, fault)
    }
    pub(super) fn mutate_owner(
        &self,
        request: &MutateSessionOwner,
        authority: Option<&SessionOwnerAuthority>,
    ) -> Result<SessionOwnerOutcome, Error> {
        request.fingerprint()?;
        let fault = fault(self);
        let outcome = self.transact(|state| {
            let (outcome, public_change) =
                state
                    .session_owners
                    .mutate(request, authority, self.clock.now())?;
            if matches!(outcome, SessionOwnerOutcome::Applied { .. }) {
                if public_change && state.sessions.contains_key(&request.session_id) {
                    bump_public_revision(state, &request.session_id)
                        .map_err(|_| Error::UnknownStoreOutcome)?;
                }
                before(fault)?;
            }
            Ok(outcome)
        })?;
        after(outcome, fault)
    }
}
fn parent_proof(
    state: &State,
    request: &ReserveSessionOwner,
    authority: Option<&SessionOwnerParentAuthority>,
    now: OffsetDateTime,
) -> Result<Option<crate::native_parent::Witness>, Error> {
    let Some(parent) = &request.binding.parent else {
        return if authority.is_none() {
            Ok(None)
        } else {
            Err(Error::InvalidRequest)
        };
    };
    let run = state
        .runs
        .get(&parent.run_id)
        .filter(|run| run.session_id == request.binding.session_id)
        .ok_or(Error::NotFound)?;
    if run.id != parent.run_id {
        return Err(Error::UnknownStoreOutcome);
    }
    if run.status != RunStatus::Running || run.lease_until <= now {
        return Err(Error::StaleOwner);
    }
    let mut phase_witness = None;
    let attempt = match (authority, &parent.phase, &parent.call_id) {
        (Some(SessionOwnerParentAuthority::Attempt(authority)), None, None) => {
            let attempt = state
                .attempts
                .values()
                .find(|attempt| {
                    authority.matches_store(&attempt.snapshot, &attempt.token, &attempt.run_token)
                })
                .ok_or(Error::StaleOwner)?;
            if !authority.matches_store(&attempt.snapshot, &attempt.token, &attempt.run_token) {
                return Err(Error::StaleOwner);
            }
            attempt
        }
        (Some(SessionOwnerParentAuthority::Phase(authority)), Some(target), Some(call_id)) => {
            let phase = state.phases.get(&target.id).ok_or(Error::StaleOwner)?;
            phase.validate().map_err(|_| Error::UnknownStoreOutcome)?;
            if phase.snapshot.target != *target
                || phase.snapshot.session_id != run.session_id
                || phase.snapshot.run_id != run.id
                || phase.snapshot.state != PhaseState::Started
                || phase.snapshot.quiescence != crate::PhaseQuiescence::Pending
                || target.call_id != *call_id
                || !authority.matches_store(&phase.snapshot, &phase.token)
            {
                return Err(Error::StaleOwner);
            }
            let attempt = state
                .attempts
                .get(&target.attempt.id)
                .ok_or(Error::StaleOwner)?;
            if !authority.matches_attempt_store(
                &attempt.snapshot,
                &attempt.token,
                &attempt.run_token,
            ) {
                return Err(Error::StaleOwner);
            }
            let call = state
                .calls
                .get(call_id)
                .filter(|call| call.run_id == run.id)
                .ok_or(Error::NotFound)?;
            if call.status != crabber_core::ToolCallStatus::Running
                || call.result.is_some()
                || crate::phase::call_binding(call).map_err(|_| Error::UnknownStoreOutcome)?
                    != phase.snapshot.call_binding
            {
                return Err(Error::UnknownStoreOutcome);
            }
            phase_witness = Some(
                super::native_parent::verify_phase(state, run, attempt, phase)
                    .map_err(|_| Error::UnknownStoreOutcome)?,
            );
            attempt
        }
        _ => return Err(Error::StaleOwner),
    };
    attempt.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    if attempt.snapshot.session_id != run.session_id
        || attempt.snapshot.run_id != run.id
        || attempt.snapshot.state != AttemptState::Started
        || attempt.snapshot.cleanup != AttemptCleanup::Pending
        || attempt.run_token != run.claim_token
        || attempt.lease_until <= now
    {
        return Err(Error::StaleOwner);
    }
    let witness = if let Some(witness) = phase_witness {
        witness
    } else {
        super::native_parent::verify_attempt(state, run, attempt)
            .map_err(|_| Error::UnknownStoreOutcome)?
    };
    Ok(Some(witness))
}
fn fault(store: &MemoryStore) -> u8 {
    #[cfg(test)]
    {
        store
            .session_owner_fault
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    #[cfg(not(test))]
    {
        let _ = store;
        0
    }
}
fn before(fault: u8) -> Result<(), Error> {
    if fault == 1 {
        Err(Error::UnknownStoreOutcome)
    } else {
        Ok(())
    }
}
fn after(outcome: SessionOwnerOutcome, fault: u8) -> Result<SessionOwnerOutcome, Error> {
    if fault == 2 && matches!(outcome, SessionOwnerOutcome::Applied { .. }) {
        Err(Error::UnknownStoreOutcome)
    } else {
        Ok(outcome)
    }
}

#[cfg(test)]
#[path = "session_owner_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "session_owner_parent_tests.rs"]
mod parent_tests;
