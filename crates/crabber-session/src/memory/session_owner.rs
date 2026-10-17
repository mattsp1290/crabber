//! Session owner controls under the ordinary Memory mutation gate.
#[path = "session_owner/cancellation.rs"]
mod cancellation;
#[path = "session_owner/drivers.rs"]
mod drivers;
pub(in crate::memory) use drivers::classified as classify_driver_join;
#[path = "session_owner/owned_scope.rs"]
mod owned_scope;
#[path = "session_owner/preparation.rs"]
mod preparation;
#[path = "session_owner/resources.rs"]
mod resources;
#[path = "session_owner/services.rs"]
mod services;
use super::{MemoryStore, State, bump_public_revision};
use crate::session_owner::audit::RevisionState;
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
        self.reserve_owner_inner(request, parent, None)
    }
    pub(super) fn reserve_preparation(
        &self,
        request: &crate::session_owner::preparation::ReservePreparation,
    ) -> Result<SessionOwnerOutcome, Error> {
        request.fingerprint()?;
        self.reserve_owner_inner(&request.owner, None, Some(request))
    }
    fn reserve_owner_inner(
        &self,
        request: &ReserveSessionOwner,
        parent: Option<&SessionOwnerParentAuthority>,
        preparation: Option<&crate::session_owner::preparation::ReservePreparation>,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = preparation.map_or_else(
            || request.fingerprint(),
            super::super::session_owner::preparation::ReservePreparation::fingerprint,
        )?;
        let fault = fault(self);
        let read_proof =
            std::cell::RefCell::new(crate::public_session::v2::proof::Proof::bounded(32 << 20));
        let outcome = self.transact_or_replay(
            |state| {
                if let Some(replayed) = preflight_queue_reservation(
                    state,
                    preparation,
                    &digest,
                    self.clock.now(),
                    &mut read_proof.borrow_mut(),
                )? {
                    return Ok(Some(replayed));
                }
                preflight_expired_reservation(
                    state,
                    request,
                    preparation,
                    &digest,
                    self.clock.now(),
                )
            },
            |state| {
                super::deletion::ensure_live(state, &request.binding.session_id)
                    .map_err(crate::deletion::owner_error)?;
                if let Some(saved) = state
                    .session_owners
                    .receipt(&request.binding.session_id, &request.id)?
                {
                    if saved.semantic_digest != digest {
                        return Err(Error::SemanticConflict);
                    }
                    return Ok(SessionOwnerOutcome::Replayed(saved));
                }
                if request.binding.parent.is_some()
                    && !state.sessions.contains_key(&request.binding.session_id)
                {
                    return Err(Error::UnknownStoreOutcome);
                }
                if let Some(session) = state.sessions.get(&request.binding.session_id) {
                    session
                        .ensure_identity(&request.binding.workspace_id, &request.binding.directory)
                        .map_err(|_| Error::IdentityMismatch)?;
                    if state
                        .management
                        .get(&session.id)
                        .is_some_and(|metadata| metadata.archived)
                        && request.binding.parent.is_none()
                        && !preparation.is_some_and(|value| {
                            matches!(value.purpose,
                        crate::session_owner::preparation::PreparationPurpose::ExistingRun { .. })
                        })
                    {
                        return Err(Error::InvalidTransition);
                    }
                }
                let now = self.clock.now();
                let parent = parent_proof(state, request, parent, now)?;
                let revision = revision(state, &request.binding.session_id);
                if let Some(preparation) = preparation {
                    validate_preparation(
                        state,
                        preparation,
                        revision,
                        now,
                        &mut read_proof.borrow_mut(),
                    )?;
                }
                let outcome = state.session_owners.reserve_with_origin(
                    request,
                    parent,
                    now,
                    revision,
                    preparation.map(super::super::session_owner::preparation::origin),
                )?;
                if state.sessions.contains_key(&request.binding.session_id) {
                    bump_public_revision(state, &request.binding.session_id)
                        .map_err(|_| Error::UnknownStoreOutcome)?;
                }
                before(fault)?;
                Ok(outcome)
            },
        )?;
        after(outcome, fault)
    }
    pub(super) fn mutate_owner(
        &self,
        request: &MutateSessionOwner,
        authority: Option<&SessionOwnerAuthority>,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let fault = fault(self);
        let outcome = self.transact_or_replay(
            |state| preflight_expired_begin(state, request, &digest, self.clock.now()),
            |state| {
                super::deletion::ensure_live(state, &request.session_id)
                    .map_err(crate::deletion::owner_error)?;
                let revision = revision(state, &request.session_id);
                let (outcome, public_change) = state.session_owners.mutate_at_revision(
                    request,
                    authority,
                    self.clock.now(),
                    revision,
                )?;
                if matches!(outcome, SessionOwnerOutcome::Applied { .. }) {
                    if public_change && state.sessions.contains_key(&request.session_id) {
                        bump_public_revision(state, &request.session_id)
                            .map_err(|_| Error::UnknownStoreOutcome)?;
                    }
                    before(fault)?;
                }
                Ok(outcome)
            },
        )?;
        after(outcome, fault)
    }
}
fn preflight_expired_begin(
    state: &State,
    request: &MutateSessionOwner,
    digest: &crabber_core::InputFingerprint,
    now: OffsetDateTime,
) -> Result<Option<SessionOwnerOutcome>, Error> {
    if !matches!(
        request.command,
        crate::session_owner::SessionOwnerCommand::Begin
    ) {
        return Ok(None);
    }
    super::deletion::ensure_live(state, &request.session_id)
        .map_err(crate::deletion::owner_error)?;
    if let Some(saved) = state
        .session_owners
        .receipt(&request.session_id, &request.id)?
    {
        if &saved.semantic_digest != digest {
            return Err(Error::SemanticConflict);
        }
        return Ok(Some(SessionOwnerOutcome::Replayed(saved)));
    }
    let record = state
        .session_owners
        .checked(&request.session_id, &request.target.id)?;
    if let Some((run_id, target)) =
        crate::session_owner::preparation::expired::native::begin_target(record, request)
    {
        let run = state.runs.get(run_id).ok_or(Error::NotFound)?;
        let inspection =
            crate::session_owner::preparation::expired::native::begin_request(run, target)?;
        let actual = super::expired::context(state, inspection, now)?;
        if actual.target() != target {
            return Err(Error::StaleOwner);
        }
    }
    Ok(None)
}
fn preflight_queue_reservation(
    state: &State,
    preparation: Option<&crate::session_owner::preparation::ReservePreparation>,
    digest: &crabber_core::InputFingerprint,
    now: OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<SessionOwnerOutcome>, Error> {
    let Some(preparation) = preparation else {
        return Ok(None);
    };
    if !matches!(
        preparation.purpose,
        crate::session_owner::preparation::PreparationPurpose::Queue { .. }
    ) {
        return Ok(None);
    }
    let request = &preparation.owner;
    super::deletion::ensure_live(state, &request.binding.session_id)
        .map_err(crate::deletion::owner_error)?;
    if let Some(saved) = state
        .session_owners
        .receipt(&request.binding.session_id, &request.id)?
    {
        if &saved.semantic_digest != digest {
            return Err(Error::SemanticConflict);
        }
        return Ok(Some(SessionOwnerOutcome::Replayed(saved)));
    }
    validate_preparation_source_with_proof(state, preparation, now, proof)?;
    Ok(None)
}
fn preflight_expired_reservation(
    state: &State,
    request: &ReserveSessionOwner,
    preparation: Option<&crate::session_owner::preparation::ReservePreparation>,
    digest: &crabber_core::InputFingerprint,
    now: OffsetDateTime,
) -> Result<Option<SessionOwnerOutcome>, Error> {
    if preparation.is_some_and(|request| {
        matches!(
            request.source,
            Some(crate::session_owner::preparation::PreparationSource::ExpiredRun(_))
        )
    }) {
        super::deletion::ensure_live(state, &request.binding.session_id)
            .map_err(crate::deletion::owner_error)?;
        if let Some(saved) = state
            .session_owners
            .receipt(&request.binding.session_id, &request.id)?
        {
            if &saved.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            return Ok(Some(SessionOwnerOutcome::Replayed(saved)));
        }
        validate_preparation(
            state,
            preparation.ok_or(Error::InvalidRequest)?,
            revision(state, &request.binding.session_id),
            now,
            &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
        )?;
    }
    Ok(None)
}
pub(super) fn revision(state: &State, session: &crabber_core::SessionId) -> RevisionState {
    if state.sessions.contains_key(session) {
        RevisionState::Present(
            state
                .public_revisions
                .get(session)
                .copied()
                .unwrap_or(crabber_core::SessionRevision::INITIAL),
        )
    } else {
        RevisionState::Absent(state.session_owners.publication_count(session))
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

fn validate_preparation(
    state: &State,
    request: &crate::session_owner::preparation::ReservePreparation,
    revision: RevisionState,
    now: OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    use crate::session_owner::preparation::PreparationPurpose;
    if matches!(request.purpose, PreparationPurpose::OrdinaryAdmission)
        && (state.runs.values().any(|run| {
            run.session_id == request.owner.binding.session_id && !run.status.is_terminal()
        }) || state
            .session_owners
            .has_preparation(&request.owner.binding.session_id))
    {
        return Err(Error::Busy);
    }
    let actual = match revision {
        RevisionState::Absent(_) => crabber_core::SessionRevision::INITIAL,
        RevisionState::Present(value) => value,
    };
    if actual != request.expected_revision {
        return Err(Error::RevisionConflict);
    }
    validate_preparation_source_with_proof(state, request, now, proof)
}
fn validate_preparation_source(
    state: &State,
    request: &crate::session_owner::preparation::ReservePreparation,
    now: OffsetDateTime,
) -> Result<(), Error> {
    validate_preparation_source_with_proof(
        state,
        request,
        now,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
}
fn validate_preparation_source_with_proof(
    state: &State,
    request: &crate::session_owner::preparation::ReservePreparation,
    now: OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    use crate::session_owner::preparation::PreparationPurpose;
    match &request.purpose {
        PreparationPurpose::OrdinaryAdmission => Ok(()),
        PreparationPurpose::ExistingRun { run_id, .. } => {
            let run = state
                .runs
                .get(run_id)
                .filter(|run| run.session_id == request.owner.binding.session_id)
                .ok_or(Error::NotFound)?;
            let original = super::execution_binding::read(state, &run.session_id, run_id)
                .map_err(|_| Error::UnknownStoreOutcome)?
                .ok_or(Error::UnknownStoreOutcome)?;
            if crate::session_owner::preparation::resource_contract(
                &original.capsule.runtime_semantics,
            )? != request.resource_contract
            {
                return Err(Error::SemanticConflict);
            }
            super::native_parent::verify_run(state, run).map_err(|_| Error::UnknownStoreOutcome)?;
            let source = request.source.as_ref().ok_or(Error::Unsupported)?;
            crate::session_owner::preparation::source_validation::validate(source, request)?;
            match source {
                crate::session_owner::preparation::PreparationSource::ExpiredRun(source) => {
                    let current = super::expired::context(state, source.request().clone(), now)?;
                    source.verify_source(&current)?;
                }
                crate::session_owner::preparation::PreparationSource::AdmissionRecovery(
                    recovery,
                ) => {
                    super::admission_execution::validate_source(state, recovery, now)
                        .map_err(|_| Error::UnknownStoreOutcome)?;
                }
                crate::session_owner::preparation::PreparationSource::ApprovalClaim(claim) => {
                    super::approval::preparation_context(state, run, claim)
                        .map_err(Error::ApprovalSource)?;
                }
                crate::session_owner::preparation::PreparationSource::ToolClaim(claim) => {
                    super::tool_continuation::preparation_context(state, run, claim)
                        .map_err(Error::ToolSource)?;
                }
                crate::session_owner::preparation::PreparationSource::FrontendClaim(claim) => {
                    super::tool_continuation::preparation_frontend_context(state, run, claim)
                        .map_err(Error::ToolSource)?;
                }
                crate::session_owner::preparation::PreparationSource::FrontendRecovery(
                    recovery,
                ) => {
                    super::tool_continuation::preparation_frontend_recovery_context(
                        state, run, recovery, now,
                    )
                    .map_err(Error::ToolSource)?;
                }
                crate::session_owner::preparation::PreparationSource::ApprovalRecovery(
                    recovery,
                ) => {
                    super::approval::preparation_recovery_context(state, run, recovery, now)
                        .map_err(Error::ApprovalSource)?;
                }
                crate::session_owner::preparation::PreparationSource::ToolRecovery(recovery) => {
                    super::tool_continuation::preparation_recovery_context(
                        state, run, recovery, now,
                    )
                    .map_err(Error::ToolSource)?;
                }
            }
            Ok(())
        }
        PreparationPurpose::Queue { authority } => {
            super::queue::preparation_context(state, authority, &request.intent, now, proof)
                .map_err(|_| Error::StaleOwner)
        }
    }
}
