//! Preparation relinquishment and native successor publication share one gate.
#[path = "preparation/inspection.rs"]
mod inspection;
#[path = "preparation/native.rs"]
mod native;
native_pending_initial_items! {
    #[path = "preparation/pending_initial.rs"]
    mod pending_initial;
}
#[path = "preparation/preflight.rs"]
mod preflight;
#[path = "preparation/queue.rs"]
mod queue;
#[path = "preparation/queue_inspection.rs"]
mod queue_inspection;
#[path = "preparation/replay.rs"]
mod replay;
use super::{MemoryStore, revision};
use crate::StoreError;
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationCompletion, PreparedDriverCompletion,
};

impl MemoryStore {
    pub(in crate::memory) fn complete_preparation(
        &self,
        joined: &JoinedPreparation,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        self.complete_preparation_captured(joined, false)
            .map(|v| v.completion)
    }
    pub(in crate::memory) fn complete_preparation_captured(
        &self,
        joined: &JoinedPreparation,
        require_input_birth: bool,
    ) -> Result<PreparedDriverCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        self.complete_preparation_captured_with_proof(
            joined,
            require_input_birth,
            &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
        )
    }
    pub(in crate::memory) fn complete_preparation_captured_with_proof(
        &self,
        joined: &JoinedPreparation,
        require_input_birth: bool,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<PreparedDriverCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        crate::native_enrollment::caller::initial::preparation_input(joined, read_proof)
            .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
        joined.verify_original_intent()?;
        let mut guard = self.state.lock().expect("memory store poisoned");
        let selected = match self.before_prepared_native(&guard, joined, read_proof)? {
            queue::Before::Other => None,
            queue::Before::Selected(selected) => Some(*selected),
            queue::Before::Replayed(outcome) => {
                return Ok(
                    crate::session_owner::preparation::driver_completion::replayed(*outcome),
                );
            }
        };
        // Retain borrowed protected originals under this same lock while the
        // inherited State clone is mutated. That whole clone is not bounded.
        let mut next = guard.clone();
        let outcome = self.complete_preparation_transaction(
            &mut next,
            joined,
            require_input_birth,
            read_proof,
            selected,
        )?;
        *guard = next;
        drop(guard);
        #[cfg(test)]
        after_commit(self, &outcome.completion)?;
        Ok(outcome)
    }
    fn complete_preparation_transaction(
        &self,
        state: &mut super::super::State,
        joined: &JoinedPreparation,
        require_input_birth: bool,
        read_proof: &mut crate::public_session::v2::proof::Proof,
        selected: Option<super::super::queue::selection::Selected<'_>>,
    ) -> Result<PreparedDriverCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        let session = &joined
            .observed_grant()
            .original_reservation()
            .owner
            .binding
            .session_id;
        super::super::deletion::ensure_live(state, session)?;
        let now = self.clock.now();
        ensure_input_birth(state, joined, require_input_birth, read_proof)?;
        let before = revision(state, session);
        let mut proof = state
            .session_owners
            .verify_preparation_with_proof(joined.observed_grant(), before, now, read_proof)
            .map_err(storage_error)?;
        proof.bind_observed_operation(joined)?;
        state
            .session_owners
            .mutate_preparation_join(&proof, now, before)
            .map_err(storage_error)?;
        if state.sessions.contains_key(session) {
            super::super::bump_public_revision(state, session)?;
        }
        let outcome = self.apply_prepared_native(
            state,
            joined.observed_operation().clone(),
            &proof,
            read_proof,
            selected,
        )?;
        let outcome =
            crate::session_owner::preparation::driver_completion::native(outcome, &proof)?;
        super::before(super::fault(self))?;
        #[cfg(test)]
        crate::binding_fault::trip(&self.binding_fault, 1)?;
        Ok(outcome)
    }
}
fn ensure_input_birth(
    state: &super::super::State,
    joined: &JoinedPreparation,
    required: bool,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), crate::session_owner::preparation::PreparationCommitError> {
    if required {
        let scope = crate::session_owner::preparation::driver_completion::protected_scope(joined)?;
        if !state
            .continuation_inputs
            .protocol(&scope, proof)
            .map_err(|_| crate::session_owner::SessionOwnerError::UnknownStoreOutcome)?
        {
            return Err(crate::session_owner::SessionOwnerError::Unsupported.into());
        }
    }
    Ok(())
}
fn storage_error(error: crate::session_owner::SessionOwnerError) -> StoreError {
    match error {
        crate::session_owner::SessionOwnerError::RevisionConflict => {
            StoreError::AdmissionRevisionConflict
        }
        _ => StoreError::Validation("preparation ownership proof unavailable".into()),
    }
}

fn verify_replay(
    state: &super::super::State,
    grant: &crate::session_owner::preparation::PreparationGrant,
) -> Result<(), crate::session_owner::SessionOwnerError> {
    state.session_owners.verify_preparation_join(grant)
}

#[cfg(test)]
fn after_commit(
    store: &super::MemoryStore,
    value: &PreparationCompletion,
) -> Result<(), crate::session_owner::preparation::PreparationCommitError> {
    use std::sync::atomic::Ordering;
    if value.is_replayed() {
        return Ok(());
    }
    if store.session_owner_fault.load(Ordering::SeqCst) == 2 {
        return Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into());
    }
    match value {
        PreparationCompletion::Approval(_) if store.approval_fault.load(Ordering::SeqCst) == 2 => {
            return Err(crate::ApprovalError::UnknownStoreOutcome.into());
        }
        PreparationCompletion::ToolContinuation(_)
            if store.tool_continuation_fault.load(Ordering::SeqCst) == 2 =>
        {
            return Err(crate::ToolContinuationError::UnknownStoreOutcome.into());
        }
        PreparationCompletion::Queue(_) => crate::binding_fault::trip(&store.queue_fault, 2)?,
        _ => (),
    }
    crate::binding_fault::trip(&store.binding_fault, 2)?;
    Ok(())
}

pub(in crate::memory) use native::{VerifiedTool, verify_tool};
