//! Preparation relinquishment and native successor publication share one gate.
#[path = "preparation/inspection.rs"]
mod inspection;
#[path = "preparation/native.rs"]
mod native;
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
        require_approval_birth: bool,
    ) -> Result<PreparedDriverCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        joined.verify_original_intent()?;
        let session = joined
            .observed_grant()
            .original_reservation()
            .owner
            .binding
            .session_id
            .clone();
        let read_proof =
            std::cell::RefCell::new(crate::public_session::v2::proof::Proof::bounded(32 << 20));
        let queue_selection = std::cell::RefCell::new(None);
        let outcome = self.transact_or_replay(
            |state| {
                match queue::before(
                    state,
                    joined,
                    &mut read_proof.borrow_mut(),
                    self.clock.now(),
                )? {
                    queue::Before::Other => (),
                    queue::Before::Replayed(outcome) => {
                        return Ok(Some(
                            crate::session_owner::preparation::driver_completion::replayed(
                                *outcome,
                            ),
                        ));
                    }
                    queue::Before::Selected(selected) => {
                        *queue_selection.borrow_mut() = Some(*selected);
                    }
                }
                if let crate::session_owner::preparation::PreparedOperation::ExpiredRunResume(
                    prepared,
                ) = joined.observed_operation()
                {
                    super::super::deletion::ensure_live(state, &session)?;
                    if let Some(outcome) = Self::replay_expired(state, joined)? {
                        return Ok(Some(
                            crate::session_owner::preparation::driver_completion::replayed(outcome),
                        ));
                    }
                    let current = super::super::expired::context(
                        state,
                        prepared.source.request().clone(),
                        self.clock.now(),
                    )?;
                    prepared.source.verify_source(&current)?;
                }
                Ok(None)
            },
            |state| -> Result<_, crate::session_owner::preparation::PreparationCommitError> {
                super::super::deletion::ensure_live(state, &session)?;
                let now = self.clock.now();
                // Original native reconciliation precedes current owner/CAS checks.
                if let Some(outcome) = self.replay_prepared(state, joined)? {
                    return Ok(
                        crate::session_owner::preparation::driver_completion::replayed(outcome),
                    );
                }
                ensure_approval_birth(
                    state,
                    joined,
                    require_approval_birth,
                    &mut read_proof.borrow_mut(),
                )?;
                let before = revision(state, &session);
                let mut proof = state
                    .session_owners
                    .verify_preparation(joined.observed_grant(), before, now)
                    .map_err(storage_error)?;
                proof.bind_observed_operation(joined)?;
                state
                    .session_owners
                    .mutate_preparation_join(&proof, now, before)
                    .map_err(storage_error)?;
                if state.sessions.contains_key(&session) {
                    super::super::bump_public_revision(state, &session)?;
                }
                let outcome = self.apply_prepared_native(
                    state,
                    joined.observed_operation().clone(),
                    &proof,
                    &mut read_proof.borrow_mut(),
                    queue_selection.borrow_mut().take(),
                )?;
                let outcome =
                    crate::session_owner::preparation::driver_completion::native(outcome, &proof)?;
                super::before(super::fault(self))?;
                #[cfg(test)]
                crate::binding_fault::trip(&self.binding_fault, 1)?;
                Ok(outcome)
            },
        )?;
        #[cfg(test)]
        after_commit(self, &outcome.completion)?;
        Ok(outcome)
    }
}
fn ensure_approval_birth(
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
