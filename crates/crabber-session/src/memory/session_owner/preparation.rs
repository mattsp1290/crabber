//! Preparation relinquishment and native successor publication share one gate.
#[path = "preparation/inspection.rs"]
mod inspection;
#[path = "preparation/native.rs"]
mod native;
#[path = "preparation/replay.rs"]
mod replay;
use super::{MemoryStore, revision};
use crate::StoreError;
use crate::session_owner::preparation::{JoinedPreparation, PreparationCompletion};

impl MemoryStore {
    pub(in crate::memory) fn complete_preparation(
        &self,
        joined: &JoinedPreparation,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        joined.verify_original_intent()?;
        let session = joined
            .observed_grant()
            .original_reservation()
            .owner
            .binding
            .session_id
            .clone();
        let outcome = self.transact_or_replay(
            |state| {
                if let crate::session_owner::preparation::PreparedOperation::ExpiredRunResume(
                    prepared,
                ) = joined.observed_operation()
                {
                    super::super::deletion::ensure_live(state, &session)?;
                    if let Some(outcome) = Self::replay_expired(state, joined)? {
                        return Ok(Some(outcome));
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
                    return Ok(outcome);
                }
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
                let outcome =
                    self.apply_prepared_native(state, joined.observed_operation().clone(), &proof)?;
                super::before(super::fault(self))?;
                #[cfg(test)]
                crate::binding_fault::trip(&self.binding_fault, 1)?;
                Ok(outcome)
            },
        )?;
        #[cfg(test)]
        after_commit(self, &outcome)?;
        Ok(outcome)
    }
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
