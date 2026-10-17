//! Native selected carriers while the original Memory State lock is held.
use super::{queue, storage_error};
use crate::session_owner::preparation::{JoinedPreparation, PreparationCommitError};
use crate::{
    StoreError,
    memory::{MemoryStore, State},
    public_session::v2::proof::Proof,
};
impl MemoryStore {
    pub(super) fn before_prepared_native(
        &self,
        state: &State,
        joined: &JoinedPreparation,
        proof: &mut Proof,
    ) -> Result<queue::Before, PreparationCommitError> {
        if let Some(outcome) = Self::replay_prepared(state, joined)? {
            return Ok(queue::Before::Replayed(Box::new(outcome)));
        }
        let session = &joined
            .observed_grant()
            .original_reservation()
            .owner
            .binding
            .session_id;
        state
            .session_owners
            .preflight_preparation(joined.observed_grant(), proof)
            .map_err(storage_error)?;
        let selected = queue::before(state, joined, proof, self.clock.now())?;
        if matches!(selected, queue::Before::Replayed(_)) {
            return Ok(selected);
        }
        match joined.observed_operation() {
            crate::session_owner::preparation::PreparedOperation::AdmissionRecovery(recovery) => {
                crate::native_enrollment::caller::measure_recovery_source(&recovery.source, proof)
                    .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
                crate::memory::admission_execution::validate_source_with_proof(
                    state,
                    &recovery.source,
                    self.clock.now(),
                    proof,
                )?;
            }
            crate::session_owner::preparation::PreparedOperation::Admit(bound) => {
                proof
                    .measure(&(&bound.request, &bound.binding))
                    .map_err(|_| StoreError::Conflict)?;
                crate::memory::admission::selected(state, &bound.request, None, proof)?;
            }
            crate::session_owner::preparation::PreparedOperation::AdmitKeyed(bound) => {
                proof
                    .measure(&(&bound.keyed, &bound.binding))
                    .map_err(|_| StoreError::Conflict)?;
                crate::memory::admission::selected(
                    state,
                    &bound.keyed.request,
                    Some(&bound.keyed),
                    proof,
                )?;
            }
            _ => (),
        }
        if let crate::session_owner::preparation::PreparedOperation::ExpiredRunResume(prepared) =
            joined.observed_operation()
        {
            crate::memory::deletion::ensure_live(state, session)?;
            let current = crate::memory::expired::context_with_proof(
                state,
                prepared.source.request().clone(),
                self.clock.now(),
                proof,
            )?;
            prepared.source.verify_source(&current)?;
        }
        Ok(selected)
    }
}
