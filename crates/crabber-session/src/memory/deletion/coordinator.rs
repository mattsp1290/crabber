//! The ordinary Memory session gate encloses proof and clone-publication.
use super::super::MemoryStore;
use crate::deletion::{
    coordinator,
    coverage::{CoverageRead, MAX_PROOF_BYTES},
};
use crate::public_session::v2::proof::{Failure, Proof};
use crate::{DeleteSession, DeletionBlocker as Blocker, DeletionError as Error, DeletionOutcome};
use crabber_core::SessionRevision;

impl MemoryStore {
    pub(in crate::memory) fn guarded_delete(
        &self,
        request: DeleteSession,
    ) -> Result<DeletionOutcome, Error> {
        let digest = request.fingerprint()?;
        let result = self.transact(|state| {
            if let Some(saved) = state.tombstones.get(&request.session_id) {
                if state.sessions.contains_key(&request.session_id) {
                    return Err(Error::UnknownStoreOutcome);
                }
                return coordinator::replay(saved, &request, &digest);
            }
            if state
                .operation_receipts
                .contains_key(&(request.session_id.clone(), request.id.clone()))
            {
                return Err(Error::SemanticConflict);
            }
            if !state.sessions.contains_key(&request.session_id) {
                return Err(Error::NotFound);
            }
            let revision = state
                .public_revisions
                .get(&request.session_id)
                .copied()
                .unwrap_or(SessionRevision::INITIAL);
            if revision != request.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let coverage = match super::coverage(state, &request.session_id)? {
                CoverageRead::Unknown => {
                    return Ok(DeletionOutcome::Blocked(Blocker::OwnershipUnknown));
                }
                CoverageRead::Limited => return Ok(DeletionOutcome::Blocked(Blocker::ProofLimit)),
                CoverageRead::Verified(value) => value,
            };
            let mut proof = Proof::bounded(MAX_PROOF_BYTES);
            if let Some(blocker) = super::readiness::read(state, &coverage, &mut proof)? {
                return Ok(DeletionOutcome::Blocked(blocker));
            }
            let saved = coordinator::tombstone(request, digest, self.clock.now())?;
            // Purge's borrowed preflight uses the SAME remaining proof allowance.
            // A refusal publishes neither the tombstone nor any partial erasure.
            match super::purge::purge(state, &saved.receipt.session_id, &mut proof) {
                Ok(()) => {}
                Err(Failure::Limited(_)) => {
                    return Ok(DeletionOutcome::Blocked(Blocker::ProofLimit));
                }
                Err(Failure::Error(_)) => return Err(Error::UnknownStoreOutcome),
            }
            state
                .tombstones
                .insert(saved.receipt.session_id.clone(), saved.clone());
            #[cfg(test)]
            crate::binding_fault::trip(&self.deletion_fault, 1)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            Ok(DeletionOutcome::Applied(saved.receipt))
        })?;
        #[cfg(test)]
        if matches!(result, DeletionOutcome::Applied(_)) {
            crate::binding_fault::trip(&self.deletion_fault, 2)
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
        Ok(result)
    }
}

#[cfg(test)]
#[path = "coordinator/tests.rs"]
mod tests;
