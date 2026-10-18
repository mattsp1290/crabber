//! The ordinary Memory session gate encloses proof and clone-publication.
use super::super::{MemoryStore, State};
use crate::deletion::{
    coordinator,
    coverage::{CoverageRead, MAX_PROOF_BYTES},
};
use crate::public_session::v2::proof::{Failure, Proof};
use crate::{DeleteSession, DeletionBlocker as Blocker, DeletionError as Error, DeletionOutcome};
use crabber_core::SessionRevision;

enum Abort {
    Domain(Error),
    ProofLimit,
}
impl From<Error> for Abort {
    fn from(error: Error) -> Self {
        Self::Domain(error)
    }
}
impl From<Failure> for Abort {
    fn from(error: Failure) -> Self {
        match error {
            Failure::Limited(_) => Self::ProofLimit,
            Failure::Error(_) => Self::Domain(Error::UnknownStoreOutcome),
        }
    }
}
impl MemoryStore {
    pub(in crate::memory) fn guarded_delete(
        &self,
        request: DeleteSession,
    ) -> Result<DeletionOutcome, Error> {
        self.guarded_delete_with_proof(request, &mut Proof::bounded(MAX_PROOF_BYTES))
    }
    fn guarded_delete_with_proof(
        &self,
        request: DeleteSession,
        proof: &mut Proof,
    ) -> Result<DeletionOutcome, Error> {
        let digest = request.fingerprint()?;
        let result = self.transact(|state| -> Result<DeletionOutcome, Abort> {
            if let Some(saved) = state.tombstones.get(&request.session_id) {
                if state.sessions.contains_key(&request.session_id) {
                    return Err(Error::UnknownStoreOutcome.into());
                }
                return coordinator::replay(saved, &request, &digest).map_err(Abort::Domain);
            }
            if state
                .operation_receipts
                .contains_key(&(request.session_id.clone(), request.id.clone()))
            {
                return Err(Error::SemanticConflict.into());
            }
            if !state.sessions.contains_key(&request.session_id) {
                return Err(Error::NotFound.into());
            }
            let revision = state
                .public_revisions
                .get(&request.session_id)
                .copied()
                .unwrap_or(SessionRevision::INITIAL);
            if revision != request.expected_revision {
                return Err(Error::RevisionConflict.into());
            }
            let coverage = match super::coverage(state, &request.session_id)? {
                CoverageRead::Unknown => {
                    return Ok(DeletionOutcome::Blocked(Blocker::OwnershipUnknown));
                }
                CoverageRead::Limited => return Ok(DeletionOutcome::Blocked(Blocker::ProofLimit)),
                CoverageRead::Verified(value) => value,
            };
            if let Some(blocker) = super::readiness::read(state, &coverage, proof)? {
                return Ok(DeletionOutcome::Blocked(blocker));
            }
            let saved = coordinator::tombstone(request, digest, self.clock.now())?;
            let receipt = finish_delete(state, saved, proof)?;
            #[cfg(test)]
            crate::binding_fault::trip(&self.deletion_fault, 1)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            Ok(DeletionOutcome::Applied(receipt))
        });
        let result = match result {
            Ok(value) => value,
            Err(Abort::ProofLimit) => return Ok(DeletionOutcome::Blocked(Blocker::ProofLimit)),
            Err(Abort::Domain(error)) => return Err(error),
        };
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

// A purge refusal must abort the entire candidate, including already-erased
// Pending projections. Returning Ok(Blocked) inside transact would commit it.
fn finish_delete(
    state: &mut State,
    saved: crate::deletion::Tombstone,
    proof: &mut Proof,
) -> Result<crate::DeletionReceipt, Abort> {
    proof.measure(&(&saved.receipt.session_id, &saved))?;
    let session = &saved.receipt.session_id;
    state.tombstones.insert(session.clone(), saved.clone());
    super::purge::purge(state, session, proof)?;
    Ok(saved.receipt)
}

native_pending_initial_items! {
    #[cfg(test)]
    #[path = "coordinator/fixed_initial_readiness.rs"]
    mod fixed_initial_readiness;
}
