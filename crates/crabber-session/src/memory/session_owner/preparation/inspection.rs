//! Borrowed original sources under one lock; no cloned State or current head.
use crate::memory::{MemoryStore, State};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::SessionOwnerError as Error;
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationAdmissionReceipt, PreparationCommitError, inspection,
};

impl MemoryStore {
    pub(in crate::memory) fn inspect_initial_admission(
        &self,
        original: &JoinedPreparation,
    ) -> Result<Option<PreparationAdmissionReceipt>, PreparationCommitError> {
        self.inspect_initial_admission_with_proof(
            original,
            &mut Proof::bounded(inspection::MAX_PROOF),
        )
    }
    pub(in crate::memory) fn inspect_initial_admission_with_proof(
        &self,
        original: &JoinedPreparation,
        proof: &mut Proof,
    ) -> Result<Option<PreparationAdmissionReceipt>, PreparationCommitError> {
        let state = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        let session = &original
            .observed_grant()
            .original_reservation()
            .owner
            .binding
            .session_id;
        crate::memory::deletion::ensure_live(&state, session)
            .map_err(crate::deletion::owner_error)?;
        read(&state, original, proof)
    }
}
fn read(
    state: &State,
    original: &JoinedPreparation,
    proof: &mut Proof,
) -> Result<Option<PreparationAdmissionReceipt>, PreparationCommitError> {
    let bound = inspection::original(original, proof)?;
    let grant = original.observed_grant();
    let metadata = grant.reconciliation()?;
    let key = (metadata.session_id.clone(), metadata.join_id);
    let Some(run) = state.preparation_admission_joins.get(&key) else {
        proof.charge(1).map_err(|error| {
            proof.admission_failure(
                &error,
                crate::session_owner::preparation::PreparationCommitError::Owner(Error::Capacity),
            )
        })?;
        return if state.session_owners.source_audit(&key.0, &key.1).is_some() {
            Err(Error::UnknownStoreOutcome.into())
        } else {
            Ok(None)
        };
    };
    proof.measure(&(&key, run)).map_err(|error| {
        proof.admission_failure(
            &error,
            crate::session_owner::preparation::PreparationCommitError::Owner(Error::Capacity),
        )
    })?;
    if !state.preparation_admissions.contains_key(run) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let (session, initial, witness, sources) =
        super::super::drivers::sources_read(state, run, proof)
            .map_err(|error| inspection::read_error(proof, error))?;
    let protected =
        crate::memory::native_parent::source_protocol::original(state, &key.0, run, 1, proof)
            .map_err(|failure| {
                inspection::read_error(
                    proof,
                    crate::public_session::v2::proof::ReadError::Budget(failure),
                )
            })?
            .ok_or(Error::UnknownStoreOutcome)?;
    if !state
        .native_run_protocols
        .contains(&(key.0.clone(), run.clone()))
    {
        return Err(Error::Unsupported.into());
    }
    if session.id != key.0 || witness.join_id()? != key.1 || protected != initial {
        return Err(Error::UnknownStoreOutcome.into());
    }
    witness
        .replay(grant, session, initial, &sources, &bound.binding)
        .map(Some)
        .map_err(Into::into)
}

#[cfg(test)]
#[path = "inspection_tests.rs"]
mod tests;
