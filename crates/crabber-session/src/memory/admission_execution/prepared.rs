//! Actual claim under the exact authenticated preparation Join transaction.
use super::{Error, MemoryStore};
use crate::session_owner::preparation::{
    PreparationCommitError, PreparedAdmissionRecovery, proof::VerifiedPreparation,
};
impl MemoryStore {
    pub(in crate::memory) fn claim_prepared_admission(
        &self,
        state: &mut super::super::State,
        recovery: &PreparedAdmissionRecovery,
        proof: &VerifiedPreparation<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::ClaimedAdmission, PreparationCommitError> {
        recovery
            .source
            .measure_original(read_proof)
            .map_err(|_| Error::UnknownStoreFailure)?;
        read_proof
            .measure(&*recovery.binding)
            .map_err(|_| Error::UnknownStoreFailure)?;
        recovery.validate()?;
        let source = &recovery.source;
        super::inspection::validate_source_with_proof(state, source, self.clock.now(), read_proof)?;
        let crate::session_owner::audit::RevisionState::Present(revision) =
            super::super::session_owner::revision(state, &source.request().session_id)
        else {
            return Err(Error::MissingEvidence.into());
        };
        let completion = crate::session_owner::preparation::recovery::completion_source(
            source, proof, revision,
        )?;
        Ok(self.claim_admission_transaction(
            state,
            source.request(),
            Some(completion),
            read_proof,
        )?)
    }
}
