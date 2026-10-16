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
    ) -> Result<crate::ClaimedAdmission, PreparationCommitError> {
        recovery.validate()?;
        let source = &recovery.source;
        super::validate_source(state, source, self.clock.now())?;
        let crate::session_owner::audit::RevisionState::Present(revision) =
            super::super::session_owner::revision(state, &source.request().session_id)
        else {
            return Err(Error::MissingEvidence.into());
        };
        let completion = crate::session_owner::preparation::recovery::completion_source(
            source, proof, revision,
        )?;
        Ok(self.claim_admission_transaction(state, source.request().clone(), Some(completion))?)
    }
}
