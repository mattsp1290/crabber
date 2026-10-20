//! Actual generic expired-run successor and its original publisher result.
use super::super::State;
use super::{Error, MemoryStore, unknown};
use crate::native_enrollment::caller::{
    Caller, Fresh, initial::Capture, recovery::ExpiredRecovery,
};
use crate::session_owner::preparation::{
    self, PreparationCommitError, PreparedExpiredRunResume, proof::VerifiedPreparation,
};
impl MemoryStore {
    pub(in crate::memory) fn claim_prepared_expired(
        &self,
        state: &mut State,
        prepared: &PreparedExpiredRunResume,
        proof: &VerifiedPreparation<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<preparation::ExpiredRunOutcome, PreparationCommitError> {
        crate::native_enrollment::caller::recovery::measure_expired(&prepared.source, read_proof)
            .map_err(unknown)?;
        read_proof.measure(&*prepared.binding).map_err(unknown)?;
        prepared.validate()?;
        let source = &prepared.source;
        let current = super::context_with_proof(
            state,
            source.request().clone(),
            self.clock.now(),
            read_proof,
        )?;
        source.verify_source(&current)?;
        let completion =
            preparation::expired::completion_source(source, proof, current.expected_revision())?;
        let original = {
            let selected = state
                .runs
                .get(&source.request().expected_fence.run_id)
                .ok_or(Error::NotFound)?;
            read_proof.measure(selected).map_err(unknown)?;
            selected.clone()
        };
        read_proof.measure(&original).map_err(unknown)?;
        let next = preparation::expired::replacement(&original, source, self.clock.now())?;
        let publication = super::super::native_parent::record_run_completed_publication_with_proof(
            state,
            &original,
            &next,
            Some(&completion),
            read_proof,
            Capture::Original,
        )
        .map_err(unknown)?;
        let outcome =
            preparation::expired::claimed(source, &completion, current.expected_revision(), &next);
        let published = publication
            .updated
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?;
        super::super::native_enrollment::record_raw_native(
            state,
            Caller::Raw,
            &Fresh::ExpiredRecovery(ExpiredRecovery {
                source,
                previous: &original,
                updated: &next,
                completion: &completion,
                outcome: &outcome,
                publication: published,
            }),
            read_proof,
            #[cfg(test)]
            &self.native_raw_probe,
        )
        .map_err(unknown)?;
        state.runs.insert(next.id.clone(), next);
        Ok(outcome)
    }
}
