//! Actual generic expired-run successor and its original publisher result.
use super::{Error, SqliteStore, unknown};
use crate::native_enrollment::caller::{
    Caller, Fresh, initial::Capture, recovery::ExpiredRecovery,
};
use crate::session_owner::preparation::{
    self, PreparationCommitError, PreparedExpiredRunResume, proof::VerifiedPreparation,
};
use crate::transaction::Transaction;
use sqlx::Sqlite;
impl SqliteStore {
    pub(in crate::sqlite) async fn claim_prepared_expired(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        prepared: &PreparedExpiredRunResume,
        proof: &VerifiedPreparation<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<preparation::ExpiredRunOutcome, PreparationCommitError> {
        crate::native_enrollment::caller::recovery::measure_expired(&prepared.source, read_proof)
            .map_err(unknown)?;
        read_proof.measure(&*prepared.binding).map_err(unknown)?;
        prepared.validate()?;
        let source = &prepared.source;
        let current =
            super::context_with_proof(tx, source.request().clone(), self.clock.now(), read_proof)
                .await?;
        source.verify_source(&current)?;
        let completion =
            preparation::expired::completion_source(source, proof, current.expected_revision())?;
        let original = super::load_run_with_proof(tx, source.request(), read_proof).await?;
        read_proof.measure(&original).map_err(unknown)?;
        let next = preparation::expired::replacement(&original, source, self.clock.now())?;
        let publication = super::super::records::save_run_publication_with_proof(
            tx,
            &next,
            Some(&completion),
            read_proof,
            Capture::Original,
        )
        .await
        .map_err(unknown)?;
        let outcome =
            preparation::expired::claimed(source, &completion, current.expected_revision(), &next);
        let published = publication
            .updated
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?;
        super::super::native_enrollment::record_raw_native(
            tx,
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
        .await
        .map_err(unknown)?;
        Ok(outcome)
    }
}
