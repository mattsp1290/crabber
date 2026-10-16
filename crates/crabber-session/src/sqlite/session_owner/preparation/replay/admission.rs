//! Exact owned unkeyed replay carries metadata, never a replacement Run fence.
use crate::session_owner::SessionOwnerError as Error;
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationCompletion, PreparedOperation,
    admission::{SourceAudits, Witness},
};
use sqlx::{Row, Sqlite, Transaction};
impl crate::sqlite::SqliteStore {
    pub(super) async fn replay_admission(
        tx: &mut Transaction<'_, Sqlite>,
        joined: &JoinedPreparation,
    ) -> Result<Option<PreparationCompletion>, Error> {
        if !matches!(joined.observed_operation(), PreparedOperation::Admit(_)) {
            return Ok(None);
        }
        let grant = joined.observed_grant();
        let metadata = grant.reconciliation()?;
        let row = sqlx::query(ORIGINAL)
            .bind(&metadata.session_id.0)
            .bind(&metadata.join_id.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unknown)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let witness: Witness =
            serde_json::from_str(row.try_get("witness").map_err(unknown)?).map_err(unknown)?;
        witness.validate()?;
        if witness.session_id() != &metadata.session_id
            || witness.join_id()? != metadata.join_id
            || row.try_get::<Option<&str>, _>("run_id").map_err(unknown)?
                != Some(witness.run_id().0.as_str())
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let session =
            serde_json::from_str(row.try_get("session").map_err(unknown)?).map_err(unknown)?;
        let initial =
            serde_json::from_str(row.try_get("initial").map_err(unknown)?).map_err(unknown)?;
        let mut audits = Vec::with_capacity(5);
        for id in witness.audit_ids()? {
            let audit = crate::sqlite::session_owner::source_audit(tx, &metadata.session_id, &id)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            audits.push(audit);
        }
        let audits = audits.try_into().map_err(|_| Error::UnknownStoreOutcome)?;
        witness
            .replay(
                grant,
                &session,
                &initial,
                &SourceAudits::from_array(&audits),
            )
            .map(PreparationCompletion::AdmissionReplayed)
            .map(Some)
    }
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
const ORIGINAL: &str = "SELECT CASE WHEN length(CAST(w.data_json AS BLOB))<=8192 THEN w.data_json END AS witness,CASE WHEN length(CAST(i.data_json AS BLOB))<=2048 THEN i.data_json END AS initial,CASE WHEN length(CAST(s.data AS BLOB))<=16384 THEN s.data END AS session,CASE WHEN length(CAST(w.run_id AS BLOB))<=512 THEN w.run_id END AS run_id FROM preparation_admission_witnesses w JOIN sessions s ON s.id=w.session_id LEFT JOIN native_run_issuances i ON i.session_id=w.session_id AND i.run_id=w.run_id AND i.sequence=1 WHERE w.session_id=$1 AND w.root_join=$2";
