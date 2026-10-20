//! Exact owned unkeyed replay carries metadata, never a replacement Run fence.
use crate::postgres::transaction::Transaction;
use crate::session_owner::SessionOwnerError as Error;
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationCompletion, PreparedOperation,
    admission::{SourceAudits, Witness},
};
use sqlx::{Postgres, Row};
impl crate::postgres::PostgresStore {
    pub(super) async fn replay_admission(
        tx: &mut Transaction<'_, Postgres>,
        joined: &JoinedPreparation,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Option<PreparationCompletion>, Error> {
        let PreparedOperation::Admit(bound) = joined.observed_operation() else {
            return Ok(None);
        };
        let grant = joined.observed_grant();
        let metadata = grant.reconciliation()?;
        let query = format!(
            "SELECT CAST(COALESCE(octet_length(witness),0)+COALESCE(octet_length(initial),0)+COALESCE(octet_length(session),0)+COALESCE(octet_length(run_id),0) AS BIGINT) FROM ({ORIGINAL}) selected"
        );
        crate::postgres::read_proof::charge(
            tx,
            sqlx::query_scalar(&query)
                .bind(&metadata.session_id.0)
                .bind(&metadata.join_id.0),
            proof,
        )
        .await
        .map_err(|_| Error::Capacity)?;
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
            let audit = crate::postgres::session_owner::source_audit_with_proof(
                tx,
                &metadata.session_id,
                &id,
                proof,
            )
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
                &bound.binding,
            )
            .map(PreparationCompletion::AdmissionReplayed)
            .map(Some)
    }
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
const ORIGINAL: &str = "SELECT CASE WHEN octet_length(w.data_json)<=8192 THEN w.data_json END AS witness,CASE WHEN octet_length(i.data_json)<=2048 THEN i.data_json END AS initial,CASE WHEN octet_length(s.data::text)<=16384 THEN s.data::text END AS session,CASE WHEN octet_length(w.run_id)<=512 THEN w.run_id END AS run_id FROM preparation_admission_witnesses w JOIN sessions s ON s.id=w.session_id LEFT JOIN native_run_issuances i ON i.session_id=w.session_id AND i.run_id=w.run_id AND i.sequence=1 WHERE w.session_id=$1 AND w.root_join=$2";
