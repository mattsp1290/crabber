//! Exact successor issuer plus original sources, never current state or a grant.
use crate::postgres::transaction::Transaction;
use crate::session_owner::SessionOwnerError as Error;
use crate::session_owner::preparation::{
    AdmissionRecoveryOutcome, JoinedPreparation, PreparationCommitError, PreparationCompletion,
    PreparedOperation,
    recovery::replay::{self, NativeSources},
};
use sqlx::{Postgres, Row};
impl crate::postgres::PostgresStore {
    pub(super) async fn replay_recovery(
        tx: &mut Transaction<'_, Postgres>,
        joined: &JoinedPreparation,
    ) -> Result<Option<PreparationCompletion>, PreparationCommitError> {
        let PreparedOperation::AdmissionRecovery(recovery) = joined.observed_operation() else {
            return Ok(None);
        };
        let source = &recovery.source;
        let request = source.request();
        let Some(row) = sqlx::query(ORIGINAL)
            .bind(&request.session_id.0)
            .bind(&request.expected_fence.run_id.0)
            .bind(i64::try_from(source.expected_issuance()).map_err(unknown)?)
            .bind(request.key.as_str())
            .fetch_optional(&mut **tx)
            .await
            .map_err(unknown)?
        else {
            return Ok(None);
        };
        let previous = decode(&row, "previous")?;
        let next = decode(&row, "next")?;
        let initial = decode(&row, "initial")?;
        let init: crate::admission_journal::Source = decode(&row, "init")?;
        let receipt = decode(&row, "receipt")?;
        let crate::admission_journal::Source::Init(init) = init else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        let mut audits = Vec::with_capacity(5);
        for id in replay::audit_ids(joined.observed_grant())? {
            let audit = crate::postgres::session_owner::source_audit(tx, &request.session_id, &id)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            audits.push(audit);
        }
        let receipt = replay::verify(
            joined.observed_grant(),
            recovery,
            &audits,
            &NativeSources {
                previous: &previous,
                next: &next,
                initial: &initial,
                init: &init,
                receipt: &receipt,
            },
        )?;
        Ok(Some(PreparationCompletion::AdmissionRecovery(
            AdmissionRecoveryOutcome::Replayed(receipt),
        )))
    }
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
fn decode<T: serde::de::DeserializeOwned>(
    row: &sqlx::postgres::PgRow,
    name: &str,
) -> Result<T, Error> {
    let text = row
        .try_get::<Option<String>, _>(name)
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    serde_json::from_str(&text).map_err(unknown)
}
const ORIGINAL: &str = "SELECT CASE WHEN octet_length(p.data_json)<=2048 THEN p.data_json END AS previous,CASE WHEN octet_length(n.data_json)<=2048 THEN n.data_json END AS next,CASE WHEN octet_length(f.data_json)<=2048 THEN f.data_json END AS initial,CASE WHEN octet_length(a.data_json)<=8192 THEN a.data_json END AS init,CASE WHEN octet_length(r.data::text)<=8192 THEN r.data::text END AS receipt FROM native_run_issuances n LEFT JOIN native_run_issuances p ON p.session_id=n.session_id AND p.run_id=n.run_id AND p.sequence=$3 LEFT JOIN native_run_issuances f ON f.session_id=n.session_id AND f.run_id=n.run_id AND f.sequence=1 LEFT JOIN admission_execution_sources a ON a.session_id=n.session_id AND a.run_id=n.run_id AND a.kind='init' LEFT JOIN admission_receipts r ON r.session_id=n.session_id AND r.admission_key=$4 WHERE n.session_id=$1 AND n.run_id=$2 AND n.sequence=$3+1";
