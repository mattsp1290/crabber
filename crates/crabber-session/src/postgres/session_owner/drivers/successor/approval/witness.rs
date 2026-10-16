//! Complete bounded witnesses, never nullable target-field classification.
use super::{Error, Proof, unknown};
use crate::native_parent::{Kind, Witness};
use crabber_core::Run;
use futures::TryStreamExt;
use sqlx::{Postgres, Row, Transaction};
pub(super) async fn unstarted(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    parent: &crate::AttemptTarget,
    proof: &mut Proof,
) -> Result<(), Error> {
    crate::postgres::read_proof::charge(
        tx,
        sqlx::query_scalar(COST)
            .bind(&run.session_id.0)
            .bind(&run.id.0),
        proof,
    )
    .await
    .map_err(unknown)?;
    let mut rows = sqlx::query(RECORDS)
        .bind(&run.session_id.0)
        .bind(&run.id.0)
        .fetch(&mut **tx);
    while let Some(row) = rows.try_next().await.map_err(unknown)? {
        let raw = row
            .try_get::<Option<String>, _>("data")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        let witness: Witness = serde_json::from_str(&raw).map_err(unknown)?;
        witness.validate().map_err(unknown)?;
        let kind = match witness.source.kind {
            Kind::Attempt => "attempt",
            Kind::Phase => "phase",
        };
        if witness.source.session_id != run.session_id
            || witness.source.run_id != run.id
            || i64::try_from(witness.source.event_cursor.0).map_err(unknown)?
                != row.try_get::<i64, _>("event_seq").map_err(unknown)?
            || row
                .try_get::<Option<String>, _>("kind")
                .map_err(unknown)?
                .as_deref()
                != Some(kind)
            || row
                .try_get::<Option<String>, _>("operation")
                .map_err(unknown)?
                .as_deref()
                != Some(witness.source.operation_id.0.as_str())
        {
            return Err(Error::UnknownStoreOutcome);
        }
        if witness.source.attempt == *parent {
            return Err(Error::StaleOwner);
        }
    }
    Ok(())
}
const COST: &str = "SELECT CAST(COALESCE(SUM(octet_length(data_json)+octet_length(kind)+octet_length(operation_id)+8),0) AS BIGINT) FROM native_parent_witnesses WHERE session_id=$1 AND run_id=$2";
const RECORDS: &str = "SELECT event_seq,CASE WHEN octet_length(data_json)<=8192 THEN data_json END AS data,CASE WHEN octet_length(kind)<=16 THEN kind END AS kind,CASE WHEN octet_length(operation_id)<=128 THEN operation_id END AS operation FROM native_parent_witnesses WHERE session_id=$1 AND run_id=$2";
