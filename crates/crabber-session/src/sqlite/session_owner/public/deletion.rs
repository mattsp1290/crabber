//! Source-bound detached resource coverage; the Run's terminal proof is separate.
use crate::native_parent::{RunIssuance, Witness};
use crate::public_session::v2::proof::{Failure, Proof, ReadError, unknown};
use crate::session_owner::{coverage::detached_parent, record::Record};
use crate::transaction::Transaction;
use sqlx::{Row, Sqlite};

pub(super) async fn classified(
    tx: &mut Transaction<'_, Sqlite>,
    record: &Record,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    if record.driver.is_some() {
        return Box::pin(super::super::drivers::classified(tx, record, proof)).await;
    }
    let Some(parent) = detached_parent(record) else {
        return Ok(false);
    };
    if record
        .snapshot
        .binding
        .parent
        .as_ref()
        .is_some_and(|parent| parent.invocation.is_some())
    {
        super::super::invocation::verify(tx, record, proof)
            .await
            .map_err(unknown)?;
    } else {
        verify_parent(tx, parent, proof).await?;
    }
    Ok(true)
}
pub(in crate::sqlite) async fn verify_parent(
    tx: &mut Transaction<'_, Sqlite>,
    parent: &Witness,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let cursor = i64::try_from(parent.source.event_cursor.0).map_err(unknown)?;
    let sequence = i64::try_from(parent.run_issuance).map_err(unknown)?;
    let costs = sqlx::query(COST)
        .bind(&parent.source.session_id.0)
        .bind(&parent.source.run_id.0)
        .bind(&parent.source.operation_id.0)
        .bind(cursor)
        .bind(sequence)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or_else(|| unknown("detached original parent"))?;
    for field in ["witness", "receipt", "issuance"] {
        let bytes = costs
            .try_get::<Option<i64>, _>(field)
            .map_err(unknown)?
            .ok_or_else(|| unknown("detached parent source absent"))?;
        proof.charge(usize::try_from(bytes).map_err(unknown)?)?;
    }
    let row = sqlx::query(DATA)
        .bind(&parent.source.session_id.0)
        .bind(&parent.source.run_id.0)
        .bind(&parent.source.operation_id.0)
        .bind(cursor)
        .bind(sequence)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let witness: Witness = decode(&row, "witness")?;
    if &witness != parent {
        return Err(unknown("detached original witness association"));
    }
    let receipt = decode(&row, "receipt")?;
    let issuance: RunIssuance = decode(&row, "issuance")?;
    let event = super::super::super::read_proof::event(
        tx,
        &parent.source.session_id,
        parent.source.event_cursor,
        false,
        proof,
    )
    .await
    .map_err(|error| match error {
        ReadError::Budget(error) => error,
        ReadError::Domain(error) => unknown(error),
    })?;
    witness
        .verify_historical_phase(&receipt, &event, &issuance)
        .map_err(unknown)?;
    Ok(())
}

fn decode<T: serde::de::DeserializeOwned>(
    row: &sqlx::sqlite::SqliteRow,
    field: &str,
) -> Result<T, Failure> {
    let raw = row
        .try_get::<Option<&str>, _>(field)
        .map_err(unknown)?
        .ok_or_else(|| unknown("detached parent source bound"))?;
    serde_json::from_str(raw).map_err(unknown)
}
const COST: &str = "WITH scope AS (SELECT CAST($1 AS TEXT) AS session,CAST($2 AS TEXT) AS run,CAST($3 AS TEXT) AS operation,CAST($4 AS BIGINT) AS cursor,CAST($5 AS BIGINT) AS issuance) SELECT CAST(length(CAST(w.data_json AS BLOB)) AS BIGINT) AS witness,CAST(length(CAST(r.data_json AS BLOB)) AS BIGINT) AS receipt,CAST(length(CAST(i.data_json AS BLOB)) AS BIGINT) AS issuance FROM scope s JOIN native_parent_witnesses w ON w.event_seq=s.cursor LEFT JOIN operation_receipts r ON r.session_id=s.session AND r.operation_id=s.operation LEFT JOIN native_run_issuances i ON i.session_id=s.session AND i.run_id=s.run AND i.sequence=s.issuance";
const DATA: &str = "WITH scope AS (SELECT CAST($1 AS TEXT) AS session,CAST($2 AS TEXT) AS run,CAST($3 AS TEXT) AS operation,CAST($4 AS BIGINT) AS cursor,CAST($5 AS BIGINT) AS issuance) SELECT CASE WHEN length(CAST(w.data_json AS BLOB))<=8192 AND w.session_id=s.session AND w.run_id=s.run AND w.operation_id=s.operation AND w.kind='phase' THEN w.data_json END AS witness,CASE WHEN length(CAST(r.data_json AS BLOB))<=8192 AND r.kind='phase' AND r.event_seq=s.cursor THEN r.data_json END AS receipt,CASE WHEN length(CAST(i.data_json AS BLOB))<=2048 THEN i.data_json END AS issuance FROM scope s JOIN native_parent_witnesses w ON w.event_seq=s.cursor LEFT JOIN operation_receipts r ON r.session_id=s.session AND r.operation_id=s.operation LEFT JOIN native_run_issuances i ON i.session_id=s.session AND i.run_id=s.run AND i.sequence=s.issuance";
