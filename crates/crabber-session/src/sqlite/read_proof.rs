//! Materialization accounting shared by every nested aggregate proof reader.
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use sqlx::Row;
use sqlx::{Sqlite, Transaction};
pub(super) async fn charge<'q>(
    tx: &mut Transaction<'_, Sqlite>,
    query: sqlx::query::QueryScalar<'q, Sqlite, Option<i64>, sqlx::sqlite::SqliteArguments<'q>>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if !proof.active() {
        return Ok(());
    }
    let bytes = query
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .flatten()
        .unwrap_or(0);
    proof.charge(usize::try_from(bytes).map_err(unknown)?)
}

/// Exact indexed source, or the immediately preceding session event at a native
/// publication boundary. Indexed and serialized scope/cursor must agree.
pub(super) async fn event(
    tx: &mut Transaction<'_, Sqlite>,
    session: &crabber_core::SessionId,
    cursor: crabber_core::EventCursor,
    previous: bool,
    proof: &mut Proof,
) -> Result<crabber_core::EventRecord, crate::public_session::v2::proof::ReadError<crate::StoreError>>
{
    use crate::public_session::v2::proof::ReadError;
    let comparison = if previous { "<" } else { "=" };
    let predicate = format!("session_id=$1 AND seq{comparison}$2 ORDER BY seq DESC LIMIT 1");
    let cost = format!(
        "SELECT CAST(length(CAST(data AS BLOB))+length(CAST(run_id AS BLOB)) AS BIGINT) FROM events WHERE {predicate}"
    );
    let seq = i64::try_from(cursor.0).map_err(|_| crate::StoreError::AdmissionConflict)?;
    charge(
        tx,
        sqlx::query_scalar(&cost).bind(&session.0).bind(seq),
        proof,
    )
    .await
    .map_err(ReadError::Budget)?;
    let query = format!(
        "SELECT seq,CASE WHEN length(CAST(run_id AS BLOB))<=128 THEN run_id END AS run_id,CASE WHEN length(CAST(data AS BLOB))<=65536 THEN data END AS data FROM events WHERE {predicate}"
    );
    let row = sqlx::query(&query)
        .bind(&session.0)
        .bind(seq)
        .fetch_optional(&mut **tx)
        .await
        .map_err(super::db)?
        .ok_or(crate::StoreError::NotFound)?;
    let text = row
        .try_get::<Option<String>, _>("data")
        .map_err(super::db)?
        .ok_or(crate::StoreError::AdmissionConflict)?;
    let actual = crabber_core::EventCursor(
        u64::try_from(row.try_get::<i64, _>("seq").map_err(super::db)?)
            .map_err(|_| crate::StoreError::AdmissionConflict)?,
    );
    let mut event: crabber_core::EventRecord =
        serde_json::from_str(&text).map_err(|_| crate::StoreError::AdmissionConflict)?;
    if event.cursor.is_some_and(|cursor| cursor != actual)
        || event.session_id != *session
        || row
            .try_get::<Option<String>, _>("run_id")
            .map_err(super::db)?
            .as_deref()
            != Some(&event.run_id.0)
    {
        return Err(crate::StoreError::AdmissionConflict.into());
    }
    event.cursor = Some(actual);
    Ok(event)
}
