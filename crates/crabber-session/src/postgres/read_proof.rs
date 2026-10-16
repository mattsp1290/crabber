//! Materialization accounting shared by every nested aggregate proof reader.
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use sqlx::Row;
use sqlx::{Postgres, Transaction};
pub(super) async fn charge(
    tx: &mut Transaction<'_, Postgres>,
    query: sqlx::query::QueryScalar<'_, Postgres, Option<i64>, sqlx::postgres::PgArguments>,
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
    tx: &mut Transaction<'_, Postgres>,
    session: &crabber_core::SessionId,
    cursor: crabber_core::EventCursor,
    previous: bool,
    proof: &mut Proof,
) -> Result<crabber_core::EventRecord, crate::public_session::v2::proof::ReadError<crate::StoreError>>
{
    event_with_limit(tx, session, cursor, previous, 65536, proof).await
}

pub(super) async fn event_with_limit(
    tx: &mut Transaction<'_, Postgres>,
    session: &crabber_core::SessionId,
    cursor: crabber_core::EventCursor,
    previous: bool,
    maximum: usize,
    proof: &mut Proof,
) -> Result<crabber_core::EventRecord, crate::public_session::v2::proof::ReadError<crate::StoreError>>
{
    use crate::public_session::v2::proof::ReadError;
    let comparison = if previous { "<" } else { "=" };
    let predicate = format!("session_id=$1 AND seq{comparison}$2 ORDER BY seq DESC LIMIT 1");
    let cost = format!(
        "SELECT CAST(octet_length(data::text)+octet_length(run_id) AS BIGINT) FROM events WHERE {predicate}"
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
        "SELECT seq,CASE WHEN octet_length(run_id)<=128 THEN run_id END AS run_id,CASE WHEN octet_length(data::text)<={maximum} THEN data::text END AS data FROM events WHERE {predicate}"
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
        crate::public_state::decode_event(sqlx::types::Json(
            serde_json::from_str(&text).map_err(|_| crate::StoreError::AdmissionConflict)?,
        ))?;
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
