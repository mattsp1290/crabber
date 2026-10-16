//! Only parent-proof metadata crosses this read, never checkpoints or errors.
use super::{Error, Postgres, SessionId, Transaction, unknown};
use crabber_core::{Run, RunId};
use sqlx::Row;
pub(super) async fn load(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &RunId,
) -> Result<Run, Error> {
    let row=sqlx::query("SELECT CASE WHEN octet_length((data-'checkpoint'-'error')::text)<=16384 THEN (data-'checkpoint'-'error')::text END AS data,lease_until,(data->>'id'=id AND data->>'session_id'=session_id AND data->>'status'=status AND data->>'claim_token'=claim_token) AS valid FROM runs WHERE session_id=$1 AND id=$2 FOR UPDATE").bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::NotFound)?;
    if row.try_get::<Option<bool>, _>("valid").map_err(unknown)? != Some(true) {
        return Err(Error::UnknownStoreOutcome);
    }
    let text = row
        .try_get::<Option<String>, _>("data")
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let run: Run = serde_json::from_str(&text).map_err(unknown)?;
    if run.session_id != *session
        || run.id != *id
        || super::super::micros(run.lease_until)
            != row.try_get::<i64, _>("lease_until").map_err(unknown)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(run)
}
