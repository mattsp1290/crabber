//! Only parent-proof metadata crosses this read, never checkpoints or errors.
use super::{Error, SessionId, Sqlite, Transaction, unknown};
use crabber_core::{Run, RunId};
use sqlx::Row;
pub(in crate::sqlite) async fn load(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &RunId,
) -> Result<Run, Error> {
    let row=sqlx::query("SELECT CASE WHEN length(CAST(json_remove(data,'$.checkpoint','$.error') AS BLOB))<=16384 THEN json_remove(data,'$.checkpoint','$.error') END AS data,lease_until,(json_extract(data,'$.id')=id AND json_extract(data,'$.session_id')=session_id AND json_extract(data,'$.status')=status AND json_extract(data,'$.claim_token')=claim_token) AS valid FROM runs WHERE session_id=$1 AND id=$2").bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::NotFound)?;
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
        || super::super::records::micros(run.lease_until)
            != row.try_get::<i64, _>("lease_until").map_err(unknown)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(run)
}
