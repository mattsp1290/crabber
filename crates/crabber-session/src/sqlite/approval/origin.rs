//! Original suspension anchors presentation and Store-clock deadlines.
use super::{Error, approval, sql};
use crabber_core::{EventRecord, Run};
use sqlx::{Row, Sqlite, Transaction};

pub(in super::super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
) -> Result<Run, Error> {
    let suspension = super::receipt(tx, &record.session_id, &record.batch.id)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let key = approval::derived_id("suspend-attempt", &record.batch.id);
    let native = super::super::attempt::records::receipt(tx, &record.session_id, &key)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let row = sqlx::query("SELECT CASE WHEN length(CAST(data AS BLOB))<=4194304 THEN data END AS data FROM runs WHERE session_id=$1 AND id=$2").bind(&record.session_id.0).bind(&record.run_id.0).fetch_one(&mut **tx).await.map_err(super::super::db).map_err(sql::unknown)?;
    let run = serde_json::from_str::<Run>(
        &row.get::<Option<String>, _>("data")
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    let audit = event(tx, record, suspension.event_cursor.0, false).await?;
    let native_audit = event(tx, record, native.event_cursor.0, false).await?;
    let source = event(tx, record, native.event_cursor.0, true).await?;
    record.verify_origin(&run, &suspension, &audit, &native, &native_audit, &source)?;
    Ok(run)
}
async fn event(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
    cursor: u64,
    previous: bool,
) -> Result<EventRecord, Error> {
    let query = if previous {
        "SELECT seq,CASE WHEN length(CAST(run_id AS BLOB))<=128 THEN run_id END AS run_id,CASE WHEN length(CAST(data AS BLOB))<=65536 THEN data END AS data FROM events WHERE session_id=$1 AND seq<$2 ORDER BY seq DESC LIMIT 1"
    } else {
        "SELECT seq,CASE WHEN length(CAST(run_id AS BLOB))<=128 THEN run_id END AS run_id,CASE WHEN length(CAST(data AS BLOB))<=65536 THEN data END AS data FROM events WHERE session_id=$1 AND seq=$2"
    };
    let row = sqlx::query(query)
        .bind(&record.session_id.0)
        .bind(i64::try_from(cursor).map_err(|_| Error::UnknownStoreOutcome)?)
        .fetch_optional(&mut **tx)
        .await
        .map_err(super::super::db)
        .map_err(sql::unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let mut event = serde_json::from_str::<EventRecord>(
        &row.get::<Option<String>, _>("data")
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    let cursor = crabber_core::EventCursor(
        u64::try_from(row.get::<i64, _>("seq")).map_err(|_| Error::UnknownStoreOutcome)?,
    );
    if event.cursor.is_some_and(|saved| saved != cursor) {
        return Err(Error::UnknownStoreOutcome);
    }
    event.cursor = Some(cursor);
    if row.get::<Option<String>, _>("run_id").as_deref() != Some(&record.run_id.0) {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(event)
}
