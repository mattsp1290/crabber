//! Immutable event selection is independent of nullable private answer/claim fields.
use super::{Error, Row, Sqlite, ToolContinuationRecord, Transaction, domain, read, unknown};
use crabber_core::{ControlId, EventCursor, EventRecord};
pub(super) const CARRIER: &str = "e.data";
const BYTES: &str = "length(CAST(e.data AS BLOB))";
const DATA: &str = "e.data";

const RUN_STATUS: &str = "SELECT CASE WHEN length(CAST(status AS BLOB))<=32 THEN status END, CASE WHEN length(CAST(json_extract(data,'$.status') AS BLOB))<=32 THEN json_extract(data,'$.status') END, CASE WHEN json_type(data,'$.id')='text' AND length(CAST(json_extract(data,'$.id') AS BLOB))<=128 THEN json_extract(data,'$.id') END, CASE WHEN json_type(data,'$.session_id')='text' AND length(CAST(json_extract(data,'$.session_id') AS BLOB))<=512 THEN json_extract(data,'$.session_id') END FROM runs WHERE id=$1 AND session_id=$2";
pub(super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    let mut audits = domain::CurrentAudits::default();
    let events = select(tx, record, "CAST(json_extract({event},'$.payload.outcome.continuation.id') AS TEXT)=$3 AND CAST(json_extract({event},'$.payload.kind') AS TEXT)='tool_continuation' AND CAST(json_extract({event},'$.payload.outcome.operation') AS TEXT)<>'reclaimed_unstarted'", &record.batch.continuation.id.0, false, 67).await?;
    if events.len() > 66 {
        return Err(Error::UnknownStoreOutcome);
    }
    for event in events {
        insert(tx, record, &mut audits, &event).await?;
    }
    for event in select(tx, record, "CAST(json_extract({event},'$.payload.outcome.continuation.id') AS TEXT)=$3 AND CAST(json_extract({event},'$.payload.kind') AS TEXT)='tool_continuation' AND CAST(json_extract({event},'$.payload.outcome.operation') AS TEXT)='reclaimed_unstarted'", &record.batch.continuation.id.0, true, 1).await? {
        insert(tx, record, &mut audits, &event).await?;
    }
    let released = released(tx, record).await?;
    let barrier: Option<Option<String>> = sqlx::query_scalar("SELECT CASE WHEN length(CAST(continuation_id AS BLOB))<=128 THEN continuation_id END FROM tool_continuation_barriers WHERE session_id=$1")
        .bind(&record.session_id.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    if matches!(barrier, Some(None))
        || (barrier.flatten().as_deref() == Some(record.batch.continuation.id.0.as_str()))
            != (record.state != crate::ToolContinuationState::Released)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    audits.verify(record, released)
}
async fn insert(
    tx: &mut Transaction<'_, Sqlite>,
    record: &ToolContinuationRecord,
    audits: &mut domain::CurrentAudits,
    event: &EventRecord,
) -> Result<(), Error> {
    let key = domain::audit_key(event, record)?.ok_or(Error::UnknownStoreOutcome)?;
    let (receipt, _) = read::receipt(tx, &record.session_id, &key)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    if event.cursor != Some(receipt.event_cursor) {
        return Err(Error::UnknownStoreOutcome);
    }
    audits.insert(receipt, record)
}
async fn released(
    tx: &mut Transaction<'_, Sqlite>,
    record: &ToolContinuationRecord,
) -> Result<bool, Error> {
    let native = select(tx, record, "CAST(json_extract({event},'$.payload.attempt.target.id') AS TEXT)=$3 AND CAST(json_extract({event},'$.payload.version') AS TEXT) IN ('2','3') AND CAST(json_extract({event},'$.payload.attempt.state') AS TEXT) IN ('Completed','Failed','Interrupted')", &record.batch.next_attempt.id.0, false, 2).await?;
    if native.len() > 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    for event in &native {
        if !domain::terminal_candidate(event, record) {
            return Err(Error::UnknownStoreOutcome);
        }
        let key = ControlId::from(
            event
                .correlation
                .as_deref()
                .ok_or(Error::UnknownStoreOutcome)?,
        );
        let receipt = super::super::attempt::host_receipt(tx, &record.session_id, &key)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        let (latest, _) = super::super::attempt::load_host_attempt(tx, &record.run_id, None, false)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        let expected = match receipt.attempt.state {
            crate::AttemptState::Completed => "completed",
            crate::AttemptState::Failed => "failed",
            crate::AttemptState::Interrupted => "interrupted",
            _ => return Err(Error::UnknownStoreOutcome),
        };
        let row: (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = sqlx::query_as(RUN_STATUS)
            .bind(&record.run_id.0)
            .bind(&record.session_id.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
        if event.cursor != Some(receipt.event_cursor)
            || latest.snapshot != receipt.attempt
            || row.0.as_deref() != Some(expected)
            || row.1.as_deref() != Some(expected)
            || row.2.as_deref() != Some(record.run_id.0.as_str())
            || row.3.as_deref() != Some(record.session_id.0.as_str())
        {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    let generation = record
        .batch
        .continuation
        .generation
        .next()
        .map_err(unknown)?
        .get()
        .to_string();
    let successor = select(tx, record, "CAST(json_extract({event},'$.payload.outcome.continuation.generation') AS TEXT)=$3 AND CAST(json_extract({event},'$.payload.kind') AS TEXT)='tool_continuation' AND CAST(json_extract({event},'$.payload.outcome.operation') AS TEXT)='suspended'", &generation, false, 2).await?;
    if native.len() + successor.len() > 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    for event in &successor {
        let target: crate::ContinuationTarget = serde_json::from_value(
            event
                .payload
                .pointer("/outcome/continuation")
                .cloned()
                .ok_or(Error::UnknownStoreOutcome)?,
        )
        .map_err(unknown)?;
        let successor = read::origin(tx, &record.session_id, &record.run_id, Some(&target.id))
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let (receipt, _) = read::receipt(tx, &record.session_id, &successor.batch.id)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        if successor.batch.continuation != target
            || successor.batch.attempt != record.batch.next_attempt
            || event.cursor != Some(receipt.event_cursor)
        {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    Ok(!native.is_empty() || !successor.is_empty())
}
async fn select(
    tx: &mut Transaction<'_, Sqlite>,
    record: &ToolContinuationRecord,
    predicate: &str,
    value: &str,
    descending: bool,
    limit: usize,
) -> Result<Vec<EventRecord>, Error> {
    let predicate = predicate.replace("{event}", CARRIER);
    let order = if descending { "DESC" } else { "ASC" };
    let query = format!(
        "SELECT e.seq, CASE WHEN {BYTES}<=65536 THEN {DATA} END AS event FROM events e WHERE e.session_id=$1 AND e.run_id=$2 AND ({predicate}) ORDER BY e.seq {order} LIMIT {limit}"
    );
    let rows = sqlx::query(&query)
        .bind(&record.session_id.0)
        .bind(&record.run_id.0)
        .bind(value)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    rows.into_iter()
        .map(|row| {
            let text = row
                .get::<Option<String>, _>("event")
                .ok_or(Error::UnknownStoreOutcome)?;
            let mut event = serde_json::from_str::<EventRecord>(&text).map_err(unknown)?;
            event.cursor = Some(EventCursor(
                u64::try_from(row.get::<i64, _>("seq")).map_err(unknown)?,
            ));
            Ok(event)
        })
        .collect()
}
