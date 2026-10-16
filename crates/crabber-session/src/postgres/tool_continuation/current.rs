//! Immutable event selection is independent of nullable private answer/claim fields.
use super::{Error, Postgres, Row, ToolContinuationRecord, Transaction, domain, read, unknown};
use crabber_core::{ControlId, EventCursor, EventRecord};
// Encoded records are emitted by the fixed EventRecord serializer: kind precedes
// payload. Inspect that canonical prefix without traversing unrelated user JSON;
// even PostgreSQL json (not just jsonb) de-escapes nested NUL during #>>.
pub(super) const CARRIER: &str = "(CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',\"payload\":', 1) ~ ',\"kind\":\"(continuation_changed|execution_attempt_changed)\"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END)";
const BYTES: &str = "octet_length(e.data::text)";
const DATA: &str = "e.data::text";

const RUN_STATUS: &str = "SELECT CASE WHEN octet_length(status)<=32 THEN status END, CASE WHEN octet_length(data->>'status')<=32 THEN data->>'status' END, CASE WHEN jsonb_typeof(data->'id')='string' AND octet_length(data->>'id')<=128 THEN data->>'id' END, CASE WHEN jsonb_typeof(data->'session_id')='string' AND octet_length(data->>'session_id')<=512 THEN data->>'session_id' END FROM runs WHERE id=$1 AND session_id=$2";
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    let mut audits = domain::CurrentAudits::default();
    let events = select(tx, record, "{event}#>>'{payload,outcome,continuation,id}'=$3 AND {event}#>>'{payload,kind}'='tool_continuation' AND {event}#>>'{payload,outcome,operation}'<>'reclaimed_unstarted'", &record.batch.continuation.id.0, false, 67).await?;
    if events.len() > 66 {
        return Err(Error::UnknownStoreOutcome);
    }
    for event in events {
        insert(tx, record, &mut audits, &event).await?;
    }
    for event in select(tx, record, "{event}#>>'{payload,outcome,continuation,id}'=$3 AND {event}#>>'{payload,kind}'='tool_continuation' AND {event}#>>'{payload,outcome,operation}'='reclaimed_unstarted'", &record.batch.continuation.id.0, true, 1).await? {
        insert(tx, record, &mut audits, &event).await?;
    }
    let cross = super::super::continuation_handoff::verify(
        tx,
        &record.session_id,
        &record.run_id,
        crate::continuation_handoff::Domain::Tool,
        &record.batch.continuation,
        &record.batch.next_attempt,
        &record.batch.expected_plan,
    )
    .await
    .map_err(unknown)?;
    if let Some(proof) = &cross {
        let source = super::super::continuation_handoff::tool_source(tx, record)
            .await
            .map_err(unknown)?;
        if proof.predecessor != source {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    let native_released = cross.is_none() && released(tx, record).await?;
    if cross.is_some() && native_released {
        return Err(Error::UnknownStoreOutcome);
    }
    let released = cross.is_some() || native_released;
    let barrier: Option<Option<String>> = sqlx::query_scalar("SELECT CASE WHEN octet_length(continuation_id)<=128 THEN continuation_id END FROM tool_continuation_barriers WHERE session_id=$1")
        .bind(&record.session_id.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    if matches!(barrier, Some(None))
        || (barrier.flatten().as_deref() == Some(record.batch.continuation.id.0.as_str()))
            != (record.state != crate::ToolContinuationState::Released)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    audits.verify(record, released)?;
    super::cancel::verify(tx, record).await
}
async fn insert(
    tx: &mut Transaction<'_, Postgres>,
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
    tx: &mut Transaction<'_, Postgres>,
    record: &ToolContinuationRecord,
) -> Result<bool, Error> {
    let native = select(tx, record, "{event}#>>'{payload,attempt,target,id}'=$3 AND {event}#>>'{payload,version}' IN ('2','3') AND {event}#>>'{payload,attempt,state}' IN ('Completed','Failed','Interrupted')", &record.batch.next_attempt.id.0, false, 2).await?;
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
    let successor = select(tx, record, "{event}#>>'{payload,outcome,continuation,generation}'=$3 AND {event}#>>'{payload,kind}'='tool_continuation' AND {event}#>>'{payload,outcome,operation}'='suspended'", &generation, false, 2).await?;
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
    tx: &mut Transaction<'_, Postgres>,
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
            let mut event = crate::public_state::decode_event(sqlx::types::Json(
                serde_json::from_str(&text).map_err(unknown)?,
            ))
            .map_err(unknown)?;
            event.cursor = Some(EventCursor(
                u64::try_from(row.get::<i64, _>("seq")).map_err(unknown)?,
            ));
            Ok(event)
        })
        .collect()
}
