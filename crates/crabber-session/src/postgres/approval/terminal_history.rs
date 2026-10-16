//! Fresh termination proof only; bounded source selection precedes private carrier decoding.
use super::Error;
use crate::{PhaseReceipt, phase::PhaseRecord};
use crabber_core::{ContinuationId, EventCursor, EventRecord, Run, ToolCallRecord};
use sqlx::{Postgres, Row, Transaction};
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}

pub(super) async fn validate(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    old: &PhaseRecord,
    call: &ToolCallRecord,
) -> Result<(), Error> {
    let key: Option<String> = sqlx::query_scalar(r"SELECT CASE WHEN octet_length(continuation_id)<=128 THEN continuation_id END FROM tool_continuation_phases WHERE run_id=$1 AND phase_id=$2").bind(&run.id.0).bind(&old.snapshot.target.id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let origin = super::super::tool_continuation::read::load(
        tx,
        &run.session_id,
        &run.id,
        Some(&ContinuationId(key.ok_or(Error::UnknownStoreOutcome)?)),
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::UnknownStoreOutcome)?;
    let latest = super::super::phase::latest_host_phase(tx, &run.id, &call.id)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let next = super::super::phase::load_owned(tx, &run.id, &latest.target.id)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if next.snapshot != latest {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::approval::terminal_history::origin(run, old, &next, &origin, call)?;
    let current = source(tx, run, call, false).await?;
    let terminal = source(tx, run, call, true).await?;
    let row = sqlx::query(r#"WITH previous AS (SELECT seq,data FROM events WHERE session_id=$1 AND run_id=$2 AND seq<$3 ORDER BY seq DESC LIMIT 1), candidate AS (SELECT seq,CASE WHEN data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(data->>'record', ',"payload":',1) ~ ',"kind":"tool_call_settled"$' THEN data->>'record' END ELSE CASE WHEN data->>'kind'='tool_call_settled' THEN data::text END END AS record FROM previous) SELECT seq,CASE WHEN octet_length(record)<=4198400 THEN record END AS event FROM candidate"#).bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(terminal.0.event_cursor.0).map_err(unknown)?).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let settled = event(&row)?;
    crate::approval::terminal_history::result(
        run,
        &next,
        call,
        (&current.0, &current.1),
        (&terminal.0, &terminal.1),
        &settled,
    )
}
async fn source(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    call: &ToolCallRecord,
    terminal: bool,
) -> Result<(PhaseReceipt, EventRecord), Error> {
    let row = sqlx::query(r#"WITH decoded AS (SELECT e.seq,e.data,(CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":',1) ~ ',"kind":"tool_phase_changed"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END) AS d FROM events e WHERE e.session_id=$1 AND e.run_id=$2), latest AS (SELECT * FROM decoded WHERE d #>> '{kind}'='tool_phase_changed' AND d #>> '{payload,phase,target,call_id}'=$3 AND (NOT $4 OR d #>> '{payload,operation}' IN ('complete','resolve_success','resolve_failure')) ORDER BY seq DESC LIMIT 1) SELECT l.seq,CASE WHEN octet_length(l.data::text)<=65536 THEN l.data::text END AS event,CASE WHEN octet_length(r.data_json)<=8192 AND r.kind='phase' AND r.event_seq=l.seq AND r.semantic_digest=l.d #>> '{payload,semantic_digest}' THEN r.data_json END AS receipt FROM latest l LEFT JOIN operation_receipts r ON r.session_id=$1 AND r.operation_id=l.d #>> '{correlation}' "#).bind(&run.session_id.0).bind(&run.id.0).bind(&call.id.0).bind(terminal).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let raw: Option<String> = row.try_get("receipt").map_err(unknown)?;
    let receipt = serde_json::from_str(&raw.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?;
    Ok((receipt, event(&row)?))
}
fn event(row: &sqlx::postgres::PgRow) -> Result<EventRecord, Error> {
    let raw: Option<String> = row.try_get("event").map_err(unknown)?;
    let raw = raw.ok_or(Error::UnknownStoreOutcome)?;
    let value: serde_json::Value = serde_json::from_str(&raw).map_err(unknown)?;
    let mut event = crate::public_state::decode_event(sqlx::types::Json(value)).map_err(unknown)?;
    let cursor = EventCursor(
        u64::try_from(row.try_get::<i64, _>("seq").map_err(unknown)?).map_err(unknown)?,
    );
    if event.cursor.is_some_and(|v| v != cursor) {
        return Err(Error::UnknownStoreOutcome);
    }
    event.cursor = Some(cursor);
    Ok(event)
}
