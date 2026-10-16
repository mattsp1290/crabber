//! Bounded immutable result evidence for an exact native terminal phase.
use super::Error;
use crate::{PhaseReceipt, phase::PhaseRecord};
use crabber_core::{EventCursor, EventRecord, Run, ToolCallRecord};
use sqlx::{Row, Sqlite, Transaction};
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
pub(in super::super) async fn validate(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    next: &PhaseRecord,
    call: &ToolCallRecord,
) -> Result<(), Error> {
    let current = source(tx, run, call, false).await?;
    let terminal = source(tx, run, call, true).await?;
    let row = sqlx::query(r"SELECT seq,CASE WHEN length(CAST(data AS BLOB))<=?4 AND json_extract(data,'$.kind')='tool_call_settled' THEN data END AS event FROM events WHERE session_id=?1 AND run_id=?2 AND seq<?3 ORDER BY seq DESC LIMIT 1").bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(terminal.0.event_cursor.0).map_err(unknown)?).bind(i64::try_from(crate::phase::terminal_history::MAX_SETTLED_EVENT_BYTES).map_err(unknown)?).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let settled = event(&row)?;
    crate::phase::terminal_history::verify(
        run,
        next,
        call,
        (&current.0, &current.1),
        (&terminal.0, &terminal.1),
        &settled,
    )
}
async fn source(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    call: &ToolCallRecord,
    terminal: bool,
) -> Result<(PhaseReceipt, EventRecord), Error> {
    let row = sqlx::query(r"WITH latest AS (SELECT e.seq,e.data FROM events e WHERE e.session_id=?1 AND e.run_id=?2 AND json_extract(e.data,'$.kind')='tool_phase_changed' AND json_extract(e.data,'$.payload.phase.target.call_id')=?3 AND (NOT ?4 OR json_extract(e.data,'$.payload.operation') IN ('complete','resolve_success','resolve_failure')) ORDER BY e.seq DESC LIMIT 1) SELECT l.seq,CASE WHEN length(CAST(l.data AS BLOB))<=65536 THEN l.data END AS event,CASE WHEN length(CAST(r.data_json AS BLOB))<=8192 AND r.kind='phase' AND r.event_seq=l.seq AND r.semantic_digest=json_extract(l.data,'$.payload.semantic_digest') THEN r.data_json END AS receipt FROM latest l LEFT JOIN operation_receipts r ON r.session_id=?1 AND r.operation_id=json_extract(l.data,'$.correlation')").bind(&run.session_id.0).bind(&run.id.0).bind(&call.id.0).bind(terminal).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let raw: Option<String> = row.try_get("receipt").map_err(unknown)?;
    let receipt = serde_json::from_str(&raw.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?;
    Ok((receipt, event(&row)?))
}
fn event(row: &sqlx::sqlite::SqliteRow) -> Result<EventRecord, Error> {
    let raw: Option<String> = row.try_get("event").map_err(unknown)?;
    let raw = raw.ok_or(Error::UnknownStoreOutcome)?;
    let mut event: EventRecord = serde_json::from_str(&raw).map_err(unknown)?;
    let cursor = EventCursor(
        u64::try_from(row.try_get::<i64, _>("seq").map_err(unknown)?).map_err(unknown)?,
    );
    if event.cursor.is_some_and(|v| v != cursor) {
        return Err(Error::UnknownStoreOutcome);
    }
    event.cursor = Some(cursor);
    Ok(event)
}
