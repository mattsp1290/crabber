//! Native Begin witnesses bound to independent Run fence issuance.
#[path = "native_parent/run.rs"]
mod run;
use crate::native_parent::{Error, Kind, Witness};
use crabber_core::{EventCursor, EventRecord, Run};
pub(super) use run::change as record_run;
use sqlx::{Row, Sqlite, Transaction};

pub(super) async fn record_attempt(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &crate::AttemptMutation,
    attempt: &crate::attempt::AttemptRecord,
    receipt: &crate::AttemptReceipt,
) -> Result<(), crate::AttemptError> {
    if crate::SQLITE_SCHEMA_VERSION >= 9
        && matches!(request.command, crate::AttemptCommand::Begin { .. })
    {
        let Some(issuance) = run::latest(tx, run)
            .await
            .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?
        else {
            return Ok(());
        };
        let witness = Witness::attempt(run, attempt, receipt, &issuance)
            .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
        save(tx, &witness)
            .await
            .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
    }
    Ok(())
}
pub(super) async fn record_phase(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    phase: &crate::phase::PhaseRecord,
    receipt: &crate::PhaseReceipt,
) -> Result<(), crate::PhaseError> {
    if crate::SQLITE_SCHEMA_VERSION >= 9 && receipt.operation == crate::PhaseOperation::Begin {
        let attempt = super::attempt::load_owned(tx, &run.id, &phase.snapshot.target.attempt.id)
            .await
            .map_err(|_| crate::PhaseError::UnknownStoreOutcome)?
            .ok_or(crate::PhaseError::UnknownStoreOutcome)?;
        let Some(issuance) = run::latest(tx, run)
            .await
            .map_err(|_| crate::PhaseError::UnknownStoreOutcome)?
        else {
            return Ok(());
        };
        let witness = Witness::phase(run, &attempt, phase, receipt, &issuance)
            .map_err(|_| crate::PhaseError::UnknownStoreOutcome)?;
        save(tx, &witness)
            .await
            .map_err(|_| crate::PhaseError::UnknownStoreOutcome)?;
    }
    Ok(())
}
async fn save(tx: &mut Transaction<'_, Sqlite>, witness: &Witness) -> Result<(), Error> {
    witness.validate()?;
    sqlx::query("INSERT INTO native_parent_witnesses(event_seq,session_id,run_id,kind,operation_id,data_json) VALUES(?1,?2,?3,?4,?5,?6)")
        .bind(i64::try_from(witness.source.event_cursor.0).map_err(|_| Error)?)
        .bind(&witness.source.session_id.0).bind(&witness.source.run_id.0)
        .bind(kind(witness.source.kind)).bind(&witness.source.operation_id.0)
        .bind(serde_json::to_string(witness).map_err(|_| Error)?)
        .execute(&mut **tx).await.map_err(|_| Error)?;
    Ok(())
}
fn kind(kind: Kind) -> &'static str {
    match kind {
        Kind::Attempt => "attempt",
        Kind::Phase => "phase",
    }
}

pub(super) async fn verify_attempt(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    attempt: &crate::attempt::AttemptRecord,
) -> Result<Witness, Error> {
    let row = source(tx, run, Kind::Attempt, None).await?;
    let receipt: crate::AttemptReceipt = decode(&row, "receipt")?;
    let event = event(&row)?;
    let witness: Witness = decode(&row, "witness")?;
    let issuance = run::latest(tx, run).await?.ok_or(Error)?;
    witness.verify_attempt(run, attempt, &receipt, &event, &issuance)?;
    Ok(witness)
}
pub(super) async fn verify_phase(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    attempt: &crate::attempt::AttemptRecord,
    phase: &crate::phase::PhaseRecord,
) -> Result<Witness, Error> {
    verify_attempt(tx, run, attempt).await?;
    let row = source(tx, run, Kind::Phase, Some(&phase.snapshot.target.call_id.0)).await?;
    let receipt: crate::PhaseReceipt = decode(&row, "receipt")?;
    let event = event(&row)?;
    let witness: Witness = decode(&row, "witness")?;
    let issuance = run::latest(tx, run).await?.ok_or(Error)?;
    witness.verify_phase(run, attempt, phase, &receipt, &event, &issuance)?;
    Ok(witness)
}
async fn source(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    kind_: Kind,
    call: Option<&str>,
) -> Result<sqlx::sqlite::SqliteRow, Error> {
    if crate::SQLITE_SCHEMA_VERSION < 9 {
        return Err(Error);
    }
    sqlx::query(r"WITH latest AS (SELECT e.seq,e.data FROM events e WHERE e.session_id=?1 AND e.run_id=?2 AND json_extract(e.data,'$.kind')=?3 AND (?4 IS NULL OR json_extract(e.data,'$.payload.phase.target.call_id')=?4) ORDER BY e.seq DESC LIMIT 1) SELECT l.seq,CASE WHEN length(CAST(l.data AS BLOB))<=65536 THEN l.data END AS event,CASE WHEN length(CAST(r.data_json AS BLOB))<=8192 AND r.kind=?5 AND r.event_seq=l.seq THEN r.data_json END AS receipt,CASE WHEN length(CAST(w.data_json AS BLOB))<=8192 AND w.session_id=?1 AND w.run_id=?2 AND w.kind=?5 AND w.operation_id=json_extract(l.data,'$.correlation') THEN w.data_json END AS witness FROM latest l LEFT JOIN operation_receipts r ON r.session_id=?1 AND r.operation_id=json_extract(l.data,'$.correlation') LEFT JOIN native_parent_witnesses w ON w.event_seq=l.seq")
        .bind(&run.session_id.0).bind(&run.id.0)
        .bind(match kind_ { Kind::Attempt => "execution_attempt_changed", Kind::Phase => "tool_phase_changed" })
        .bind(call).bind(kind(kind_)).fetch_optional(&mut **tx).await.map_err(|_| Error)?.ok_or(Error)
}
fn decode<T: serde::de::DeserializeOwned>(
    row: &sqlx::sqlite::SqliteRow,
    field: &str,
) -> Result<T, Error> {
    let value: String = row
        .try_get::<Option<String>, _>(field)
        .map_err(|_| Error)?
        .ok_or(Error)?;
    serde_json::from_str(&value).map_err(|_| Error)
}
fn event(row: &sqlx::sqlite::SqliteRow) -> Result<EventRecord, Error> {
    let mut event: EventRecord = decode(row, "event")?;
    let cursor = EventCursor(
        u64::try_from(row.try_get::<i64, _>("seq").map_err(|_| Error)?).map_err(|_| Error)?,
    );
    if event.cursor.is_some_and(|value| value != cursor) {
        return Err(Error);
    }
    event.cursor = Some(cursor);
    Ok(event)
}
