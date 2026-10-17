//! Immutable event selection is independent of nullable private answer/claim fields.
use super::{Error, Row, Sqlite, ToolContinuationRecord, Transaction, domain, read, unknown};
use crate::public_session::v2::proof::{Proof, ToolReadError};
use crabber_core::{ControlId, EventCursor, EventRecord};
pub(in super::super) const CARRIER: &str = "e.data";
const BYTES: &str = "length(CAST(e.data AS BLOB))";
const DATA: &str = "e.data";

const RUN_STATUS: &str = "SELECT CASE WHEN length(CAST(status AS BLOB))<=32 THEN status END, CASE WHEN length(CAST(json_extract(data,'$.status') AS BLOB))<=32 THEN json_extract(data,'$.status') END, CASE WHEN json_type(data,'$.id')='text' AND length(CAST(json_extract(data,'$.id') AS BLOB))<=128 THEN json_extract(data,'$.id') END, CASE WHEN json_type(data,'$.session_id')='text' AND length(CAST(json_extract(data,'$.session_id') AS BLOB))<=512 THEN json_extract(data,'$.session_id') END FROM runs WHERE id=$1 AND session_id=$2";
pub(super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    verify_with_proof(tx, record, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) async fn verify_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), ToolReadError> {
    let scope = crate::continuation_input::Scope {
        domain: crate::continuation_input::Domain::Tool,
        session: record.session_id.clone(),
        run: record.run_id.clone(),
        continuation: record.batch.continuation.clone(),
    };
    let inputs: Vec<_> = record
        .questions
        .iter()
        .filter_map(|q| q.answer.as_ref())
        .map(crate::continuation_input::Input::ToolAnswer)
        .collect();
    super::super::continuation_input::answers(tx, &scope, &inputs, proof)
        .await
        .map_err(|e| e.map_domain(|_| Error::UnknownStoreOutcome))?;
    let mut audits = domain::CurrentAudits::default();
    let events = select(tx, record, "CAST(json_extract({event},'$.payload.outcome.continuation.id') AS TEXT)=$3 AND CAST(json_extract({event},'$.payload.kind') AS TEXT)='tool_continuation' AND CAST(json_extract({event},'$.payload.outcome.operation') AS TEXT)NOT IN ('reclaimed_unstarted','frontend_reclaimed_unstarted')", &record.batch.continuation.id.0, false, 67, proof).await?;
    if events.len() > 66 {
        return Err(Error::UnknownStoreOutcome.into());
    }
    for event in events {
        insert(tx, record, &mut audits, &event, proof).await?;
    }
    for event in select(tx, record, "CAST(json_extract({event},'$.payload.outcome.continuation.id') AS TEXT)=$3 AND CAST(json_extract({event},'$.payload.kind') AS TEXT)='tool_continuation' AND CAST(json_extract({event},'$.payload.outcome.operation') AS TEXT)IN ('reclaimed_unstarted','frontend_reclaimed_unstarted')", &record.batch.continuation.id.0, true, 1, proof).await? {
        insert(tx, record, &mut audits, &event, proof).await?;
    }
    let cross = super::super::continuation_handoff::verify_with_proof(
        tx,
        &record.session_id,
        &record.run_id,
        &crate::continuation_handoff::ReadTarget {
            domain: crate::continuation_handoff::Domain::Tool,
            target: &record.batch.continuation,
            parent: &record.batch.next_attempt,
            plan: &record.batch.expected_plan,
        },
        proof,
    )
    .await?;
    if let Some(handoff) = &cross {
        let source =
            super::super::continuation_handoff::tool_source_with_proof(tx, record, proof).await?;
        if handoff.predecessor != source {
            return Err(Error::UnknownStoreOutcome.into());
        }
    }
    let native_released = cross.is_none() && released(tx, record, proof).await?;
    if cross.is_some() && native_released {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let released = cross.is_some() || native_released;
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(BARRIER_COST).bind(&record.session_id.0),
        proof,
    )
    .await
    .map_err(ToolReadError::Budget)?;
    let barrier: Option<Option<String>> = sqlx::query_scalar("SELECT CASE WHEN length(CAST(continuation_id AS BLOB))<=128 THEN continuation_id END FROM tool_continuation_barriers WHERE session_id=$1")
        .bind(&record.session_id.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    if matches!(barrier, Some(None))
        || (barrier.flatten().as_deref() == Some(record.batch.continuation.id.0.as_str()))
            != (record.state != crate::ToolContinuationState::Released)
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    audits.verify(record, released)?;
    super::cancel::verify_with_proof(tx, record, proof).await
}
async fn insert(
    tx: &mut Transaction<'_, Sqlite>,
    record: &ToolContinuationRecord,
    audits: &mut domain::CurrentAudits,
    event: &EventRecord,
    proof: &mut Proof,
) -> Result<(), ToolReadError> {
    let key = domain::audit_key(event, record)?.ok_or(Error::UnknownStoreOutcome)?;
    let (receipt, _) = read::receipt_with_proof(tx, &record.session_id, &key, proof)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    if event.cursor != Some(receipt.event_cursor) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(audits.insert(receipt, record)?)
}
async fn released(
    tx: &mut Transaction<'_, Sqlite>,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<bool, ToolReadError> {
    let native = select(tx, record, "CAST(json_extract({event},'$.payload.attempt.target.id') AS TEXT)=$3 AND CAST(json_extract({event},'$.payload.version') AS TEXT) IN ('2','3','4','5') AND CAST(json_extract({event},'$.payload.attempt.state') AS TEXT) IN ('Completed','Failed','Interrupted')", &record.batch.next_attempt.id.0, false, 2, proof).await?;
    if native.len() > 1 {
        return Err(Error::UnknownStoreOutcome.into());
    }
    for event in &native {
        verify_native_release(tx, event, record, proof).await?;
    }

    let generation = record
        .batch
        .continuation
        .generation
        .next()
        .map_err(unknown)?
        .get()
        .to_string();
    let successor = select(tx, record, "CAST(json_extract({event},'$.payload.outcome.continuation.generation') AS TEXT)=$3 AND CAST(json_extract({event},'$.payload.kind') AS TEXT)='tool_continuation' AND CAST(json_extract({event},'$.payload.outcome.operation') AS TEXT)='suspended'", &generation, false, 2, proof).await?;
    if native.len() + successor.len() > 1 {
        return Err(Error::UnknownStoreOutcome.into());
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
        let successor = read::origin_with_proof(
            tx,
            &record.session_id,
            &record.run_id,
            Some(&target.id),
            proof,
        )
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
        let (receipt, _) =
            read::receipt_with_proof(tx, &record.session_id, &successor.batch.id, proof)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
        if successor.batch.continuation != target
            || successor.batch.attempt != record.batch.next_attempt
            || event.cursor != Some(receipt.event_cursor)
        {
            return Err(Error::UnknownStoreOutcome.into());
        }
    }
    Ok(!native.is_empty() || !successor.is_empty())
}
async fn verify_native_release(
    tx: &mut Transaction<'_, Sqlite>,
    event: &EventRecord,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), ToolReadError> {
    if !domain::terminal_candidate(event, record) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let key = ControlId::from(
        event
            .correlation
            .as_deref()
            .ok_or(Error::UnknownStoreOutcome)?,
    );
    let receipt =
        super::super::attempt::host_receipt_with_proof(tx, &record.session_id, &key, proof)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
    let (latest, _) =
        super::super::attempt::load_host_attempt_with_proof(tx, &record.run_id, None, proof)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
    let expected = match receipt.attempt.state {
        crate::AttemptState::Completed => "completed",
        crate::AttemptState::Failed => "failed",
        crate::AttemptState::Interrupted => "interrupted",
        _ => return Err(Error::UnknownStoreOutcome.into()),
    };
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(STATUS_COST)
            .bind(&record.run_id.0)
            .bind(&record.session_id.0),
        proof,
    )
    .await
    .map_err(ToolReadError::Budget)?;
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
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(())
}
async fn select(
    tx: &mut Transaction<'_, Sqlite>,
    record: &ToolContinuationRecord,
    predicate: &str,
    value: &str,
    descending: bool,
    limit: usize,
    proof: &mut Proof,
) -> Result<Vec<EventRecord>, ToolReadError> {
    let predicate = predicate.replace("{event}", CARRIER);
    let order = if descending { "DESC" } else { "ASC" };
    let query = format!(
        "SELECT e.seq, CASE WHEN {BYTES}<=65536 THEN {DATA} END AS event FROM events e WHERE e.session_id=$1 AND e.run_id=$2 AND ({predicate}) ORDER BY e.seq {order} LIMIT {limit}"
    );
    let cost = format!(
        "SELECT CAST(COALESCE(SUM(bytes),0) AS BIGINT) FROM (SELECT {BYTES} AS bytes FROM events e WHERE e.session_id=$1 AND e.run_id=$2 AND ({predicate}) ORDER BY e.seq {order} LIMIT {limit}) bounded_tool_audits"
    );
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(&cost)
            .bind(&record.session_id.0)
            .bind(&record.run_id.0)
            .bind(value),
        proof,
    )
    .await
    .map_err(ToolReadError::Budget)?;
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

const BARRIER_COST: &str = r"SELECT CAST(length(CAST(continuation_id AS BLOB)) AS BIGINT) FROM tool_continuation_barriers WHERE session_id=$1";
const STATUS_COST: &str = r"SELECT CAST(COALESCE(length(CAST(status AS BLOB)),0)+COALESCE(length(CAST(json_extract(data,'$.status') AS BLOB)),0)+COALESCE(length(CAST(json_extract(data,'$.id') AS BLOB)),0)+COALESCE(length(CAST(json_extract(data,'$.session_id') AS BLOB)),0) AS BIGINT) FROM runs WHERE id=$1 AND session_id=$2";
