use super::{
    ControlId, Error, Row, RunId, SessionId, Sqlite, ToolContinuationOutcome,
    ToolContinuationReceipt, ToolContinuationRecord, Transaction, domain, unknown,
};
use crabber_core::{ContinuationId, EventCursor, EventRecord};
const RECORD: &str = r"SELECT CASE WHEN length(CAST(json_extract(s.data, '$.workspace_id') AS BLOB))<=512 THEN json_extract(s.data, '$.workspace_id') END AS workspace,
 CASE WHEN length(CAST(c.id AS BLOB))<=128 THEN c.id END AS id,c.generation,c.format_version AS format_version,
 CASE WHEN length(CAST(c.state AS BLOB))<=32 THEN c.state END AS state,
 CASE WHEN length(CAST(c.data_json AS BLOB))<=4194304 THEN c.data_json END AS data_json
 FROM runs r JOIN sessions s ON s.id=r.session_id LEFT JOIN tool_continuations c ON c.id=(SELECT id FROM tool_continuations WHERE session_id=$1 AND run_id=$2 AND (CAST($3 AS TEXT) IS NULL OR id=$3) ORDER BY generation DESC LIMIT 1) WHERE r.session_id=$1 AND r.id=$2";
const RECEIPT: &str = r"SELECT CASE WHEN o.kind='tool_continuation' THEN 'tool_continuation' ELSE 'foreign' END AS kind,o.event_seq,
 CASE WHEN length(CAST(o.semantic_digest AS BLOB))<=64 THEN o.semantic_digest END AS digest,
 CASE WHEN length(CAST(o.data_json AS BLOB))<=32768 THEN o.data_json END AS data_json,
 CASE WHEN length(CAST(e.run_id AS BLOB))<=128 THEN e.run_id END AS event_run_id,
 CASE WHEN o.kind='tool_continuation' AND length(CAST(e.data AS BLOB))<=65536 THEN e.data END AS event
 FROM operation_receipts o LEFT JOIN events e ON e.session_id=o.session_id AND e.seq=o.event_seq WHERE o.session_id=$1 AND o.operation_id=$2";
pub(in super::super) async fn receipt(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<(ToolContinuationReceipt, EventRecord)>, Error> {
    crate::attempt::validate_identifier(&id.0).map_err(|_| Error::InvalidRequest)?;
    let Some(row) = sqlx::query(RECEIPT)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
    else {
        return Ok(None);
    };
    if row.get::<String, _>("kind") != "tool_continuation" {
        return Err(Error::SemanticConflict);
    }
    let saved: ToolContinuationReceipt = serde_json::from_str(
        &row.get::<Option<String>, _>("data_json")
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    let mut event: EventRecord = serde_json::from_str(
        &row.get::<Option<String>, _>("event")
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    let seq: i64 = row
        .get::<Option<i64>, _>("event_seq")
        .ok_or(Error::UnknownStoreOutcome)?;
    event.cursor = Some(EventCursor(u64::try_from(seq).map_err(unknown)?));
    if saved.session_id != *session
        || saved.operation_id != *id
        || row.get::<Option<String>, _>("digest").as_deref() != Some(saved.semantic_digest.as_str())
        || row.get::<Option<String>, _>("event_run_id").as_deref() != Some(saved.run_id.0.as_str())
    {
        return Err(Error::UnknownStoreOutcome);
    }
    domain::verify_receipt(&saved, &event)?;
    Ok(Some((saved, event)))
}
pub(in super::super) async fn load(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    id: Option<&ContinuationId>,
) -> Result<Option<ToolContinuationRecord>, Error> {
    let record = origin(tx, session, run, id).await?;
    if let Some(record) = &record {
        super::current::verify(tx, record).await?;
    }
    Ok(record)
}
pub(in super::super) async fn origin(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    id: Option<&ContinuationId>,
) -> Result<Option<ToolContinuationRecord>, Error> {
    let row = sqlx::query(RECORD)
        .bind(&session.0)
        .bind(&run.0)
        .bind(id.map(|id| id.0.as_str()))
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    let Some(key) = row.get::<Option<String>, _>("id") else {
        return if row.get::<Option<i64>, _>("generation").is_some() {
            Err(Error::UnknownStoreOutcome)
        } else {
            Ok(None)
        };
    };
    let record: ToolContinuationRecord = serde_json::from_str(
        &row.get::<Option<String>, _>("data_json")
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    record.validate().map_err(unknown)?;
    if record.session_id != *session
        || record.run_id != *run
        || record.batch.continuation.id.0 != key
        || row.get::<Option<i64>, _>("generation")
            != Some(i64::try_from(record.batch.continuation.generation.get()).map_err(unknown)?)
        || row.get::<Option<i64>, _>("format_version") != Some(1)
        || row.get::<Option<String>, _>("state").as_deref()
            != Some(super::write::state(record.state))
        || row.get::<Option<String>, _>("workspace").as_deref()
            != Some(record.workspace_id.as_str())
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let binding = super::super::execution_binding::read(tx, session, run)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if binding.capsule.request.workspace_id != record.workspace_id {
        return Err(Error::UnknownStoreOutcome);
    }
    verify(tx, &record).await?;
    Box::pin(super::super::frontend::verify_batch(tx, &record)).await?;
    Ok(Some(record))
}
async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    let (origin, event) = receipt(tx, &record.session_id, &record.batch.id)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    if origin.run_id != record.run_id
        || origin.semantic_digest
            != record
                .batch
                .fingerprint(&record.session_id, &record.run_id)?
        || origin.outcome
            != (ToolContinuationOutcome::Suspended {
                continuation: record.batch.continuation.clone(),
            })
        || record.handoff.as_deref()
            != crate::continuation_handoff::Handoff::from_event(&event)
                .map_err(unknown)?
                .as_ref()
        || event.created_at != record.created_at
    {
        return Err(Error::UnknownStoreOutcome);
    }
    for question in &record.questions {
        if let Some(answer) = &question.answer {
            let (accepted, _) = receipt(tx, &record.session_id, &answer.id)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            if accepted.run_id != record.run_id
                || accepted.semantic_digest != answer.fingerprint()?
                || accepted.outcome
                    != (ToolContinuationOutcome::Answered {
                        continuation: record.batch.continuation.clone(),
                        interaction_id: question.view.id.clone(),
                        state: question.view.state,
                    })
            {
                return Err(Error::UnknownStoreOutcome);
            }
        }
    }
    if let Some(claim) = &record.claim {
        let (accepted, _) = receipt(tx, &record.session_id, &claim.id)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        record.verify_claim_receipt(&accepted)?;
    }
    if let Some(recovery) = &record.recovery {
        let (accepted, _) = receipt(tx, &record.session_id, &recovery.id)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        recovery.verify_receipt(&accepted).map_err(unknown)?;
    }
    if let Some(recovery) = &record.frontend_recovery {
        let (accepted, _) = receipt(tx, &record.session_id, &recovery.id)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        recovery.verify_receipt(&accepted).map_err(unknown)?;
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM tool_continuation_interactions WHERE continuation_id=$1",
    )
    .bind(&record.batch.continuation.id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if usize::try_from(count).ok() != Some(record.questions.len()) {
        return Err(Error::UnknownStoreOutcome);
    }
    for question in &record.questions {
        let exact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_continuation_interactions WHERE continuation_id=$1 AND run_id=$2 AND id=$3 AND call_id=$4 AND phase_id=$5)").bind(&record.batch.continuation.id.0).bind(&record.run_id.0).bind(&question.view.id.0).bind(&question.view.call_id.0).bind(&question.view.phase.id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
        if !exact {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    let phases: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM tool_continuation_phases WHERE continuation_id=$1",
    )
    .bind(&record.batch.continuation.id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if usize::try_from(phases).ok() != Some(record.batch.phases.len()) {
        return Err(Error::UnknownStoreOutcome);
    }
    for phase in &record.batch.phases {
        let exact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_continuation_phases WHERE continuation_id=$1 AND run_id=$2 AND phase_id=$3)").bind(&record.batch.continuation.id.0).bind(&record.run_id.0).bind(&phase.previous.id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
        if !exact {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    Ok(())
}
