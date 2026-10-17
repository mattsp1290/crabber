use super::{
    ControlId, Error, InputFingerprint, OffsetDateTime, Postgres, Run, ToolContinuationOutcome,
    ToolContinuationReceipt, ToolContinuationRecord, ToolContinuationState, Transaction, domain,
    revision, unknown,
};
use crabber_core::EventCursor;
pub(in super::super) fn state(state: ToolContinuationState) -> &'static str {
    match state {
        ToolContinuationState::Waiting => "waiting",
        ToolContinuationState::Claimed => "claimed",
        ToolContinuationState::Released => "released",
    }
}
pub(in super::super) async fn save(
    tx: &mut Transaction<'_, Postgres>,
    record: &ToolContinuationRecord,
    insert: bool,
) -> Result<(), Error> {
    record.validate().map_err(unknown)?;
    let query = if insert {
        "INSERT INTO tool_continuations(session_id,run_id,id,generation,state,format_version,data_json,input_source_protocol) VALUES($1,$2,$3,$4,$5,1,$6,1)"
    } else {
        "UPDATE tool_continuations SET state=$5,data_json=$6 WHERE session_id=$1 AND run_id=$2 AND id=$3 AND generation=$4 AND format_version=1"
    };
    let result = sqlx::query(query)
        .bind(&record.session_id.0)
        .bind(&record.run_id.0)
        .bind(&record.batch.continuation.id.0)
        .bind(i64::try_from(record.batch.continuation.generation.get()).map_err(unknown)?)
        .bind(state(record.state))
        .bind(serde_json::to_string(record).map_err(unknown)?)
        .execute(&mut **tx)
        .await
        .map_err(unknown)?;
    if result.rows_affected() != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    if insert {
        super::super::frontend::save_links(tx, record).await?;
        for phase in &record.batch.phases {
            sqlx::query("INSERT INTO tool_continuation_phases(continuation_id,run_id,phase_id) VALUES($1,$2,$3)").bind(&record.batch.continuation.id.0).bind(&record.run_id.0).bind(&phase.previous.id.0).execute(&mut **tx).await.map_err(unknown)?;
        }
        for question in &record.questions {
            let duplicate: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM continuation_interactions WHERE id=$1)",
            )
            .bind(&question.view.id.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
            if duplicate {
                return Err(Error::SemanticConflict);
            }
            sqlx::query("INSERT INTO tool_continuation_interactions(id,continuation_id,run_id,call_id,phase_id) VALUES($1,$2,$3,$4,$5)").bind(&question.view.id.0).bind(&record.batch.continuation.id.0).bind(&record.run_id.0).bind(&question.view.call_id.0).bind(&question.view.phase.id.0).execute(&mut **tx).await.map_err(unknown)?;
        }
    }
    Ok(())
}
pub(in super::super) async fn active(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
) -> Result<Option<ToolContinuationRecord>, Error> {
    super::scan::run(tx, run)
        .await
        .map(|records| records.active)
}
pub(in super::super) async fn replay(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: &ControlId,
    digest: &InputFingerprint,
) -> Result<Option<ToolContinuationReceipt>, Error> {
    let saved = super::read::receipt(tx, &run.session_id, id)
        .await?
        .map(|(receipt, _)| receipt);
    if saved
        .as_ref()
        .is_some_and(|receipt| receipt.run_id != run.id || receipt.semantic_digest != *digest)
    {
        return Err(Error::SemanticConflict);
    }
    Ok(saved)
}
pub(in crate::postgres) async fn replay_scope(
    tx: &mut Transaction<'_, Postgres>,
    session: &crabber_core::SessionId,
    run_id: &crabber_core::RunId,
    id: &ControlId,
    digest: &InputFingerprint,
) -> Result<Option<ToolContinuationReceipt>, Error> {
    let saved = super::read::receipt(tx, session, id)
        .await?
        .map(|(receipt, _)| receipt);
    if saved
        .as_ref()
        .is_some_and(|receipt| receipt.run_id != *run_id || receipt.semantic_digest != *digest)
    {
        return Err(Error::SemanticConflict);
    }
    Ok(saved)
}

pub(in super::super) async fn publish(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: ControlId,
    digest: InputFingerprint,
    outcome: ToolContinuationOutcome,
    now: OffsetDateTime,
) -> Result<ToolContinuationReceipt, Error> {
    publish_handoff(tx, run, id, digest, outcome, now, None).await
}
pub(in super::super) async fn publish_handoff(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: ControlId,
    digest: InputFingerprint,
    outcome: ToolContinuationOutcome,
    now: OffsetDateTime,
    handoff: Option<&crate::continuation_handoff::Handoff>,
) -> Result<ToolContinuationReceipt, Error> {
    super::super::bump_public_revision(tx, &run.session_id)
        .await
        .map_err(unknown)?;
    let mut receipt = ToolContinuationReceipt {
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        operation_id: id,
        semantic_digest: digest,
        revision: revision(tx, &run.session_id).await?,
        event_cursor: EventCursor(0),
        outcome,
    };
    let mut event = domain::audit(&receipt, now);
    if let Some(handoff) = handoff {
        event.payload["handoff"] =
            serde_json::to_value(handoff).map_err(|_| Error::InvalidRequest)?;
    }
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3) RETURNING seq",
    )
    .bind(&run.session_id.0)
    .bind(&run.id.0)
    .bind(crate::public_state::encode_event(&event).map_err(unknown)?)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    receipt.event_cursor = EventCursor(u64::try_from(seq).map_err(unknown)?);
    sqlx::query("INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'tool_continuation',$3,$4,$5)").bind(&run.session_id.0).bind(&receipt.operation_id.0).bind(receipt.semantic_digest.as_str()).bind(seq).bind(serde_json::to_string(&receipt).map_err(unknown)?).execute(&mut **tx).await.map_err(unknown)?;
    Ok(receipt)
}
