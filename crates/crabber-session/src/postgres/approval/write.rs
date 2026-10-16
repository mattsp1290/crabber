//! Record and audit writes inside the caller's session transaction.
use super::super::db;
use super::{Error, approval, receipt, sql};
use crate::ApprovalReceipt;
use crabber_core::{ControlId, EventCursor, InputFingerprint, Run};
use sqlx::{Postgres, Row, Transaction};
pub(in super::super) async fn replay(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: &ControlId,
    digest: &InputFingerprint,
) -> Result<Option<ApprovalReceipt>, Error> {
    let saved = receipt(tx, &run.session_id, id).await?;
    if saved
        .as_ref()
        .is_some_and(|r| r.run_id != run.id || r.semantic_digest != *digest)
    {
        return Err(Error::SemanticConflict);
    }
    Ok(saved)
}
pub(in super::super) async fn active(
    tx: &mut Transaction<'_, Postgres>,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    let row = sqlx::query("SELECT CASE WHEN LENGTH(continuation_id)<=128 THEN continuation_id END AS continuation_id, CASE WHEN LENGTH(run_id)<=128 THEN run_id END AS run_id FROM continuation_barriers WHERE session_id=$1")
        .bind(&record.session_id.0).fetch_optional(&mut **tx).await.map_err(db).map_err(sql::unknown)?
        .ok_or(Error::InvalidTransition)?;
    if row.get::<Option<String>, _>("continuation_id").as_deref()
        != Some(&record.batch.continuation.id.0)
        || row.get::<Option<String>, _>("run_id").as_deref() != Some(&record.run_id.0)
    {
        return Err(Error::GenerationConflict);
    }
    Ok(())
}
pub(in super::super) async fn save(
    tx: &mut Transaction<'_, Postgres>,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    record.validate(&record.session_id, &record.run_id)?;
    if crate::snapshot::encoded_bytes(record, sql::MAX_RECORD_BYTES).is_none() {
        return Err(Error::InvalidRequest);
    }
    let data = serde_json::to_string(record).map_err(|_| Error::InvalidRequest)?;
    let result = sqlx::query("UPDATE continuations SET state=$4,data_json=$5 WHERE session_id=$1 AND run_id=$2 AND id=$3 AND generation=$6 AND format_version=1")
        .bind(&record.session_id.0).bind(&record.run_id.0).bind(&record.batch.continuation.id.0)
        .bind(sql::enum_text(record.state)?).bind(data)
        .bind(i64::try_from(record.batch.continuation.generation.get()).map_err(|_| Error::GenerationConflict)?)
        .execute(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
    if result.rows_affected() != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(in super::super) async fn publish(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: ControlId,
    digest: InputFingerprint,
    outcome: crate::ApprovalOutcome,
    now: time::OffsetDateTime,
) -> Result<ApprovalReceipt, Error> {
    publish_handoff(tx, run, id, digest, outcome, now, None).await
}
pub(in super::super) async fn publish_handoff(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: ControlId,
    digest: InputFingerprint,
    outcome: crate::ApprovalOutcome,
    now: time::OffsetDateTime,
    handoff: Option<&crate::continuation_handoff::Handoff>,
) -> Result<ApprovalReceipt, Error> {
    super::super::bump_public_revision(tx, &run.session_id)
        .await
        .map_err(sql::unknown)?;
    let raw: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&run.session_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?;
    let revision = crate::attempt::sql::revision(raw).map_err(approval::attempt_error)?;
    let mut event = approval::event(run, &id, &digest, revision, &outcome, now)?;
    if let Some(handoff) = handoff {
        event.payload["handoff"] =
            serde_json::to_value(handoff).map_err(|_| Error::InvalidRequest)?;
    }
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3) RETURNING seq",
    )
    .bind(&run.session_id.0)
    .bind(&run.id.0)
    .bind(crate::public_state::encode_event(&event).map_err(sql::unknown)?)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)
    .map_err(sql::unknown)?;
    let receipt = ApprovalReceipt {
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        operation_id: id,
        semantic_digest: digest,
        revision,
        event_cursor: EventCursor(u64::try_from(seq).map_err(|_| Error::UnknownStoreOutcome)?),
        outcome,
    };
    sqlx::query("INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'approval',$3,$4,$5)")
        .bind(&run.session_id.0).bind(&receipt.operation_id.0).bind(receipt.semantic_digest.as_str())
        .bind(seq).bind(serde_json::to_string(&receipt).map_err(|_| Error::UnknownStoreOutcome)?)
        .execute(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
    Ok(receipt)
}

pub(in super::super) async fn revision(
    tx: &mut Transaction<'_, Postgres>,
    session: &crabber_core::SessionId,
) -> Result<crabber_core::SessionRevision, Error> {
    let raw: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?;
    crate::attempt::sql::revision(raw).map_err(approval::attempt_error)
}
pub(in super::super) async fn insert(
    tx: &mut Transaction<'_, Postgres>,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    record.validate(&record.session_id, &record.run_id)?;
    if crate::snapshot::encoded_bytes(record, sql::MAX_RECORD_BYTES).is_none() {
        return Err(Error::InvalidRequest);
    }
    sqlx::query("INSERT INTO continuations(id,session_id,run_id,generation,state,format_version,data_json) VALUES($1,$2,$3,$4,$5,1,$6)")
        .bind(&record.batch.continuation.id.0).bind(&record.session_id.0).bind(&record.run_id.0)
        .bind(i64::try_from(record.batch.continuation.generation.get()).map_err(|_| Error::GenerationConflict)?)
        .bind(sql::enum_text(record.state)?).bind(serde_json::to_string(record).map_err(|_| Error::InvalidRequest)?)
        .execute(&mut **tx).await.map_err(control_db)?;
    for call in &record.batch.calls {
        if let crate::CallApproval::Ask { id, .. } = &call.decision {
            sqlx::query("INSERT INTO continuation_interactions(id,continuation_id,run_id,call_id) VALUES($1,$2,$3,$4)")
                .bind(&id.0).bind(&record.batch.continuation.id.0).bind(&record.run_id.0).bind(&call.target.call_id.0)
                .execute(&mut **tx).await.map_err(control_db)?;
        }
    }
    Ok(())
}
fn control_db(error: sqlx::Error) -> Error {
    if db(error) == crate::StoreError::Conflict {
        Error::SemanticConflict
    } else {
        Error::UnknownStoreOutcome
    }
}
