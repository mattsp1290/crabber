//! Budgeted reuse of the original private approval integrity proofs.
use crate::public_session::budget::{self, Budget, Failure, unknown};
use crate::{AttemptSnapshot, PublicApprovalBarrier, PublicApprovalSummary, PublicHeaderRequest};
use crabber_core::{ControlId, RunId, RunStatus, SessionId};
use sqlx::{Row, Sqlite, Transaction};

pub(in super::super) async fn read(
    tx: &mut Transaction<'_, Sqlite>,
    request: &PublicHeaderRequest,
    status: Option<RunStatus>,
    attempt: Option<&AttemptSnapshot>,
    safe: &mut Budget,
    proof: &mut Budget,
) -> Result<(Option<PublicApprovalSummary>, Option<PublicApprovalBarrier>), Failure> {
    let barrier = sqlx::query("SELECT CAST(length(CAST(run_id AS BLOB))+length(CAST(continuation_id AS BLOB)) AS BIGINT) AS bytes,CASE WHEN length(CAST(run_id AS BLOB))<=128 AND length(CAST(run_id AS BLOB))+length(CAST(continuation_id AS BLOB))<=$2 THEN run_id END AS run_id,CASE WHEN length(CAST(continuation_id AS BLOB))<=128 AND length(CAST(run_id AS BLOB))+length(CAST(continuation_id AS BLOB))<=$2 THEN continuation_id END AS continuation_id FROM continuation_barriers WHERE session_id=$1").bind(&request.session_id.0).bind(i64::try_from(safe.remaining()).map_err(unknown)?).fetch_optional(&mut **tx).await.map_err(unknown)?;
    if let Some(row) = &barrier {
        safe.charge(
            usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?).map_err(unknown)?,
        )?;
    }
    let Some(run) = &request.run_id else {
        if barrier.is_some() {
            return Err(unknown("barrier without run"));
        }
        return Ok((None, None));
    };
    let Some(record) = preflight(tx, request, run, proof).await? else {
        if barrier.is_some() {
            return Err(unknown("barrier without approval"));
        }
        return Ok((None, None));
    };
    let active = if let Some(row) = &barrier {
        if row
            .try_get::<Option<String>, _>("run_id")
            .map_err(unknown)?
            .as_deref()
            != Some(&run.0)
            || row
                .try_get::<Option<String>, _>("continuation_id")
                .map_err(unknown)?
                .as_deref()
                != Some(&record.batch.continuation.id.0)
        {
            return Err(unknown("barrier scope"));
        }
        true
    } else {
        false
    };
    budget::validate_approval_state(
        &record,
        active,
        status.ok_or_else(|| unknown("status"))?,
        attempt,
    )?;
    let header = budget::approval(&record, request.max_interactions, safe)?;
    let barrier = active.then(|| PublicApprovalBarrier {
        run_id: run.clone(),
        continuation: record.batch.continuation.clone(),
    });
    Ok((Some(header), barrier))
}
async fn preflight(
    tx: &mut Transaction<'_, Sqlite>,
    request: &PublicHeaderRequest,
    run: &RunId,
    proof: &mut Budget,
) -> Result<Option<crate::approval::ApprovalRecord>, Failure> {
    let row = sqlx::query("SELECT CAST(length(CAST(data_json AS BLOB)) AS BIGINT) AS bytes,CASE WHEN length(CAST(data_json AS BLOB))<=$3 AND length(CAST(data_json AS BLOB))<=4194304 THEN data_json END AS data FROM continuations WHERE session_id=$1 AND run_id=$2 ORDER BY generation DESC LIMIT 1").bind(&request.session_id.0).bind(&run.0).bind(i64::try_from(proof.remaining()).map_err(unknown)?).fetch_optional(&mut **tx).await.map_err(unknown)?;
    let Some(row) = row else {
        if super::current::has_source(tx, &request.session_id, Some(run))
            .await
            .map_err(unknown)?
        {
            return Err(unknown("missing published approval"));
        }
        return Ok(None);
    };
    let bytes =
        usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?).map_err(unknown)?;
    proof.charge(bytes)?;
    if bytes > 4 * 1024 * 1024 {
        return Err(unknown("continuation size"));
    }
    let data = row
        .try_get::<Option<String>, _>("data")
        .map_err(unknown)?
        .ok_or_else(|| unknown("continuation budget"))?;
    let record: crate::approval::ApprovalRecord = serde_json::from_str(&data).map_err(unknown)?;
    if record.session_id != request.session_id || record.run_id != *run {
        return Err(unknown("approval scope"));
    }
    if record.questions.len() > usize::from(request.max_interactions) {
        return Err(Failure::Limited(crate::PublicSessionLimit::Interactions));
    }
    origin_cost(tx, &record, proof).await?;
    super::current::preflight(tx, &record, proof).await?;
    let (checked, _) = super::load(tx, &request.session_id, run)
        .await
        .map_err(unknown)?
        .ok_or_else(|| unknown("approval disappeared"))?;
    for question in &checked.questions {
        if let Some(answer) = &question.answer {
            let saved = super::receipt(tx, &request.session_id, &answer.id)
                .await
                .map_err(unknown)?
                .ok_or_else(|| unknown("answer receipt"))?;
            crate::approval::verify_recovery_answer(answer, &saved).map_err(unknown)?;
        }
    }
    Ok(Some(checked))
}
pub(in super::super) async fn receipt_cost(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
    proof: &mut Budget,
) -> Result<(i64, usize), Failure> {
    let row = sqlx::query("SELECT CAST(length(CAST(r.data_json AS BLOB)) AS BIGINT) AS data_bytes,CAST(length(CAST(e.data AS BLOB)) AS BIGINT) AS event_bytes,r.event_seq FROM operation_receipts r LEFT JOIN events e ON e.session_id=r.session_id AND e.seq=r.event_seq WHERE r.session_id=$1 AND r.operation_id=$2").bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or_else(|| unknown("receipt"))?;
    let data =
        usize::try_from(row.try_get::<i64, _>("data_bytes").map_err(unknown)?).map_err(unknown)?;
    let event = usize::try_from(
        row.try_get::<Option<i64>, _>("event_bytes")
            .map_err(unknown)?
            .ok_or_else(|| unknown("receipt event"))?,
    )
    .map_err(unknown)?;
    proof.charge(
        data.checked_add(event)
            .and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| unknown("receipt bytes"))?,
    )?;
    Ok((
        row.try_get::<Option<i64>, _>("event_seq")
            .map_err(unknown)?
            .ok_or_else(|| unknown("event cursor"))?,
        event,
    ))
}

pub(in super::super) async fn origin_cost(
    tx: &mut Transaction<'_, Sqlite>,
    record: &crate::approval::ApprovalRecord,
    proof: &mut Budget,
) -> Result<(), Failure> {
    let bytes: i64=sqlx::query_scalar("SELECT CAST(length(CAST(data_json AS BLOB)) AS BIGINT) FROM continuations WHERE session_id=$1 AND id=$2").bind(&record.session_id.0).bind(&record.batch.continuation.id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    let bytes = usize::try_from(bytes).map_err(unknown)?;
    // The normal loader reads the record again; account for both carrier reads.
    proof.charge(
        bytes
            .checked_add(1024)
            .ok_or_else(|| unknown("proof bytes"))?,
    )?;
    crate::attempt::validate_identifier(&record.batch.id.0).map_err(unknown)?;
    let (outer_cursor, outer_event) =
        receipt_cost(tx, &record.session_id, &record.batch.id, proof).await?;
    let _ = outer_cursor;
    proof.charge(
        outer_event
            .checked_add(128)
            .ok_or_else(|| unknown("proof metadata"))?,
    )?; // Original outer event is reread by origin::verify.
    let native_id = crate::approval::derived_id("suspend-attempt", &record.batch.id);
    let (native_cursor, native_event) =
        receipt_cost(tx, &record.session_id, &native_id, proof).await?;
    proof.charge(
        native_event
            .checked_add(128)
            .ok_or_else(|| unknown("proof metadata"))?,
    )?;
    let bytes: Option<i64> = sqlx::query_scalar("SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM events WHERE session_id=$1 AND seq<$2 ORDER BY seq DESC LIMIT 1").bind(&record.session_id.0).bind(native_cursor).fetch_optional(&mut **tx).await.map_err(unknown)?;
    proof.charge(
        usize::try_from(bytes.ok_or_else(|| unknown("source"))?)
            .map_err(unknown)?
            .checked_add(128)
            .ok_or_else(|| unknown("proof metadata"))?,
    )?;
    let bytes: i64 = sqlx::query_scalar(
        "SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2",
    )
    .bind(&record.session_id.0)
    .bind(&record.run_id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    proof.charge(usize::try_from(bytes).map_err(unknown)?)?;
    let bytes: i64 = sqlx::query_scalar("SELECT CAST(COALESCE(SUM(length(CAST(id AS BLOB))+length(CAST(call_id AS BLOB))),0) AS BIGINT) FROM (SELECT id,call_id FROM continuation_interactions WHERE continuation_id=$1 AND run_id=$2 LIMIT 65) bounded").bind(&record.batch.continuation.id.0).bind(&record.run_id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    proof.charge(usize::try_from(bytes).map_err(unknown)?)?;
    for question in &record.questions {
        if let Some(answer) = &question.answer {
            crate::attempt::validate_identifier(&answer.id.0).map_err(unknown)?;
            receipt_cost(tx, &record.session_id, &answer.id, proof).await?;
        }
    }

    Ok(())
}
