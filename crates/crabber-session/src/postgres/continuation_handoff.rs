//! Cross-domain barrier transfer shares the native suspension transaction.
use super::{approval, tool_continuation};
use crate::ApprovalError as Error;
use crate::continuation_handoff::{Domain, Handoff, Source};
use crabber_core::{EventCursor, InputFingerprint, Run, RunId, SessionId};
use sqlx::{Postgres, Row, Transaction};
fn unknown<T>(_: T) -> Error {
    Error::UnknownStoreOutcome
}
pub(super) async fn prepare(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    domain: Domain,
    target: &crate::ContinuationTarget,
    operation: &crabber_core::ControlId,
    digest: &InputFingerprint,
    parent: &crate::AttemptTarget,
) -> Result<Option<Handoff>, Error> {
    let predecessor = match domain {
        Domain::Approval => {
            let Some(record) = tool_continuation::write::active(tx, run)
                .await
                .map_err(unknown)?
            else {
                return Ok(None);
            };
            if record.batch.expected_plan.as_str() != run.plan_fingerprint
                || record.state != crate::ToolContinuationState::Claimed
                || record.batch.next_attempt != *parent
                || record.questions.iter().any(|q| !q.view.consumed)
            {
                return Err(Error::InvalidTransition);
            }
            tool_source(tx, &record).await?
        }
        Domain::Tool => {
            let active: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1)",
            )
            .bind(&run.session_id.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
            if !active {
                return Ok(None);
            }
            let (record, _) = approval::load(tx, &run.session_id, &run.id)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            if record.batch.expected_plan.as_str() != run.plan_fingerprint
                || record.state != crate::ApprovalContinuationState::Claimed
                || record.batch.next_attempt != *parent
                || record.questions.iter().any(|q| !q.view.consumed)
            {
                return Err(Error::InvalidTransition);
            }
            let rows =
                approval::current::rows(tx, &record, approval::current::Select::Latest, true)
                    .await?;
            let source =
                approval::current::source(rows.first().ok_or(Error::UnknownStoreOutcome)?)?;
            Source {
                domain: Domain::Approval,
                continuation: record.batch.continuation,
                operation_id: source.receipt.operation_id,
                digest: source.receipt.semantic_digest,
                cursor: source.receipt.event_cursor,
            }
        }
    };
    Ok(Some(Handoff {
        predecessor,
        successor_domain: domain,
        successor: target.clone(),
        suspension_id: operation.clone(),
        suspension_digest: digest.clone(),
        plan: InputFingerprint::new(run.plan_fingerprint.clone()).map_err(unknown)?,
        parent: parent.clone(),
    }))
}
pub(super) async fn tool_source(
    tx: &mut Transaction<'_, Postgres>,
    record: &crate::tool_continuation::ToolContinuationRecord,
) -> Result<Source, Error> {
    let id = record
        .recovery
        .as_ref()
        .map(|r| &r.id)
        .or(record.claim.as_ref().map(|c| &c.id))
        .ok_or(Error::UnknownStoreOutcome)?;
    let (receipt, _) = tool_continuation::read::receipt(tx, &record.session_id, id)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    Ok(Source {
        domain: Domain::Tool,
        continuation: record.batch.continuation.clone(),
        operation_id: receipt.operation_id,
        digest: receipt.semantic_digest,
        cursor: receipt.event_cursor,
    })
}
pub(super) async fn release(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    proof: &Handoff,
) -> Result<(), Error> {
    match proof.predecessor.domain {
        Domain::Approval => {
            let mut old = approval::current::exact_scoped(
                tx,
                &run.session_id,
                &run.id,
                &proof.predecessor.continuation,
            )
            .await?;
            old.state = crate::ApprovalContinuationState::Released;
            approval::write::save(tx, &old).await?;
            sqlx::query(
                "DELETE FROM continuation_barriers WHERE session_id=$1 AND continuation_id=$2",
            )
            .bind(&run.session_id.0)
            .bind(&proof.predecessor.continuation.id.0)
            .execute(&mut **tx)
            .await
            .map_err(unknown)?;
        }
        Domain::Tool => {
            let mut old = tool_continuation::read::origin(
                tx,
                &run.session_id,
                &run.id,
                Some(&proof.predecessor.continuation.id),
            )
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
            old.state = crate::ToolContinuationState::Released;
            tool_continuation::write::save(tx, &old, false)
                .await
                .map_err(unknown)?;
            sqlx::query(
                "DELETE FROM tool_continuation_barriers WHERE session_id=$1 AND continuation_id=$2",
            )
            .bind(&run.session_id.0)
            .bind(&proof.predecessor.continuation.id.0)
            .execute(&mut **tx)
            .await
            .map_err(unknown)?;
        }
    }
    Ok(())
}
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    domain: Domain,
    target: &crate::ContinuationTarget,
    parent: &crate::AttemptTarget,
    plan: &InputFingerprint,
) -> Result<Option<Handoff>, Error> {
    let rows = sqlx::query(CROSS)
        .bind(&session.0)
        .bind(&run.0)
        .bind(&target.id.0)
        .bind(match domain {
            Domain::Approval => "approval",
            Domain::Tool => "tool",
        })
        .bind(65536_i64)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    if rows.len() > 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let text = row
        .try_get::<Option<String>, _>("event")
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let mut event: crabber_core::EventRecord = crate::public_state::decode_event(
        sqlx::types::Json(serde_json::from_str(&text).map_err(unknown)?),
    )
    .map_err(unknown)?;
    event.cursor = Some(EventCursor(
        u64::try_from(row.get::<i64, _>("seq")).map_err(unknown)?,
    ));
    let proof = Handoff::from_event(&event)
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if proof.predecessor.continuation != *target
        || proof.parent != *parent
        || proof.plan != *plan
        || event.cursor.is_none_or(|c| c <= proof.predecessor.cursor)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let (cursor, native_key, tool_batch) =
        successor(tx, session, run, &proof, parent, plan).await?;
    let native = super::attempt::host_receipt(tx, session, &native_key)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if let Some(batch) = tool_batch {
        let source = source_event(tx, session, native.event_cursor).await?;
        crate::continuation_handoff::verify_native_origin(&batch, &native, &source)
            .map_err(unknown)?;
    }
    if event.session_id != *session
        || event.run_id != *run
        || event.cursor != Some(cursor)
        || native.attempt.target != *parent
        || native.attempt.state != crate::AttemptState::Suspended
        || native.attempt.cleanup != crate::AttemptCleanup::Complete
        || native.event_cursor >= cursor
        || native.event_cursor <= proof.predecessor.cursor
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(Some(proof))
}

const CROSS: &str = r#"SELECT e.seq,CAST(octet_length(e.data::text) AS BIGINT) AS bytes,CASE WHEN octet_length(e.data::text)<=65536 AND octet_length(e.data::text)<=$5 THEN e.data::text END AS event FROM events e WHERE e.session_id=$1 AND e.run_id=$2 AND (CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":', 1) ~ ',"kind":"continuation_changed"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END)#>>'{payload,handoff,predecessor,continuation,id}'=$3 AND (CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":', 1) ~ ',"kind":"continuation_changed"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END)#>>'{payload,handoff,predecessor,domain}'=$4 ORDER BY e.seq LIMIT 2"#;

pub(super) async fn preflight(
    tx: &mut Transaction<'_, Postgres>,
    record: &crate::approval::ApprovalRecord,
    proof: &mut crate::public_session::budget::Budget,
) -> Result<bool, crate::public_session::budget::Failure> {
    use crate::public_session::budget::unknown as fail;
    let rows = sqlx::query(CROSS)
        .bind(&record.session_id.0)
        .bind(&record.run_id.0)
        .bind(&record.batch.continuation.id.0)
        .bind("approval")
        .bind(0_i64)
        .fetch_all(&mut **tx)
        .await
        .map_err(fail)?;
    if rows.len() > 1 {
        return Err(fail("duplicate handoff"));
    }
    let Some(row) = rows.first() else {
        return Ok(false);
    };
    proof.charge(usize::try_from(row.try_get::<i64, _>("bytes").map_err(fail)?).map_err(fail)?)?;
    // Select lengths first; duplicate/corrupt carriers cannot transfer several
    // individually permitted blobs before the aggregate budget rejects them.
    let materialized = sqlx::query(CROSS)
        .bind(&record.session_id.0)
        .bind(&record.run_id.0)
        .bind(&record.batch.continuation.id.0)
        .bind("approval")
        .bind(row.try_get::<i64, _>("bytes").map_err(fail)?)
        .fetch_one(&mut **tx)
        .await
        .map_err(fail)?;
    let row = &materialized;
    let text = row
        .try_get::<Option<String>, _>("event")
        .map_err(fail)?
        .ok_or_else(|| fail("handoff budget"))?;
    let event: crabber_core::EventRecord = crate::public_state::decode_event(sqlx::types::Json(
        serde_json::from_str(&text).map_err(fail)?,
    ))
    .map_err(fail)?;
    let h = Handoff::from_event(&event)
        .map_err(fail)?
        .ok_or_else(|| fail("handoff"))?;
    let query = match h.successor_domain {
        Domain::Approval => {
            r"SELECT CAST(octet_length(data_json) AS BIGINT) AS bytes,CASE WHEN octet_length(data_json)<=$4 AND octet_length(data_json)<=4194304 THEN data_json END AS data FROM continuations WHERE session_id=$1 AND run_id=$2 AND id=$3"
        }
        Domain::Tool => {
            r"SELECT CAST(octet_length(data_json) AS BIGINT) AS bytes,CASE WHEN octet_length(data_json)<=$4 AND octet_length(data_json)<=4194304 THEN data_json END AS data FROM tool_continuations WHERE session_id=$1 AND run_id=$2 AND id=$3"
        }
    };
    let row = sqlx::query(query)
        .bind(&record.session_id.0)
        .bind(&record.run_id.0)
        .bind(&h.successor.id.0)
        .bind(i64::try_from(proof.remaining()).map_err(fail)?)
        .fetch_optional(&mut **tx)
        .await
        .map_err(fail)?
        .ok_or_else(|| fail("successor"))?;
    let bytes = usize::try_from(row.try_get::<i64, _>("bytes").map_err(fail)?).map_err(fail)?;
    proof.charge(bytes)?;
    let data = row
        .try_get::<Option<String>, _>("data")
        .map_err(fail)?
        .ok_or_else(|| fail("successor budget"))?;
    match h.successor_domain {
        Domain::Approval => {
            let next: crate::approval::ApprovalRecord =
                serde_json::from_str(&data).map_err(fail)?;
            if next.questions.len() > 64 || next.batch.calls.len() > 64 {
                return Err(fail("successor bounds"));
            }
            approval::public_header::origin_cost(tx, &next, proof).await?;
        }
        Domain::Tool => {
            tool_cost(tx, record, &data, bytes, proof).await?;
        }
    }
    Ok(true)
}

async fn source_event(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    cursor: EventCursor,
) -> Result<crabber_core::EventRecord, Error> {
    let row=sqlx::query("SELECT seq,CASE WHEN octet_length(data::text)<=65536 THEN data::text END AS event FROM events WHERE session_id=$1 AND seq<$2 ORDER BY seq DESC LIMIT 1").bind(&session.0).bind(i64::try_from(cursor.0).map_err(unknown)?).fetch_one(&mut **tx).await.map_err(unknown)?;
    let text = row
        .get::<Option<String>, _>("event")
        .ok_or(Error::UnknownStoreOutcome)?;
    let mut event: crabber_core::EventRecord = crate::public_state::decode_event(
        sqlx::types::Json(serde_json::from_str(&text).map_err(unknown)?),
    )
    .map_err(unknown)?;
    event.cursor = Some(EventCursor(
        u64::try_from(row.get::<i64, _>("seq")).map_err(unknown)?,
    ));
    Ok(event)
}

async fn successor(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    proof: &Handoff,
    parent: &crate::AttemptTarget,
    plan: &InputFingerprint,
) -> Result<
    (
        EventCursor,
        crabber_core::ControlId,
        Option<crate::ToolContinuationSuspension>,
    ),
    Error,
> {
    Ok(match proof.successor_domain {
        Domain::Approval => {
            let next = approval::current::exact_scoped(tx, session, run, &proof.successor).await?;
            approval::origin::verify(tx, &next).await?;
            if next.handoff.as_ref() != Some(proof)
                || next.batch.attempt != *parent
                || next.batch.id != proof.suspension_id
                || next.batch.expected_plan != *plan
                || next.batch.fingerprint(session, run)? != proof.suspension_digest
            {
                return Err(Error::UnknownStoreOutcome);
            }
            let saved = approval::receipt(tx, session, &next.batch.id)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            (
                saved.event_cursor,
                crate::approval::derived_id("suspend-attempt", &next.batch.id),
                None,
            )
        }
        Domain::Tool => {
            let next = tool_continuation::read::origin(tx, session, run, Some(&proof.successor.id))
                .await
                .map_err(unknown)?
                .ok_or(Error::UnknownStoreOutcome)?;
            if next.handoff.as_ref() != Some(proof)
                || next.batch.continuation != proof.successor
                || next.batch.attempt != *parent
                || next.batch.id != proof.suspension_id
                || next.batch.expected_plan != *plan
                || next.batch.fingerprint(session, run).map_err(unknown)? != proof.suspension_digest
            {
                return Err(Error::UnknownStoreOutcome);
            }
            let (saved, _) = tool_continuation::read::receipt(tx, session, &next.batch.id)
                .await
                .map_err(unknown)?
                .ok_or(Error::UnknownStoreOutcome)?;
            let native_key = crate::tool_continuation::derived_id(
                "tool-continuation-suspend-attempt",
                &next.batch.id,
            );
            (saved.event_cursor, native_key, Some(next.batch))
        }
    })
}

async fn tool_cost(
    tx: &mut Transaction<'_, Postgres>,
    record: &crate::approval::ApprovalRecord,
    data: &str,
    bytes: usize,
    proof: &mut crate::public_session::budget::Budget,
) -> Result<(), crate::public_session::budget::Failure> {
    use crate::public_session::budget::unknown as fail;
    let next: crate::tool_continuation::ToolContinuationRecord =
        serde_json::from_str(data).map_err(fail)?;
    if next.questions.len() > 64 || next.batch.calls.len() > 64 {
        return Err(fail("successor bounds"));
    }
    proof.charge(
        bytes
            .checked_add(32768)
            .ok_or_else(|| fail("successor cost"))?,
    )?;
    let binding:i64=sqlx::query_scalar(r"SELECT CAST(octet_length(data_json) AS BIGINT) FROM run_execution_bindings WHERE run_id=$1").bind(&record.run_id.0).fetch_one(&mut **tx).await.map_err(fail)?;
    proof.charge(usize::try_from(binding).map_err(fail)?)?;
    let messages:i64=sqlx::query_scalar(r"SELECT CAST(COALESCE(SUM(octet_length(snapshot_record)),0) AS BIGINT) FROM messages WHERE session_id=$1 AND run_id=$2").bind(&record.session_id.0).bind(&record.run_id.0).fetch_one(&mut **tx).await.map_err(fail)?;
    proof.charge(usize::try_from(messages).map_err(fail)?)?;
    let ids = std::iter::once(&next.batch.id)
        .chain(
            next.questions
                .iter()
                .filter_map(|q| q.answer.as_ref().map(|a| &a.id)),
        )
        .chain(next.claim.iter().map(|c| &c.id))
        .chain(next.recovery.iter().map(|c| &c.id));
    for id in ids {
        crate::attempt::validate_identifier(&id.0).map_err(fail)?;
        approval::public_header::receipt_cost(tx, &record.session_id, id, proof).await?;
    }
    let native =
        crate::tool_continuation::derived_id("tool-continuation-suspend-attempt", &next.batch.id);
    let (cursor, _) =
        approval::public_header::receipt_cost(tx, &record.session_id, &native, proof).await?;
    let bytes:i64=sqlx::query_scalar("SELECT CAST(octet_length(data::text) AS BIGINT) FROM events WHERE session_id=$1 AND seq<$2 ORDER BY seq DESC LIMIT 1").bind(&record.session_id.0).bind(cursor).fetch_one(&mut **tx).await.map_err(fail)?;
    proof.charge(usize::try_from(bytes).map_err(fail)?)?;
    Ok(())
}

#[cfg(test)]
#[path = "continuation_handoff/plan_tests.rs"]
mod plan_tests;
