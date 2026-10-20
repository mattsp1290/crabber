use crate::postgres::transaction::Transaction;
use crate::{
    RunControlError as Error,
    run_control::{
        MAX_CONTROLS, MAX_RUN_CONTROL_BYTES, Record, RunControlReceipt, RunControlSummary,
        RunControlTarget, accepted, unknown, verify_event, verify_target,
    },
};
use crabber_core::{ControlId, Run, SessionId, SessionRevision};
use serde::de::DeserializeOwned;
use sqlx::{Postgres, Row};
const CARRIER: &str = r#"(CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":', 1) LIKE '%,"kind":"run_control_changed"' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END)"#;
fn decode<T: DeserializeOwned>(value: Option<String>) -> Result<T, Error> {
    serde_json::from_str(&value.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)
}
pub(super) async fn revision(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<SessionRevision, Error> {
    let n: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&session.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    SessionRevision::new(u64::try_from(n).map_err(unknown)?).map_err(unknown)
}
pub(super) async fn load_run(
    tx: &mut Transaction<'_, Postgres>,
    target: &RunControlTarget,
) -> Result<Run, Error> {
    let run = super::super::load_run(tx, &target.run_id, false)
        .await
        .map_err(unknown)?;
    verify_target(target, &run)?;
    Ok(run)
}
async fn event_at(
    tx: &mut Transaction<'_, Postgres>,
    summary: &RunControlSummary,
) -> Result<(), Error> {
    let row = sqlx::query("SELECT CASE WHEN octet_length(data::text)<=32768 THEN data END AS data FROM events WHERE session_id=$1 AND run_id=$2 AND seq=$3")
        .bind(&summary.receipt.target.session_id.0).bind(&summary.receipt.target.run_id.0).bind(i64::try_from(summary.event_cursor.0).map_err(unknown)?)
        .fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let event = crate::public_state::decode_event(
        row.try_get::<Option<sqlx::types::Json<serde_json::Value>>, _>("data")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    verify_event(summary, &event)
}
pub(super) async fn receipt(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<RunControlReceipt>, Error> {
    let Some(row) = sqlx::query("SELECT CASE WHEN kind='run_control' THEN 'run_control' ELSE 'foreign' END AS kind,CASE WHEN octet_length(semantic_digest)<=64 THEN semantic_digest END AS digest,event_seq,CASE WHEN octet_length(data_json)<=8192 THEN data_json END AS data FROM operation_receipts WHERE session_id=$1 AND operation_id=$2")
        .bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)? else { return Ok(None); };
    if row.try_get::<String, _>("kind").map_err(unknown)? != "run_control" {
        return Err(Error::SemanticConflict);
    }
    let saved: RunControlReceipt = decode(row.try_get("data").map_err(unknown)?)?;
    if saved.target.session_id != *session
        || saved.operation_id != *id
        || row
            .try_get::<Option<String>, _>("digest")
            .map_err(unknown)?
            .as_deref()
            != Some(saved.semantic_digest.as_str())
        || row
            .try_get::<Option<i64>, _>("event_seq")
            .map_err(unknown)?
            != Some(i64::try_from(saved.event_cursor.0).map_err(unknown)?)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    event_at(tx, &accepted(&saved)).await?;
    Ok(Some(saved))
}
pub(super) async fn summaries(
    tx: &mut Transaction<'_, Postgres>,
    target: &RunControlTarget,
) -> Result<Vec<RunControlSummary>, Error> {
    let query = "SELECT CASE WHEN octet_length(q.operation_id)<=128 THEN q.operation_id END AS id,q.sequence,(SELECT MAX(e.seq) FROM events e WHERE e.session_id=q.session_id AND e.run_id=q.run_id AND {event}#>>'{kind}'='run_control_changed' AND {event}#>>'{correlation}'=q.operation_id) AS audit_cursor,CASE WHEN octet_length(q.summary_json)<=8192 THEN q.summary_json END AS data,CASE WHEN octet_length(p.summary_json)<=8192 THEN p.summary_json END AS latest FROM run_controls q LEFT JOIN run_control_progress p ON p.session_id=q.session_id AND p.operation_id=q.operation_id AND p.event_seq=(SELECT MAX(x.event_seq) FROM run_control_progress x WHERE x.session_id=q.session_id AND x.operation_id=q.operation_id) WHERE q.session_id=$1 AND q.run_id=$2 ORDER BY q.sequence LIMIT 65".replace("{event}", CARRIER);
    let rows = sqlx::query(&query)
        .bind(&target.session_id.0)
        .bind(&target.run_id.0)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    if rows.len() > MAX_CONTROLS {
        return Err(Error::UnknownStoreOutcome);
    }
    let query = "SELECT COUNT(*) FROM events e WHERE e.session_id=$1 AND e.run_id=$2 AND {event}#>>'{kind}'='run_control_changed' AND {event}#>>'{payload,status}'='Accepted'".replace("{event}", CARRIER);
    let accepted: i64 = sqlx::query_scalar(&query)
        .bind(&target.session_id.0)
        .bind(&target.run_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if usize::try_from(accepted).map_err(unknown)? != rows.len() {
        return Err(Error::UnknownStoreOutcome);
    }
    let mut result = Vec::new();
    let mut bytes = 0;
    for (index, row) in rows.into_iter().enumerate() {
        let current: RunControlSummary = decode(row.try_get("data").map_err(unknown)?)?;
        let latest: RunControlSummary = decode(row.try_get("latest").map_err(unknown)?)?;
        if latest != current
            || row
                .try_get::<Option<i64>, _>("audit_cursor")
                .map_err(unknown)?
                != Some(i64::try_from(current.event_cursor.0).map_err(unknown)?)
            || current.receipt.target != *target
            || row
                .try_get::<Option<String>, _>("id")
                .map_err(unknown)?
                .as_deref()
                != Some(&current.receipt.operation_id.0)
            || u64::try_from(row.try_get::<i64, _>("sequence").map_err(unknown)?)
                .map_err(unknown)?
                != index as u64 + 1
            || current.receipt.sequence != index as u64 + 1
            || receipt(tx, &target.session_id, &current.receipt.operation_id)
                .await
                .map_err(unknown)?
                .as_ref()
                != Some(&current.receipt)
        {
            return Err(Error::UnknownStoreOutcome);
        }
        event_at(tx, &current).await?;
        bytes += serde_json::to_vec(&current).map_err(unknown)?.len();
        if bytes > MAX_RUN_CONTROL_BYTES {
            return Err(Error::UnknownStoreOutcome);
        }
        result.push(current);
    }
    Ok(result)
}
pub(super) async fn records(
    tx: &mut Transaction<'_, Postgres>,
    target: &RunControlTarget,
) -> Result<Vec<Record>, Error> {
    let mut result = Vec::new();
    let mut bytes = 0;
    for summary in summaries(tx, target).await? {
        let data: Option<String> = sqlx::query_scalar("SELECT CASE WHEN octet_length(request_json)<=65536 THEN request_json END FROM run_controls WHERE session_id=$1 AND operation_id=$2")
            .bind(&target.session_id.0).bind(&summary.receipt.operation_id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
        bytes += data.as_ref().ok_or(Error::UnknownStoreOutcome)?.len();
        if bytes > MAX_RUN_CONTROL_BYTES {
            return Err(Error::UnknownStoreOutcome);
        }
        let record = Record {
            request: decode(data)?,
            summary,
        };
        record.validate()?;
        result.push(record);
    }
    Ok(result)
}

pub(super) async fn verify_public_target(
    tx: &mut Transaction<'_, Postgres>,
    target: &RunControlTarget,
) -> Result<(), Error> {
    crate::attempt::validate_identifier(&target.run_id.0).map_err(|_| Error::InvalidRequest)?;
    let row = sqlx::query("SELECT CASE WHEN octet_length(session_id)<=512 THEN session_id END AS session,CASE WHEN octet_length(data->>'config_hash')<=512 THEN data->>'config_hash' END AS config,CASE WHEN octet_length(data->>'plan_fingerprint')<=512 THEN data->>'plan_fingerprint' END AS plan FROM runs WHERE id=$1")
        .bind(&target.run_id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::NotFound)?;
    if row
        .try_get::<Option<String>, _>("session")
        .map_err(unknown)?
        .as_deref()
        != Some(&target.session_id.0)
        || row
            .try_get::<Option<String>, _>("config")
            .map_err(unknown)?
            .as_deref()
            != Some(&target.config_hash)
        || row
            .try_get::<Option<String>, _>("plan")
            .map_err(unknown)?
            .as_deref()
            != Some(&target.plan_fingerprint)
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}

pub(super) async fn binding(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &crabber_core::RunId,
) -> Result<crate::RunControlBinding, Error> {
    crate::attempt::validate_identifier(&run.0).map_err(|_| Error::InvalidRequest)?;
    let row = sqlx::query("SELECT CASE WHEN octet_length(session_id)<=512 THEN session_id END AS session,CASE WHEN octet_length(data->>'config_hash')<=512 THEN data->>'config_hash' END AS config,CASE WHEN octet_length(data->>'plan_fingerprint')<=512 THEN data->>'plan_fingerprint' END AS plan FROM runs WHERE id=$1")
        .bind(&run.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::NotFound)?;
    if row
        .try_get::<Option<String>, _>("session")
        .map_err(unknown)?
        .as_deref()
        != Some(&session.0)
    {
        return Err(Error::NotFound);
    }
    let target = RunControlTarget {
        session_id: session.clone(),
        run_id: run.clone(),
        config_hash: row
            .try_get::<Option<String>, _>("config")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
        plan_fingerprint: row
            .try_get::<Option<String>, _>("plan")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    };
    crate::run_control::validate_target(&target).map_err(unknown)?;
    Ok(crate::RunControlBinding {
        revision: revision(tx, session).await?,
        target,
    })
}

pub(super) async fn status(
    tx: &mut Transaction<'_, Postgres>,
    target: &RunControlTarget,
) -> Result<crabber_core::RunStatus, Error> {
    let status: Option<String> = sqlx::query_scalar("SELECT CASE WHEN status IN ('pending','running','paused','interrupted','failed','completed') THEN status END FROM runs WHERE session_id=$1 AND id=$2").bind(&target.session_id.0).bind(&target.run_id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::NotFound)?;
    serde_json::from_value(serde_json::Value::String(
        status.ok_or(Error::UnknownStoreOutcome)?,
    ))
    .map_err(unknown)
}
