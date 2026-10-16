use super::{Error, FrontendOriginRecord, FrontendStagingReceipt, unknown};
use crate::FrontendCallStaging;
use crabber_core::{
    ControlId, EventCursor, EventRecord, Message, Run, RunId, SessionId, ToolCallId, ToolCallRecord,
};
use serde::de::DeserializeOwned;
use sqlx::{Postgres, Row, Transaction};
fn decode<T: DeserializeOwned>(value: Option<String>) -> Result<T, Error> {
    serde_json::from_str(&value.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)
}
fn event(row: &sqlx::postgres::PgRow) -> Result<EventRecord, Error> {
    let value = decode(row.try_get("event").map_err(unknown)?)?;
    let mut event = crate::public_state::decode_event(sqlx::types::Json(value)).map_err(unknown)?;
    event.cursor = Some(EventCursor(
        u64::try_from(row.try_get::<i64, _>("event_seq").map_err(unknown)?).map_err(unknown)?,
    ));
    Ok(event)
}
pub(super) async fn run(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &RunId,
    lock: bool,
) -> Result<Run, Error> {
    let mut query = "SELECT CASE WHEN octet_length(data::text)<=4210688 THEN data::text END AS data,CASE WHEN octet_length(status)<=32 THEN status END AS status,CASE WHEN octet_length(claim_token)<=512 THEN claim_token END AS token,lease_until FROM runs WHERE session_id=$1 AND id=$2".to_owned();
    if lock {
        query.push_str(" FOR UPDATE");
    }
    let row = sqlx::query(&query)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    let run: Run = decode(row.try_get("data").map_err(unknown)?)?;
    if run.session_id != *session
        || run.id != *id
        || row
            .try_get::<Option<String>, _>("status")
            .map_err(unknown)?
            .as_deref()
            != Some(super::super::status(run.status))
        || row
            .try_get::<Option<String>, _>("token")
            .map_err(unknown)?
            .as_deref()
            != Some(run.claim_token.as_str())
        || row.try_get::<i64, _>("lease_until").map_err(unknown)?
            != super::super::micros(run.lease_until)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(run)
}
pub(super) async fn receipt(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<FrontendStagingReceipt>, Error> {
    crate::attempt::validate_identifier(&id.0).map_err(|_| Error::InvalidRequest)?;
    let Some(row) = sqlx::query("SELECT CASE WHEN o.kind='frontend_staging' THEN 'frontend_staging' ELSE 'foreign' END AS kind,CASE WHEN octet_length(o.semantic_digest)<=64 THEN o.semantic_digest END AS digest,o.event_seq,CASE WHEN octet_length(o.data_json)<=8192 THEN o.data_json END AS data,CASE WHEN o.kind='frontend_staging' AND octet_length(e.data::text)<=65536 THEN e.data::text END AS event FROM operation_receipts o LEFT JOIN events e ON e.session_id=o.session_id AND e.seq=o.event_seq WHERE o.session_id=$1 AND o.operation_id=$2")
        .bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)? else { return Ok(None); };
    if row.try_get::<String, _>("kind").map_err(unknown)? != "frontend_staging" {
        return Err(Error::SemanticConflict);
    }
    let saved: FrontendStagingReceipt = decode(row.try_get("data").map_err(unknown)?)?;
    if saved.session_id != *session
        || saved.operation_id != *id
        || row
            .try_get::<Option<String>, _>("digest")
            .map_err(unknown)?
            .as_deref()
            != Some(saved.semantic_digest.as_str())
    {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::frontend_continuation::verify_staging_receipt(&saved, &event(&row)?)?;
    Ok(Some(saved))
}
pub(super) async fn origin(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    call: &ToolCallId,
) -> Result<Option<FrontendOriginRecord>, Error> {
    let prefix = format!(
        "{{\"call_id\":{},\"frontend_origin\":",
        serde_json::to_string(call).map_err(unknown)?
    );
    // Match the canonical protected prefix before interpreting any payload JSON.
    // Arbitrary neighboring ToolCallPending data may contain escaped NULs.
    let sources = sqlx::query(r#"SELECT seq AS event_seq,CASE WHEN octet_length(data::text)<=65536 THEN data::text END AS event FROM events WHERE session_id=$1 AND run_id=$2 AND data->>'encoding'='crabber.event.json-text.v1' AND split_part(data->>'record', ',"payload":',1) ~ ',"kind":"tool_call_pending"$' AND left(split_part(data->>'record', ',"payload":',2),length($3))=$3 ORDER BY seq LIMIT 2"#)
        .bind(&run.session_id.0).bind(&run.id.0).bind(prefix).fetch_all(&mut **tx).await.map_err(unknown)?;
    let row = sqlx::query("SELECT CASE WHEN octet_length(session_id)<=512 THEN session_id END AS session_id,CASE WHEN octet_length(run_id)<=128 THEN run_id END AS run_id,format_version,CASE WHEN octet_length(data_json)<=262144 THEN data_json END AS data FROM frontend_tool_origins WHERE call_id=$1")
        .bind(&call.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    let Some(row) = row else {
        if !sources.is_empty() || declared_call(tx, run, call).await? {
            return Err(Error::UnknownStoreOutcome);
        }
        return Ok(None);
    };
    if sources.len() != 1
        || row
            .try_get::<Option<String>, _>("session_id")
            .map_err(unknown)?
            .as_deref()
            != Some(run.session_id.0.as_str())
        || row
            .try_get::<Option<String>, _>("run_id")
            .map_err(unknown)?
            .as_deref()
            != Some(run.id.0.as_str())
        || row.try_get::<i32, _>("format_version").map_err(unknown)? != 1
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let record: FrontendOriginRecord = decode(row.try_get("data").map_err(unknown)?)?;
    if record.request.call.id != *call
        || record.request.session_id != run.session_id
        || record.request.run_id != run.id
    {
        return Err(Error::UnknownStoreOutcome);
    }
    record.verify(&event(&sources[0])?)?;
    if receipt(tx, &run.session_id, &record.receipt.operation_id)
        .await
        .map_err(unknown)?
        .as_ref()
        != Some(&record.receipt)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    binding(tx, run, &record.request).await.map_err(unknown)?;
    let current = load_call(tx, run, call)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    if crate::phase::call_binding(&current).map_err(unknown)?
        != crate::phase::call_binding(&record.request.call).map_err(unknown)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(Some(record))
}
async fn declared_call(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    call: &ToolCallId,
) -> Result<bool, Error> {
    let Some(call) = load_call(tx, run, call).await? else {
        return Ok(false);
    };
    declared(tx, run, &call.name).await
}
async fn load_call(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: &ToolCallId,
) -> Result<Option<ToolCallRecord>, Error> {
    let Some(row) = sqlx::query("SELECT CASE WHEN octet_length(status)<=32 THEN status END AS status,CASE WHEN octet_length(snapshot_record)<=4194304 THEN snapshot_record END AS data FROM tool_calls WHERE run_id=$1 AND id=$2").bind(&run.id.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)? else { return Ok(None); };
    let call: ToolCallRecord = decode(row.try_get("data").map_err(unknown)?)?;
    if call.id != *id
        || call.run_id != run.id
        || row
            .try_get::<Option<String>, _>("status")
            .map_err(unknown)?
            .as_deref()
            != Some(super::super::call_status(call.status))
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(Some(call))
}
pub(in super::super) async fn declared(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    name: &str,
) -> Result<bool, Error> {
    let binding = super::super::execution_binding::read(tx, &run.session_id, &run.id)
        .await
        .map_err(unknown)?;
    Ok(binding
        .as_ref()
        .and_then(|b| b.capsule.input_envelope.as_ref())
        .is_some_and(|input| input.frontend_tools().iter().any(|tool| tool.name == name)))
}
pub(super) async fn binding(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &FrontendCallStaging,
) -> Result<(), Error> {
    let binding = super::super::execution_binding::read(tx, &run.session_id, &run.id)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    crate::frontend_continuation::verify_declaration(
        request,
        binding
            .capsule
            .input_envelope
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?,
    )?;
    let row = sqlx::query("SELECT CASE WHEN octet_length(snapshot_record)<=4194304 THEN snapshot_record END AS data FROM messages WHERE session_id=$1 AND run_id=$2 AND id=$3").bind(&run.session_id.0).bind(&run.id.0).bind(&request.message_id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::SemanticConflict)?;
    let message: Message = decode(row.try_get("data").map_err(unknown)?)?;
    crate::frontend_continuation::verify_assistant_source(request, &message)
}
