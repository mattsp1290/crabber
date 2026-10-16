use super::{Error, FrontendOriginRecord, FrontendStagingReceipt, unknown};
use crate::FrontendCallStaging;
use crate::frontend_continuation::FrontendReadScope;
use crate::public_session::v2::proof::{Proof, ToolReadError};
use crabber_core::{
    ControlId, EventCursor, EventRecord, Message, Run, RunId, SessionId, ToolCallId, ToolCallRecord,
};
use serde::de::DeserializeOwned;
use sqlx::{Row, Sqlite, Transaction};
fn decode<T: DeserializeOwned>(value: Option<String>) -> Result<T, Error> {
    serde_json::from_str(&value.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)
}
fn event(row: &sqlx::sqlite::SqliteRow) -> Result<EventRecord, Error> {
    let mut event: EventRecord = decode(row.try_get("event").map_err(unknown)?)?;
    event.cursor = Some(EventCursor(
        u64::try_from(row.try_get::<i64, _>("event_seq").map_err(unknown)?).map_err(unknown)?,
    ));
    Ok(event)
}
pub(super) async fn run(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &RunId,
) -> Result<Run, Error> {
    let query = "SELECT CASE WHEN length(CAST(data AS BLOB))<=4210688 THEN data END AS data,CASE WHEN length(CAST(status AS BLOB))<=32 THEN status END AS status,CASE WHEN length(CAST(claim_token AS BLOB))<=512 THEN claim_token END AS token,lease_until FROM runs WHERE session_id=$1 AND id=$2";
    let row = sqlx::query(query)
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
            != Some(super::super::records::status(run.status))
        || row
            .try_get::<Option<String>, _>("token")
            .map_err(unknown)?
            .as_deref()
            != Some(run.claim_token.as_str())
        || row.try_get::<i64, _>("lease_until").map_err(unknown)?
            != super::super::records::micros(run.lease_until)
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(run)
}
pub(super) async fn receipt(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<FrontendStagingReceipt>, Error> {
    receipt_with_proof(tx, session, id, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in super::super) async fn receipt_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<Option<FrontendStagingReceipt>, ToolReadError> {
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(RECEIPT_COST)
            .bind(&session.0)
            .bind(&id.0),
        proof,
    )
    .await
    .map_err(ToolReadError::Budget)?;
    crate::attempt::validate_identifier(&id.0).map_err(|_| Error::InvalidRequest)?;
    let Some(row) = sqlx::query("SELECT CASE WHEN o.kind='frontend_staging' THEN 'frontend_staging' ELSE 'foreign' END AS kind,CASE WHEN length(CAST(o.semantic_digest AS BLOB))<=64 THEN o.semantic_digest END AS digest,o.event_seq,CASE WHEN length(CAST(o.data_json AS BLOB))<=8192 THEN o.data_json END AS data,CASE WHEN o.kind='frontend_staging' AND length(CAST(e.data AS BLOB))<=65536 THEN e.data END AS event FROM operation_receipts o LEFT JOIN events e ON e.session_id=o.session_id AND e.seq=o.event_seq WHERE o.session_id=$1 AND o.operation_id=$2")
        .bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)? else { return Ok(None); };
    if row.try_get::<String, _>("kind").map_err(unknown)? != "frontend_staging" {
        return Err(Error::SemanticConflict.into());
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
        return Err(Error::UnknownStoreOutcome.into());
    }
    crate::frontend_continuation::verify_staging_receipt(&saved, &event(&row)?)?;
    Ok(Some(saved))
}
pub(super) async fn origin(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    call: &ToolCallId,
) -> Result<Option<FrontendOriginRecord>, Error> {
    let scope = FrontendReadScope::new(&run.session_id, &run.id)?;
    origin_scoped(tx, &scope, call).await
}
pub(in super::super) async fn origin_scoped(
    tx: &mut Transaction<'_, Sqlite>,
    run: &FrontendReadScope,
    call: &ToolCallId,
) -> Result<Option<FrontendOriginRecord>, Error> {
    origin_with_proof(tx, run, call, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in super::super) async fn origin_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &FrontendReadScope,
    call: &ToolCallId,
    proof: &mut Proof,
) -> Result<Option<FrontendOriginRecord>, ToolReadError> {
    let prefix = format!(
        "{{\"call_id\":{},\"frontend_origin\":",
        serde_json::to_string(call).map_err(unknown)?
    );
    // Match the canonical protected prefix before interpreting any payload JSON.
    // Arbitrary neighboring ToolCallPending data may contain escaped NULs.
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(SOURCE_COST)
            .bind(&run.session_id.0)
            .bind(&run.id.0)
            .bind(&prefix),
        proof,
    )
    .await
    .map_err(ToolReadError::Budget)?;
    let sources = sqlx::query(r#"SELECT seq AS event_seq,CASE WHEN length(CAST(data AS BLOB))<=65536 THEN data END AS event FROM events WHERE session_id=$1 AND run_id=$2 AND instr(data, ',"payload":')>0 AND substr(data,1,instr(data, ',"payload":')-1) GLOB '*,"kind":"tool_call_pending"' AND substr(data,instr(data, ',"payload":')+length(',"payload":'),length($3))=$3 ORDER BY seq LIMIT 2"#)
        .bind(&run.session_id.0).bind(&run.id.0).bind(prefix).fetch_all(&mut **tx).await.map_err(unknown)?;
    super::super::read_proof::charge(tx, sqlx::query_scalar(ORIGIN_COST).bind(&call.0), proof)
        .await
        .map_err(ToolReadError::Budget)?;
    let row = sqlx::query("SELECT CASE WHEN length(CAST(session_id AS BLOB))<=512 THEN session_id END AS session_id,CASE WHEN length(CAST(run_id AS BLOB))<=128 THEN run_id END AS run_id,format_version,CASE WHEN length(CAST(data_json AS BLOB))<=262144 THEN data_json END AS data FROM frontend_tool_origins WHERE call_id=$1")
        .bind(&call.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    let Some(row) = row else {
        if !sources.is_empty() || declared_call_with_proof(tx, run, call, proof).await? {
            return Err(Error::UnknownStoreOutcome.into());
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
        || row.try_get::<i64, _>("format_version").map_err(unknown)? != 1
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let record: FrontendOriginRecord = decode(row.try_get("data").map_err(unknown)?)?;
    if record.request.call.id != *call
        || record.request.session_id != run.session_id
        || record.request.run_id != run.id
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    record.verify(&event(&sources[0])?)?;
    if receipt_with_proof(tx, &run.session_id, &record.receipt.operation_id, proof)
        .await
        .map_err(|error| error.map_domain(unknown))?
        .as_ref()
        != Some(&record.receipt)
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    binding_with_proof(tx, run, &record.request, proof)
        .await
        .map_err(|error| error.map_domain(unknown))?;
    let current = load_call_with_proof(tx, run, call, proof)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    if crate::phase::call_binding(&current).map_err(unknown)?
        != crate::phase::call_binding(&record.request.call).map_err(unknown)?
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(Some(record))
}
async fn declared_call_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &FrontendReadScope,
    call: &ToolCallId,
    proof: &mut Proof,
) -> Result<bool, ToolReadError> {
    let Some(call) = load_call_with_proof(tx, run, call, proof).await? else {
        return Ok(false);
    };
    declared_with_proof(tx, run, &call.name, proof).await
}
async fn load_call_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &FrontendReadScope,
    id: &ToolCallId,
    proof: &mut Proof,
) -> Result<Option<ToolCallRecord>, ToolReadError> {
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(CALL_COST).bind(&run.id.0).bind(&id.0),
        proof,
    )
    .await
    .map_err(ToolReadError::Budget)?;
    let Some(row) = sqlx::query("SELECT CASE WHEN length(CAST(status AS BLOB))<=32 THEN status END AS status,CASE WHEN length(CAST(data AS BLOB))<=4194304 THEN data END AS data FROM tool_calls WHERE run_id=$1 AND id=$2").bind(&run.id.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)? else { return Ok(None); };
    let call: ToolCallRecord = decode(row.try_get("data").map_err(unknown)?)?;
    if call.id != *id
        || call.run_id != run.id
        || row
            .try_get::<Option<String>, _>("status")
            .map_err(unknown)?
            .as_deref()
            != Some(super::super::records::call_status(call.status))
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(Some(call))
}
pub(in super::super) async fn declared(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    name: &str,
) -> Result<bool, Error> {
    let scope = FrontendReadScope::new(&run.session_id, &run.id)?;
    declared_scoped(tx, &scope, name).await
}
pub(in super::super) async fn declared_scoped(
    tx: &mut Transaction<'_, Sqlite>,
    run: &FrontendReadScope,
    name: &str,
) -> Result<bool, Error> {
    declared_with_proof(tx, run, name, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in super::super) async fn declared_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &FrontendReadScope,
    name: &str,
    proof: &mut Proof,
) -> Result<bool, ToolReadError> {
    let binding =
        super::super::execution_binding::read_with_proof(tx, &run.session_id, &run.id, proof)
            .await?;
    Ok(binding
        .as_ref()
        .and_then(|b| b.capsule.input_envelope.as_ref())
        .is_some_and(|input| input.frontend_tools().iter().any(|tool| tool.name == name)))
}
pub(super) async fn binding(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &FrontendCallStaging,
) -> Result<(), Error> {
    let scope = FrontendReadScope::new(&run.session_id, &run.id)?;
    binding_scoped(tx, &scope, request).await
}
pub(in super::super) async fn binding_scoped(
    tx: &mut Transaction<'_, Sqlite>,
    run: &FrontendReadScope,
    request: &FrontendCallStaging,
) -> Result<(), Error> {
    binding_with_proof(tx, run, request, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in super::super) async fn binding_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &FrontendReadScope,
    request: &FrontendCallStaging,
    proof: &mut Proof,
) -> Result<(), ToolReadError> {
    let binding =
        super::super::execution_binding::read_with_proof(tx, &run.session_id, &run.id, proof)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
    crate::frontend_continuation::verify_declaration(
        request,
        binding
            .capsule
            .input_envelope
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?,
    )?;
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(MESSAGE_COST)
            .bind(&run.session_id.0)
            .bind(&run.id.0)
            .bind(&request.message_id.0),
        proof,
    )
    .await
    .map_err(ToolReadError::Budget)?;
    let row = sqlx::query("SELECT CASE WHEN length(CAST(data AS BLOB))<=4194304 THEN data END AS data FROM messages WHERE session_id=$1 AND run_id=$2 AND id=$3").bind(&run.session_id.0).bind(&run.id.0).bind(&request.message_id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::SemanticConflict)?;
    let message: Message = decode(row.try_get("data").map_err(unknown)?)?;
    crate::frontend_continuation::verify_assistant_source(request, &message).map_err(Into::into)
}

const RECEIPT_COST: &str = r###"SELECT CAST(COALESCE(length(CAST(o.data_json AS BLOB)),0)+COALESCE(length(CAST(o.semantic_digest AS BLOB)),0)+16+COALESCE(length(CAST(e.data AS BLOB)),0) AS BIGINT) FROM operation_receipts o LEFT JOIN events e ON e.session_id=o.session_id AND e.seq=o.event_seq WHERE o.session_id=$1 AND o.operation_id=$2"###;
const ORIGIN_COST: &str = r###"SELECT CAST(length(CAST(session_id AS BLOB))+length(CAST(run_id AS BLOB))+length(CAST(data_json AS BLOB)) AS BIGINT) FROM frontend_tool_origins WHERE call_id=$1"###;
const MESSAGE_COST: &str = r###"SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM messages WHERE session_id=$1 AND run_id=$2 AND id=$3"###;
const CALL_COST: &str = r###"SELECT CAST(length(CAST(data AS BLOB))+length(CAST(status AS BLOB)) AS BIGINT) FROM tool_calls WHERE run_id=$1 AND id=$2"###;
const SOURCE_COST: &str = r###"SELECT CAST(COALESCE(SUM(bytes),0) AS BIGINT) FROM (SELECT length(CAST(data AS BLOB)) AS bytes FROM events WHERE session_id=$1 AND run_id=$2 AND instr(data, ',"payload":')>0 AND substr(data,1,instr(data, ',"payload":')-1) GLOB '*,"kind":"tool_call_pending"' AND substr(data,instr(data, ',"payload":')+length(',"payload":'),length($3))=$3 ORDER BY seq LIMIT 2) proof_sources"###;

/// Validate narrow immutable routing and status coherence, without worker tokens,
/// lease, checkpoint, error or a fabricated Run.
pub(in super::super) async fn scope_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<FrontendReadScope, ToolReadError> {
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(SCOPE_COST).bind(&run.0).bind(&session.0),
        proof,
    )
    .await
    .map_err(ToolReadError::Budget)?;
    let row = sqlx::query(SCOPE)
        .bind(&run.0)
        .bind(&session.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    let scope = FrontendReadScope::new(session, run)?;
    if row
        .try_get::<Option<String>, _>("id")
        .map_err(unknown)?
        .as_deref()
        != Some(&run.0)
        || row
            .try_get::<Option<String>, _>("body_id")
            .map_err(unknown)?
            .as_deref()
            != Some(&run.0)
        || row
            .try_get::<Option<String>, _>("session")
            .map_err(unknown)?
            .as_deref()
            != Some(&session.0)
        || row
            .try_get::<Option<String>, _>("body_session")
            .map_err(unknown)?
            .as_deref()
            != Some(&session.0)
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let status: String = row
        .try_get::<Option<String>, _>("status")
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if row
        .try_get::<Option<String>, _>("body_status")
        .map_err(unknown)?
        .as_deref()
        != Some(status.as_str())
        || serde_json::from_value::<crabber_core::RunStatus>(serde_json::Value::String(status))
            .is_err()
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(scope)
}
const SCOPE: &str = r###"SELECT CASE WHEN length(CAST(id AS BLOB))<=128 THEN id END AS id,CASE WHEN length(CAST(session_id AS BLOB))<=512 THEN session_id END AS session,CASE WHEN length(CAST(status AS BLOB))<=32 THEN status END AS status,CASE WHEN length(CAST(json_extract(data,'$.id') AS BLOB))<=128 THEN json_extract(data,'$.id') END AS body_id,CASE WHEN length(CAST(json_extract(data,'$.session_id') AS BLOB))<=512 THEN json_extract(data,'$.session_id') END AS body_session,CASE WHEN length(CAST(json_extract(data,'$.status') AS BLOB))<=32 THEN json_extract(data,'$.status') END AS body_status FROM runs WHERE id=$1 AND session_id=$2"###;
const SCOPE_COST: &str = r###"SELECT CAST(COALESCE(length(CAST(id AS BLOB)),0)+COALESCE(length(CAST(session_id AS BLOB)),0)+COALESCE(length(CAST(status AS BLOB)),0)+COALESCE(length(CAST(json_extract(data,'$.id') AS BLOB)),0)+COALESCE(length(CAST(json_extract(data,'$.session_id') AS BLOB)),0)+COALESCE(length(CAST(json_extract(data,'$.status') AS BLOB)),0) AS BIGINT) FROM runs WHERE id=$1 AND session_id=$2"###;
