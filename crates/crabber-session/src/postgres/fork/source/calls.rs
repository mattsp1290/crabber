//! Charge both selected raw-history and immutable-proof carriers before decoding.
use super::{Error, Failure, ForkSession, Proof, ToolCallRecord, unknown};
use crabber_core::{RunId, ToolCallId};
use sqlx::{Postgres, Row, Transaction};
pub(super) async fn read(
    tx: &mut Transaction<'_, Postgres>,
    request: &ForkSession,
    run: &RunId,
    id: &ToolCallId,
    proof: &mut Proof,
    usage: &mut crate::SnapshotUsage,
) -> Result<ToolCallRecord, Failure> {
    crate::fork::history::validate_id(&id.0)?;
    let row = sqlx::query(CALL)
        .bind(&request.source_session_id.0)
        .bind(&id.0)
        .bind(i64::try_from(proof.0.min(4 * 1024 * 1024)).map_err(unknown)?)
        .bind(
            i64::try_from(
                request
                    .limits
                    .encoded_bytes
                    .saturating_sub(usage.encoded_bytes),
            )
            .map_err(unknown)?,
        )
        .bind(
            i64::try_from(request.limits.text_bytes.saturating_sub(usage.text_bytes))
                .map_err(unknown)?,
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::Unsupported)?;
    let bytes = i64::from(row.try_get::<i32, _>("bytes").map_err(unknown)?);
    let bytes = usize::try_from(bytes).map_err(unknown)?;
    let text = usize::try_from(row.try_get::<i64, _>("text").map_err(unknown)?).map_err(unknown)?;
    if bytes > 4 * 1024 * 1024 {
        return Err(Error::UnknownStoreOutcome.into());
    }
    usage.tool_calls = usage.tool_calls.saturating_add(1);
    usage.encoded_bytes = usage.encoded_bytes.saturating_add(bytes);
    usage.text_bytes = usage.text_bytes.saturating_add(text);
    if let Some(limit) = usage.exceeded(request.limits) {
        return Err(Failure::RawLimited(limit));
    }
    proof.0 = proof.0.checked_sub(bytes).ok_or(Failure::Limited)?;
    let encoded = row
        .try_get::<Option<String>, _>("record")
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let call: ToolCallRecord = serde_json::from_str(&encoded).map_err(unknown)?;
    let accounting = crate::sql_snapshot::call_record(&call).map_err(unknown)?;
    if call.id != *id
        || call.run_id != *run
        || encoded.len() != bytes
        || usize::try_from(accounting.text).ok() != Some(text)
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(call)
}

const CALL: &str = r"SELECT octet_length(c.snapshot_record) AS bytes,c.snapshot_text AS text,CASE WHEN octet_length(c.snapshot_record)<=$3 AND octet_length(c.snapshot_record)<=$4 AND c.snapshot_text<=$5 THEN CASE WHEN c.id=c.snapshot_record::json->>'id' AND c.run_id=c.snapshot_record::json->>'run_id' AND c.status=c.snapshot_record::json->>'status' THEN c.snapshot_record END END AS record FROM tool_calls c JOIN runs r ON r.id=c.run_id WHERE r.session_id=$1 AND c.id=$2";
