//! Frontend calls extend the same exact compound barrier and native ownership.
use super::{Error, FrontendOriginRecord, origin, read, supported, unknown};
use crate::public_session::v2::proof::{Proof, ToolReadError};
use crate::{
    FrozenFrontendCall, PhaseSnapshot, ToolContinuationSuspension,
    tool_continuation::ToolContinuationRecord,
};
use crabber_core::{Run, ToolCallRecord, ToolCallStatus};
use sqlx::{Postgres, Row, Transaction};
fn verify_frozen(record: &FrontendOriginRecord, frozen: &FrozenFrontendCall) -> Result<(), Error> {
    if record.request.descriptor != frozen.descriptor
        || record.request.descriptor_fingerprint != frozen.descriptor_fingerprint
        || record.request.call.arguments != frozen.arguments
        || crate::phase::call_binding(&record.request.call).map_err(unknown)? != frozen.call_binding
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(in super::super) async fn pending_call(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    call: &ToolCallRecord,
    batch: &ToolContinuationSuspension,
    latest: Option<&PhaseSnapshot>,
    waiting: bool,
) -> Result<bool, Error> {
    if crate::POSTGRES_SCHEMA_VERSION < 14 {
        batch.require_frontend_support(false)?;
        return Ok(false);
    }
    let saved = origin(tx, run, &call.id).await?;
    let frozen = batch.frontend.iter().find(|f| f.target.call_id == call.id);
    let Some(frozen) = frozen else {
        return if saved.is_some() {
            Err(Error::SemanticConflict)
        } else {
            Ok(false)
        };
    };
    verify_frozen(&saved.ok_or(Error::UnknownStoreOutcome)?, frozen)?;
    let occupied: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_phases WHERE id=$1)")
        .bind(&frozen.target.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if latest.is_some()
        || occupied
        || call.result.is_some()
        || call.status
            != if waiting {
                ToolCallStatus::Waiting
            } else {
                ToolCallStatus::Pending
            }
        || batch
            .phases
            .iter()
            .any(|phase| phase.previous.call_id == call.id)
    {
        return Err(Error::CleanupBlocked);
    }
    Ok(true)
}
pub(in super::super) async fn verify_batch(
    tx: &mut Transaction<'_, Postgres>,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    verify_batch_with_proof(tx, record, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in super::super) async fn verify_batch_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), ToolReadError> {
    if crate::POSTGRES_SCHEMA_VERSION < 14 {
        record.batch.require_frontend_support(false)?;
        return Ok(());
    }
    if !record.batch.frontend.is_empty() {
        let run = read::scope_with_proof(tx, &record.session_id, &record.run_id, proof).await?;
        for frozen in &record.batch.frontend {
            let saved = read::origin_with_proof(tx, &run, &frozen.target.call_id, proof)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            verify_frozen(&saved, frozen)?;
        }
    }
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(LINK_COST).bind(&record.batch.continuation.id.0),
        proof,
    )
    .await
    .map_err(ToolReadError::Budget)?;
    let rows = sqlx::query("SELECT CASE WHEN octet_length(run_id)<=128 THEN run_id END AS run_id,CASE WHEN octet_length(call_id)<=512 THEN call_id END AS call_id,CASE WHEN octet_length(phase_id)<=128 THEN phase_id END AS phase_id FROM tool_continuation_frontend_calls WHERE continuation_id=$1 ORDER BY call_id LIMIT 65").bind(&record.batch.continuation.id.0).fetch_all(&mut **tx).await.map_err(unknown)?;
    if rows.len() != record.batch.frontend.len() {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let mut seen = std::collections::BTreeSet::new();
    for row in rows {
        let run: Option<String> = row.try_get("run_id").map_err(unknown)?;
        let call: String = row
            .try_get::<Option<String>, _>("call_id")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        let phase: String = row
            .try_get::<Option<String>, _>("phase_id")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        if run.as_deref() != Some(record.run_id.0.as_str())
            || !seen.insert(call.clone())
            || !record
                .batch
                .frontend
                .iter()
                .any(|f| f.target.call_id.0 == call && f.target.id.0 == phase)
        {
            return Err(Error::UnknownStoreOutcome.into());
        }
    }
    Ok(())
}
pub(in super::super) async fn save_links(
    tx: &mut Transaction<'_, Postgres>,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    if record.batch.frontend.is_empty() {
        return Ok(());
    }
    supported()?;
    for frozen in &record.batch.frontend {
        sqlx::query("INSERT INTO tool_continuation_frontend_calls(continuation_id,run_id,call_id,phase_id) VALUES($1,$2,$3,$4)")
            .bind(&record.batch.continuation.id.0).bind(&record.run_id.0).bind(&frozen.target.call_id.0).bind(&frozen.target.id.0)
            .execute(&mut **tx).await.map_err(unknown)?;
    }
    Ok(())
}
pub(in super::super) async fn mark_waiting(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    batch: &ToolContinuationSuspension,
) -> Result<(), Error> {
    for frozen in &batch.frontend {
        let mut call = super::super::phase::load_call(tx, run, &frozen.target.call_id)
            .await
            .map_err(unknown)?;
        call.status = ToolCallStatus::Waiting;
        super::super::phase::save_host_call(tx, &call)
            .await
            .map_err(unknown)?;
    }
    Ok(())
}

pub(in super::super) fn recover_call(
    call: &ToolCallRecord,
    record: &ToolContinuationRecord,
    latest: Option<&PhaseSnapshot>,
) -> Result<bool, Error> {
    let Some(frozen) = record
        .batch
        .frontend
        .iter()
        .find(|f| f.target.call_id == call.id)
    else {
        return Ok(false);
    };
    let native = latest.ok_or(Error::CleanupBlocked)?;
    if native.target != frozen.target
        || native.call_binding != frozen.call_binding
        || native.state != crate::PhaseState::Claimed
        || native.quiescence != crate::PhaseQuiescence::Pending
    {
        return Err(Error::CleanupBlocked);
    }
    Ok(true)
}

const LINK_COST: &str = r"SELECT CAST(COALESCE(SUM(bytes),0) AS BIGINT) FROM (SELECT octet_length(run_id)+octet_length(call_id)+octet_length(phase_id) AS bytes FROM tool_continuation_frontend_calls WHERE continuation_id=$1 ORDER BY call_id LIMIT 65) proof_links";
