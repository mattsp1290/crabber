//! Exact current phase/call carriers, not a grant or a historical membership claim.
use super::{Error, Proof, unknown};
use crate::postgres::transaction::Transaction;
use crate::session_owner::driver::successor::tool_fresh as validate;
use crate::session_owner::preparation::pending_tool as choice;
use crabber_core::Run;
use sqlx::{Postgres, Row};
pub(super) async fn check(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    record: &crate::tool_continuation::ToolContinuationRecord,
    recovered: bool,
    proof: &mut Proof,
) -> Result<(), Error> {
    proof.charge(8).map_err(unknown)?;
    let count: i64 = sqlx::query_scalar(super::super::tool::CALL_COUNT)
        .bind(&run.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if usize::try_from(count).map_err(unknown)? != record.batch.calls.len() {
        return Err(Error::Busy);
    }
    for frozen in &record.batch.calls {
        super::super::super::proof::charge(
            tx,
            super::super::approval::CALL_COST,
            &run.id.0,
            &frozen.target.call_id.0,
            proof,
        )
        .await?;
        let row = sqlx::query(super::super::approval::CALL)
            .bind(&run.id.0)
            .bind(&frozen.target.call_id.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?;
        let call: crabber_core::ToolCallRecord = serde_json::from_str(
            &row.try_get::<Option<String>, _>("record")
                .map_err(unknown)?
                .ok_or(Error::UnknownStoreOutcome)?,
        )
        .map_err(unknown)?;
        if call.id != frozen.target.call_id
            || row
                .try_get::<Option<String>, _>("status")
                .map_err(unknown)?
                .as_deref()
                != Some(match call.status {
                    crabber_core::ToolCallStatus::Pending => "pending",
                    crabber_core::ToolCallStatus::Waiting => "waiting",
                    _ => return Err(Error::Busy),
                })
        {
            return Err(Error::UnknownStoreOutcome);
        }
        choice::call(record, &call, recovered)?;
        let expected = if recovered {
            validate::expected_phases(&record.batch)
                .find(|(t, _)| t.call_id == call.id)
                .map(|(t, b)| (t, b, true))
        } else {
            record
                .batch
                .phases
                .iter()
                .find(|v| v.previous.call_id == call.id)
                .map(|v| (&v.previous, &v.call_binding, false))
        };
        latest(tx, run, &call.id, expected, proof).await?;
    }
    for frozen in &record.batch.phases {
        let old = crate::postgres::phase::load_with_proof(tx, &run.id, &frozen.previous.id, proof)
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?;
        validate::phase(run, &old, &frozen.previous, &frozen.call_binding, false)?;
    }
    proof.charge(8).map_err(unknown)?;
    let count: i64 = sqlx::query_scalar(super::super::tool::PHASE_COUNT)
        .bind(&run.id.0)
        .bind(&record.batch.next_attempt.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let expected = if recovered {
        record.batch.phases.len() + record.batch.frontend.len()
    } else {
        0
    };
    if usize::try_from(count).map_err(unknown)? != expected {
        return Err(Error::Busy);
    }
    Ok(())
}
async fn latest(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    call: &crabber_core::ToolCallId,
    expected: Option<(&crate::PhaseTarget, &crabber_core::InputFingerprint, bool)>,
    proof: &mut Proof,
) -> Result<(), Error> {
    super::super::super::proof::charge(tx, COST, &run.id.0, &call.0, proof).await?;
    let latest: Option<Option<String>> = sqlx::query_scalar(LATEST)
        .bind(&run.id.0)
        .bind(&call.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?;
    match (latest, expected) {
        (None, None) => Ok(()),
        (Some(Some(id)), Some((target, binding, current))) if id == target.id.0 => {
            let phase = crate::postgres::phase::load_with_proof(tx, &run.id, &target.id, proof)
                .await
                .map_err(unknown)?
                .ok_or(Error::NotFound)?;
            validate::phase(run, &phase, target, binding, current)
        }
        _ => Err(Error::Busy),
    }
}
const COST: &str = "SELECT CAST(COALESCE(SUM(octet_length(id)+8),0) AS BIGINT) FROM tool_phases WHERE run_id=$1 AND call_id=$2";
const LATEST: &str = "SELECT CASE WHEN octet_length(id)<=128 THEN id END FROM tool_phases WHERE run_id=$1 AND call_id=$2 ORDER BY generation DESC LIMIT 1";
