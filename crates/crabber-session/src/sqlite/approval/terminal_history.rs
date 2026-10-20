//! Fresh termination proof only; bounded source selection precedes private carrier decoding.
use super::Error;
use crate::phase::PhaseRecord;
use crate::transaction::Transaction;
use crabber_core::{ContinuationId, Run, ToolCallRecord};
use sqlx::Sqlite;
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}

pub(super) async fn validate_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    old: &PhaseRecord,
    call: &ToolCallRecord,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    super::super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(length(CAST(continuation_id AS BLOB)) AS BIGINT) FROM tool_continuation_phases WHERE run_id=$1 AND phase_id=$2").bind(&run.id.0).bind(&old.snapshot.target.id.0), proof).await.map_err(unknown)?;
    let key: Option<String> = sqlx::query_scalar(r"SELECT CASE WHEN length(CAST(continuation_id AS BLOB))<=128 THEN continuation_id END FROM tool_continuation_phases WHERE run_id=?1 AND phase_id=?2").bind(&run.id.0).bind(&old.snapshot.target.id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let origin = super::super::tool_continuation::read::load_with_proof(
        tx,
        &run.session_id,
        &run.id,
        Some(&ContinuationId(key.ok_or(Error::UnknownStoreOutcome)?)),
        proof,
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::UnknownStoreOutcome)?;
    let latest = super::super::phase::latest_host_phase_with_proof(tx, &run.id, &call.id, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let next = super::super::phase::load_with_proof(tx, &run.id, &latest.target.id, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if next.snapshot != latest {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::approval::terminal_history::origin(run, old, &next, &origin, call)?;
    super::super::phase::terminal_history::validate_with_proof(tx, run, &next, call, proof)
        .await
        .map_err(unknown)
}
