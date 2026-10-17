//! Fresh termination proof only; bounded source selection precedes private carrier decoding.
use super::Error;
use crate::phase::PhaseRecord;
use crate::postgres::transaction::Transaction;
use crabber_core::{ContinuationId, Run, ToolCallRecord};
use sqlx::Postgres;
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}

pub(super) async fn validate(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    old: &PhaseRecord,
    call: &ToolCallRecord,
) -> Result<(), Error> {
    let key: Option<String> = sqlx::query_scalar(r"SELECT CASE WHEN octet_length(continuation_id)<=128 THEN continuation_id END FROM tool_continuation_phases WHERE run_id=$1 AND phase_id=$2").bind(&run.id.0).bind(&old.snapshot.target.id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let origin = super::super::tool_continuation::read::load(
        tx,
        &run.session_id,
        &run.id,
        Some(&ContinuationId(key.ok_or(Error::UnknownStoreOutcome)?)),
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::UnknownStoreOutcome)?;
    let latest = super::super::phase::latest_host_phase(tx, &run.id, &call.id)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let next = super::super::phase::load_owned(tx, &run.id, &latest.target.id)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if next.snapshot != latest {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::approval::terminal_history::origin(run, old, &next, &origin, call)?;
    super::super::phase::terminal_history::validate(tx, run, &next, call)
        .await
        .map_err(unknown)
}
