//! Compound suspension journals authenticate historical checkpoint phases.
use super::super::{run_ids, scan};
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Result, unknown};
use crate::{PhaseState, ToolContinuationState};
use crabber_core::{ContinuationId, PhaseId, RunId, SessionId};
use sqlx::{Postgres, Transaction};
pub(in super::super::super) async fn released_suspensions(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<()> {
    let mut after = String::new();
    loop {
        let runs = run_ids(tx, session, &after, proof).await?;
        if runs.is_empty() {
            return Ok(());
        }
        for run in runs {
            verify_run(tx, session, &run, proof).await?;
            after = run.0;
        }
    }
}
async fn verify_run(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<()> {
    let mut after = String::new();
    loop {
        let ids = scan::page(tx, session, Some(run), &after, scan::Kind::Phase, proof).await?;
        if ids.is_empty() {
            return Ok(());
        }
        for id in ids {
            let native = super::super::super::super::super::phase::load_with_proof(
                tx,
                run,
                &PhaseId(id.clone()),
                proof,
            )
            .await?
            .ok_or_else(|| unknown("missing historical phase"))?;
            if native.snapshot.state == PhaseState::Suspended {
                verify(tx, session, run, &native, proof).await?;
            }
            after = id;
        }
    }
}
async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    native: &crate::phase::PhaseRecord,
    proof: &mut Proof,
) -> Result<()> {
    let mapping = "SELECT continuation_id FROM tool_continuation_phases WHERE run_id=$1 AND phase_id=$2 UNION ALL SELECT continuation_id FROM tool_continuation_frontend_calls WHERE run_id=$1 AND phase_id=$2 LIMIT 2";
    let cost = format!(
        "SELECT CAST(COALESCE(SUM(octet_length(continuation_id)),0) AS BIGINT) FROM ({mapping}) bounded_mapping"
    );
    super::super::super::super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(&cost)
            .bind(&run.0)
            .bind(&native.snapshot.target.id.0),
        proof,
    )
    .await?;
    let query = format!(
        "SELECT CASE WHEN octet_length(continuation_id)<=128 THEN continuation_id END FROM ({mapping}) bounded_mapping"
    );
    let mut keys: Vec<Option<String>> = sqlx::query_scalar(&query)
        .bind(&run.0)
        .bind(&native.snapshot.target.id.0)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    if keys.len() != 1 {
        return Err(unknown("suspended source membership"));
    }
    let key = ContinuationId(
        keys.pop()
            .flatten()
            .ok_or_else(|| unknown("continuation routing"))?,
    );
    crate::attempt::validate_identifier(&key.0).map_err(unknown)?;
    let record = super::super::super::super::super::tool_continuation::read::load_with_proof(
        tx,
        session,
        run,
        Some(&key),
        proof,
    )
    .await?
    .ok_or_else(|| unknown("suspended source"))?;
    let backend = record.batch.phases.iter().any(|frozen| {
        frozen.previous == native.snapshot.target
            && frozen.call_binding == native.snapshot.call_binding
    });
    let frontend = record.batch.frontend.iter().any(|frozen| {
        frozen.target == native.snapshot.target
            && frozen.call_binding == native.snapshot.call_binding
    });
    if record.state != ToolContinuationState::Released || !(backend || frontend) {
        return Err(unknown("suspended phase ancestry"));
    }
    Ok(())
}
