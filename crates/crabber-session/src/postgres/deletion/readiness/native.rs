//! Native stages use narrow facts and independently anchored retained records.
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Result, blocked, unknown};
use crate::{AttemptCleanup, AttemptState, DeletionBlocker};
use crabber_core::{AttemptId, RunId, RunStatus, SessionId};
use sqlx::Postgres;
mod phases;
mod runs;
mod scan;
mod source;
pub(super) async fn run_ids(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    after: &str,
    proof: &mut Proof,
) -> Result<Vec<RunId>> {
    Ok(scan::page(tx, session, None, after, scan::Kind::Run, proof)
        .await?
        .into_iter()
        .map(RunId)
        .collect())
}
pub(super) async fn runtime(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<bool> {
    let mut unproven = false;
    let mut after = String::new();
    loop {
        let ids = run_ids(tx, session, &after, proof).await?;
        if ids.is_empty() {
            return Ok(unproven);
        }
        for id in ids {
            let facts = super::super::super::run_facts::read(tx, session, &id, proof).await?;
            if !matches!(
                facts.status,
                RunStatus::Completed | RunStatus::Failed | RunStatus::Interrupted
            ) {
                return Err(blocked(DeletionBlocker::Runtime));
            }
            unproven |= !runs::verify(tx, session, &id, facts.status, proof)
                .await?
                .complete();
            after = id.0;
        }
    }
}
pub(super) async fn attempts(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<bool> {
    source::membership(tx, session, false).await?;
    let mut after = String::new();
    let mut retained = false;
    loop {
        let runs = run_ids(tx, session, &after, proof).await?;
        if runs.is_empty() {
            return Ok(retained);
        }
        for run in runs {
            retained = true;
            let mut last = String::new();
            loop {
                let ids =
                    scan::page(tx, session, Some(&run), &last, scan::Kind::Attempt, proof).await?;
                if ids.is_empty() {
                    break;
                }
                for id in ids {
                    let id = AttemptId(id);
                    let (record, _) = super::super::super::attempt::load_host_attempt_with_proof(
                        tx,
                        &run,
                        Some(&id),
                        proof,
                    )
                    .await?
                    .ok_or_else(|| unknown("missing attempt"))?;
                    let event =
                        source::latest(tx, session, &run, &id.0, source::Kind::Attempt, proof)
                            .await?;
                    let operation = source::operation(&event)?;
                    let receipt = super::super::super::attempt::host_receipt_with_proof(
                        tx, session, &operation, proof,
                    )
                    .await?
                    .ok_or_else(|| unknown("missing attempt receipt"))?;
                    if receipt.attempt != record.snapshot
                        || event.cursor != Some(receipt.event_cursor)
                    {
                        return Err(unknown("attempt source"));
                    }
                    crate::attempt::verify_receipt(&receipt, &event).map_err(unknown)?;
                    if matches!(
                        record.snapshot.state,
                        AttemptState::Started | AttemptState::OutcomeUnknown
                    ) || record.snapshot.cleanup != AttemptCleanup::Complete
                    {
                        return Err(blocked(DeletionBlocker::Attempt));
                    }
                    last = id.0;
                }
            }
            after = run.0;
        }
    }
}
pub(super) use phases::{phases, released_suspensions};
