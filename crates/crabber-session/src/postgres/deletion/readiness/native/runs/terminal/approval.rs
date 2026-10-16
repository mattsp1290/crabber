//! Original approval cancellation/expiry with independently frozen pause facts.
use super::super::super::super::super::super as backend;
use super::{fields, source_cursor};
use crate::approval::terminal;
use crate::attempt::terminal::TerminalFacts;
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Result, unknown};
use crabber_core::{RunId, RunStatus, SessionId};
use sqlx::{Postgres, Transaction};
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    status: RunStatus,
    proof: &mut Proof,
) -> Result<bool> {
    // This bounded loader authenticates original suspension, native conclusion,
    // exact frozen batch via verify_origin_at, and independent latest/current
    // sources under this same proof before returning any private presentation.
    let Some((record, _)) = backend::approval::load_with_proof(tx, session, run, proof).await?
    else {
        return Ok(false);
    };
    let rows = backend::approval::current::rows_with_proof(
        tx,
        &record,
        backend::approval::current::Select::Latest,
        proof,
    )
    .await?;
    let source = backend::approval::current::source(
        rows.first()
            .ok_or_else(|| unknown("latest approval source"))?,
    )
    .map_err(unknown)?;
    if !terminal::candidate(&source.event) {
        return Ok(false);
    }
    let saved = terminal::settlement(&source.receipt, &source.event).map_err(unknown)?;
    let cursor = saved
        .cursor
        .ok_or_else(|| unknown("approval settlement cursor"))?;
    source_cursor(tx, session, run, source.receipt.event_cursor, cursor).await?;
    let original =
        backend::read_proof::event_with_limit(tx, session, cursor, false, 32768, proof).await?;
    let fields = fields(tx, session, run, status, proof).await?;
    terminal::verify_scope(
        TerminalFacts {
            session,
            run,
            status,
            error: fields.error.as_deref(),
            usage: &fields.usage,
        },
        &source.receipt,
        &source.event,
        &original,
        &record.batch,
    )
    .map_err(unknown)?;
    Ok(true)
}

#[cfg(test)]
mod tests;
