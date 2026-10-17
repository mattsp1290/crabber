//! Original accepted cancellation and independently authenticated frozen pause.
use super::super::super::super::super::super as backend;
use super::{fields, source_cursor};
use crate::attempt::terminal::TerminalFacts;
use crate::public_session::{budget::Budget, v2::proof::Proof};
use crate::run_control::{
    self,
    terminal::{self, Source},
};
use crate::sql_deletion::readiness::{MAX_PROOF_BYTES, Result, unknown};
use crate::transaction::Transaction;
use crabber_core::{RunId, RunStatus, SessionId};
use sqlx::Sqlite;
pub(super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    status: RunStatus,
    proof: &mut Proof,
) -> Result<bool> {
    let mut safe = Budget::new(MAX_PROOF_BYTES, crate::PublicSessionLimit::ProofBytes);
    let controls =
        backend::run_control::public::controls(tx, session, run, status, 64, &mut safe, proof)
            .await?;
    let mut verified = false;
    for summary in controls {
        let audit = backend::read_proof::event_with_limit(
            tx,
            session,
            summary.event_cursor,
            false,
            32768,
            proof,
        )
        .await?;
        if audit.payload.get("terminal_version").is_none() {
            continue;
        }
        let source: Source = serde_json::from_value(
            audit
                .payload
                .get("terminal")
                .cloned()
                .ok_or_else(|| unknown("cancel terminal source"))?,
        )
        .map_err(unknown)?;
        let cursor = source
            .settlement
            .cursor
            .ok_or_else(|| unknown("cancel settlement cursor"))?;
        source_cursor(tx, session, run, summary.event_cursor, cursor).await?;
        let original =
            backend::read_proof::event_with_limit(tx, session, cursor, false, 32768, proof).await?;
        let fields = fields(tx, session, run, status, proof).await?;
        let facts = TerminalFacts {
            session,
            run,
            status,
            error: fields.error.as_deref(),
            usage: &fields.usage,
        };
        let tool = backend::tool_continuation::read::load_with_proof(tx, session, run, None, proof)
            .await?;
        if let Some(record) = tool.as_ref().filter(|record| {
            record
                .termination
                .as_ref()
                .is_some_and(|termination| termination.original == source.request)
        }) {
            terminal::verify_scope(
                facts,
                &summary,
                &audit,
                &original,
                terminal::FrozenFacts {
                    usage: &record.batch.usage,
                    turn: record.batch.source_turn_id.as_ref(),
                },
            )
            .map_err(unknown)?;
        } else {
            let paused = suspension(tx, session, run, proof).await?;
            let (latest, _) = backend::attempt::load_host_attempt_with_proof(tx, run, None, proof)
                .await?
                .ok_or_else(|| unknown("suspended attempt"))?;
            let receipt = backend::attempt::host_receipt_with_proof(
                tx,
                session,
                &paused.receipt.operation_id,
                proof,
            )
            .await?
            .ok_or_else(|| unknown("original suspended receipt"))?;
            let event =
                backend::read_proof::event(tx, session, receipt.event_cursor, false, proof).await?;
            let boundary =
                backend::read_proof::event(tx, session, receipt.event_cursor, true, proof).await?;
            let frozen = paused
                .history_scope(session, run, &latest.snapshot, &receipt, &event, &boundary)
                .map_err(unknown)?;
            terminal::verify_scope(facts, &summary, &audit, &original, frozen).map_err(unknown)?;
        }
        verified = true;
    }
    Ok(verified)
}
async fn suspension(
    tx: &mut Transaction<'_, Sqlite>,
    _session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<run_control::SuspensionProof> {
    backend::read_proof::charge(tx, sqlx::query_scalar(COST).bind(&run.0), proof).await?;
    let text: Option<String> = sqlx::query_scalar(RECORD)
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .flatten();
    serde_json::from_str(&text.ok_or_else(|| unknown("historical suspension"))?).map_err(unknown)
}
const COST: &str =
    "SELECT length(CAST(data_json AS BLOB)) FROM run_suspension_proofs WHERE run_id=?1";
const RECORD: &str = "SELECT CASE WHEN length(CAST(data_json AS BLOB))<=4227072 THEN data_json END FROM run_suspension_proofs WHERE run_id=?1";

#[cfg(test)]
mod tests;
