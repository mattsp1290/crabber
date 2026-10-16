//! Accepted waiting cancellation with independent original suspension facts.
use super::super::super::{OperationReceipt, State, run_control, tool_continuation};
use crate::attempt::terminal::TerminalFacts;
use crate::public_session::{
    budget::Budget,
    v2::proof::{Failure, Proof, ReadError, unknown},
};
use crate::run_control::terminal::{self, Source};
use crabber_core::{EventKind, Run};
pub(super) fn verify(state: &State, run: &Run, proof: &mut Proof) -> Result<bool, Failure> {
    let mut safe = Budget::new(32 << 20, crate::PublicSessionLimit::ProofBytes);
    let controls = run_control::public_controls(state, run, 64, &mut safe, proof)?;
    let mut verified = false;
    for summary in controls {
        let audit = state
            .events
            .get(summary.event_cursor)
            .ok_or_else(|| unknown("cancel audit"))?;
        proof.measure(audit)?;
        if audit.payload.get("terminal_version").is_none() {
            continue;
        }
        let source: Source = serde_json::from_value(
            audit
                .payload
                .get("terminal")
                .cloned()
                .ok_or_else(|| unknown("cancel source"))?,
        )
        .map_err(unknown)?;
        let cursor = source
            .settlement
            .cursor
            .ok_or_else(|| unknown("cancel settlement cursor"))?;
        let original = state
            .events
            .get(cursor)
            .ok_or_else(|| unknown("cancel original settlement"))?;
        proof.measure(original)?;
        let latest = state.events.iter().rev().find(|event| {
            event.session_id == run.session_id
                && event.run_id == run.id
                && event.kind == EventKind::RunSettled
                && event
                    .cursor
                    .is_some_and(|cursor| cursor < summary.event_cursor)
        });
        if latest != Some(original) {
            return Err(unknown("latest cancel settlement"));
        }
        proof.measure(&(&run.error, &run.usage, run.status))?;
        let facts = TerminalFacts {
            session: &run.session_id,
            run: &run.id,
            status: run.status,
            error: run.error.as_deref(),
            usage: &run.usage,
        };
        let record = state
            .tool_continuations
            .values()
            .filter(|record| record.session_id == run.session_id && record.run_id == run.id)
            .max_by_key(|record| record.batch.continuation.generation);
        let record = record
            .map(|record| {
                tool_continuation::checked_record_with_proof(
                    state,
                    &record.batch.continuation.id,
                    proof,
                )
                .map_err(ReadError::public)
            })
            .transpose()?;
        if let Some(record) = record.as_ref().filter(|record| {
            record
                .termination
                .as_ref()
                .is_some_and(|termination| termination.original == source.request)
        }) {
            terminal::verify_scope(
                facts,
                &summary,
                audit,
                original,
                terminal::FrozenFacts {
                    usage: &record.batch.usage,
                    turn: record.batch.source_turn_id.as_ref(),
                },
            )
            .map_err(unknown)?;
        } else {
            let frozen = suspension(state, run, proof)?;
            terminal::verify_scope(facts, &summary, audit, original, frozen).map_err(unknown)?;
        }
        verified = true;
    }
    Ok(verified)
}

fn suspension<'a>(
    state: &'a State,
    run: &Run,
    proof: &mut Proof,
) -> Result<terminal::FrozenFacts<'a>, Failure> {
    let paused = state
        .suspension_proofs
        .get(&run.id)
        .ok_or_else(|| unknown("original suspension"))?;
    proof.measure(paused)?;
    let latest = state
        .attempts
        .values()
        .filter(|record| record.snapshot.run_id == run.id)
        .max_by_key(|record| record.snapshot.target.generation)
        .ok_or_else(|| unknown("suspended attempt"))?;
    proof.measure(&(
        &latest.snapshot,
        &latest.owner,
        &latest.token,
        &latest.run_token,
        latest.lease_until,
    ))?;
    latest.validate().map_err(unknown)?;
    let Some(OperationReceipt::Attempt(receipt)) = state
        .operation_receipts
        .get(&(run.session_id.clone(), paused.receipt.operation_id.clone()))
    else {
        return Err(unknown("suspension original receipt"));
    };
    proof.measure(receipt)?;
    let event = state
        .events
        .get(receipt.event_cursor)
        .ok_or_else(|| unknown("suspension original audit"))?;
    proof.measure(event)?;
    let boundary = state
        .events
        .iter()
        .rev()
        .find(|event| {
            event.session_id == run.session_id
                && event
                    .cursor
                    .is_some_and(|cursor| cursor < receipt.event_cursor)
        })
        .ok_or_else(|| unknown("original paused source"))?;
    proof.measure(boundary)?;
    paused
        .history_scope(
            &run.session_id,
            &run.id,
            &latest.snapshot,
            receipt,
            event,
            boundary,
        )
        .map_err(unknown)
}

#[cfg(test)]
#[path = "waiting_tests.rs"]
mod tests;
