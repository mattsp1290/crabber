//! Native storage readiness is only one part of deletion. An empty local owner
//! collection never supplies the missing preparation-to-Run ownership witness.
use super::super::{State, approval, queue, run_control, tool_continuation};
use crate::deletion::coverage::{MAX_PROOF_BYTES, VerifiedDeletionCoverage};
use crate::public_session::{
    budget::Budget,
    v2::proof::{Failure, Proof, ReadError, unknown},
};
use crate::{DeletionBlocker as Blocker, DeletionError as Error, PublicSessionLimit};
use crabber_core::{SessionId, SessionRevision};

pub(super) fn read(
    state: &State,
    coverage: &VerifiedDeletionCoverage,
    proof: &mut Proof,
) -> Result<Option<Blocker>, Error> {
    match verify(state, coverage, proof) {
        Ok(value) => Ok(value),
        Err(Failure::Limited(_)) => Ok(Some(Blocker::ProofLimit)),
        Err(Failure::Error(_)) => Err(Error::UnknownStoreOutcome),
    }
}
fn verify(
    state: &State,
    coverage: &VerifiedDeletionCoverage,
    proof: &mut Proof,
) -> Result<Option<Blocker>, Failure> {
    let session = coverage.session_id();
    if !proof.active() {
        return Err(unknown("deletion requires bounded proof"));
    }
    // Positive-origin reads have already happened. They consume this same
    // whole-operation allowance before even the first native carrier is read.
    proof.charge(coverage.proof_bytes())?;
    let current = state
        .sessions
        .get(session)
        .ok_or_else(|| unknown("missing session"))?;
    proof.measure(current)?;
    if current.id != *session
        || state.tombstones.contains_key(session)
        || state
            .public_revisions
            .get(session)
            .copied()
            .unwrap_or(SessionRevision::INITIAL)
            != coverage.revision()
    {
        return Err(unknown("coverage scope/revision"));
    }
    let mut has_runs = false;
    for (id, run) in &state.runs {
        if &run.session_id != session {
            continue;
        }
        proof.measure(&(id, run))?;
        if id != &run.id {
            return Err(unknown("run routing"));
        }
        if !run.status.is_terminal() {
            return Ok(Some(Blocker::Runtime));
        }
        has_runs = true;
        for ((scope, run_id, sequence), issuance) in &state.native_run_issuances {
            if scope == session && run_id == id {
                proof.measure(&(scope, run_id, sequence, issuance))?;
            }
        }
        super::super::native_parent::verify_run(state, run).map_err(unknown)?;
    }
    super::owned::verify(state, session, proof)?;
    let unproven_run_sources = super::run_history::verify(state, session, proof)?;
    if native_attempts(state, session, proof)? {
        return Ok(Some(Blocker::Attempt));
    }
    if super::native::phases(state, session, proof)? {
        return Ok(Some(Blocker::Phase));
    }
    if continuations(state, session, proof)? {
        return Ok(Some(Blocker::Continuation));
    }
    super::native::suspended(state, session)?;
    let mut safe = Budget::new(MAX_PROOF_BYTES, PublicSessionLimit::HeaderBytes);
    let pending = queue::public_pending(state, session, 64, &mut safe, proof)?;
    if !pending.is_empty() {
        return Ok(Some(Blocker::Queue));
    }
    for row in &state.inbox {
        if &row.session_id != session {
            continue;
        }
        proof.measure(&(&row.session_id, &row.message, &row.consumed_by_run))?;
        if row.message.session_id != *session {
            return Err(unknown("input routing"));
        }
        if row.consumed_by_run.is_none() {
            return Ok(Some(Blocker::PendingInput));
        }
    }
    for run in state.runs.values().filter(|run| &run.session_id == session) {
        let controls = run_control::public_controls(state, run, 64, &mut safe, proof)?;
        if controls.iter().any(|control| {
            matches!(
                control.status,
                crate::RunControlStatus::Accepted | crate::RunControlStatus::Observed
            )
        }) {
            return Ok(Some(Blocker::PendingControl));
        }
    }
    let owners = super::owners::read(state, session, &mut safe, proof)?;
    if !owners.live.is_empty() {
        return Ok(Some(Blocker::SessionOwner));
    }
    super::imported::verify(state, session, proof)?;
    // A creation/fork marker does not cover later bare native admissions or
    // generic Joined owner history. Sealed native preparation associations and
    // typed positive resource/dispatch join proofs are still required.
    Ok(if has_runs || unproven_run_sources || owners.unclassified {
        Some(Blocker::OwnershipUnknown)
    } else {
        None
    })
}
fn native_attempts(state: &State, session: &SessionId, proof: &mut Proof) -> Result<bool, Failure> {
    super::native::membership(
        state,
        session,
        &crabber_core::EventKind::ExecutionAttemptChanged,
        proof,
    )?;
    let mut blocked = false;
    for (id, record) in &state.attempts {
        let snapshot = &record.snapshot;
        let run_matches = state
            .runs
            .get(&snapshot.run_id)
            .is_some_and(|run| &run.session_id == session);
        if &snapshot.session_id != session && !run_matches {
            continue;
        }
        proof.measure(&(
            id,
            snapshot,
            &record.owner,
            &record.token,
            &record.run_token,
            record.lease_until,
        ))?;
        if &snapshot.session_id != session || !run_matches || id != &snapshot.target.id {
            return Err(unknown("attempt scope"));
        }
        record.validate().map_err(unknown)?;
        let event = state
            .events
            .iter()
            .rev()
            .find(|event| {
                &event.session_id == session
                    && event.run_id == snapshot.run_id
                    && event.kind == crabber_core::EventKind::ExecutionAttemptChanged
                    && event
                        .payload
                        .pointer("/attempt/target/id")
                        .and_then(serde_json::Value::as_str)
                        == Some(&id.0)
            })
            .ok_or_else(|| unknown("attempt source"))?;
        proof.measure(event)?;
        let operation = event
            .correlation
            .as_ref()
            .ok_or_else(|| unknown("attempt correlation"))?;
        crate::attempt::validate_identifier(operation).map_err(unknown)?;
        let Some(super::super::OperationReceipt::Attempt(receipt)) = state
            .operation_receipts
            .get(&(session.clone(), crabber_core::ControlId(operation.clone())))
        else {
            return Err(unknown("attempt receipt"));
        };
        proof.measure(receipt)?;
        crate::attempt::verify_receipt(receipt, event).map_err(unknown)?;
        if receipt.attempt != *snapshot {
            return Err(unknown("attempt rollback"));
        }
        blocked |= snapshot.cleanup != crate::AttemptCleanup::Complete
            || matches!(
                snapshot.state,
                crate::AttemptState::Unstarted
                    | crate::AttemptState::Started
                    | crate::AttemptState::OutcomeUnknown
            );
    }
    Ok(blocked)
}

#[cfg(test)]
#[path = "readiness_tests.rs"]
mod tests;

fn continuations(state: &State, session: &SessionId, proof: &mut Proof) -> Result<bool, Failure> {
    // Current proof checks independently select immutable membership before any
    // Released filter. Clearing a private record/barrier cannot manufacture quiet.
    approval::current::session_with_proof(state, session, proof).map_err(ReadError::public)?;
    let mut active = state.approval_barriers.contains_key(session);
    active |= state.approvals.values().any(|record| {
        &record.session_id == session && record.state != crate::ApprovalContinuationState::Released
    });
    let mut count = 0usize;
    for (id, record) in &state.tool_continuations {
        if &record.session_id != session {
            continue;
        }
        let checked = tool_continuation::checked_record_with_proof(state, id, proof)
            .map_err(ReadError::public)?;
        if checked.batch.continuation.id != *id {
            return Err(unknown("tool routing"));
        }
        active |= checked.state != crate::ToolContinuationState::Released;
        count = count
            .checked_add(1)
            .ok_or_else(|| unknown("continuation count"))?;
    }
    if count
        != state
            .events
            .iter()
            .filter(|event| &event.session_id == session && tool_continuation::scan::origin(event))
            .count()
    {
        return Err(unknown("continuation membership"));
    }
    if active || state.tool_barriers.contains_key(session) {
        return Ok(true);
    }
    Ok(false)
}
