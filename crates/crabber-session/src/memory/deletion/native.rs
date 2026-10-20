//! Borrowed historical phase proof selection. Current rows never select their
//! own alleged source operation, and all materialized evidence shares one cap.
use super::super::{OperationReceipt, State};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::{PhaseQuiescence, PhaseReceipt, PhaseState};
use crabber_core::{EventKind, EventRecord, SessionId, ToolCallStatus};

pub(super) fn phases(
    state: &State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    membership(state, session, &EventKind::ToolPhaseChanged, proof)?;
    let mut blocked = false;
    for (id, phase) in &state.phases {
        let snapshot = &phase.snapshot;
        let run_matches = state
            .runs
            .get(&snapshot.run_id)
            .is_some_and(|run| &run.session_id == session);
        let call = state.calls.get(&snapshot.target.call_id);
        let call_matches = call
            .and_then(|call| state.runs.get(&call.run_id))
            .is_some_and(|run| &run.session_id == session);
        if &snapshot.session_id != session && !run_matches && !call_matches {
            continue;
        }
        proof.measure(&(id, snapshot, &phase.token, &phase.output))?;
        let call = call.ok_or_else(|| unknown("orphan phase"))?;
        proof.measure(call)?;
        if &snapshot.session_id != session
            || !run_matches
            || call.run_id != snapshot.run_id
            || id != &snapshot.target.id
            || crate::phase::call_binding(call).map_err(unknown)? != snapshot.call_binding
        {
            return Err(unknown("phase scope/binding"));
        }
        phase.validate().map_err(unknown)?;
        if snapshot.state == PhaseState::Suspended {
            blocked |= snapshot.quiescence != PhaseQuiescence::Complete;
            continue;
        }
        // Compound continuation claims do not issue a standalone PhaseChanged;
        // they remain an honest blocker, rather than inventing a no-effect proof.
        if matches!(
            snapshot.state,
            PhaseState::Claimed | PhaseState::Started | PhaseState::Uncertain
        ) {
            blocked = true;
            continue;
        }
        let current = source(state, session, &snapshot.run_id, id, false, proof)?;
        let terminal = source(state, session, &snapshot.run_id, id, true, proof)?;
        let settled = state
            .events
            .iter()
            .rev()
            .find(|event| {
                event.session_id == *session
                    && event.run_id == snapshot.run_id
                    && event.cursor < terminal.1.cursor
            })
            .ok_or_else(|| unknown("settlement source"))?;
        proof.measure(settled)?;
        let quiescence = crate::phase::terminal_history::verify_history_scope(
            session,
            &snapshot.run_id,
            phase,
            call,
            current,
            terminal,
            settled,
        )
        .map_err(unknown)?;
        blocked |= quiescence != PhaseQuiescence::Complete;
    }
    for (id, call) in &state.calls {
        if state
            .runs
            .get(&call.run_id)
            .is_none_or(|run| &run.session_id != session)
        {
            continue;
        }
        proof.measure(&(id, call))?;
        if id != &call.id {
            return Err(unknown("call routing"));
        }
        if matches!(
            call.status,
            ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
        ) {
            blocked = true;
        } else {
            crate::attempt_control::validate_logical(call).map_err(unknown)?;
        }
    }
    Ok(blocked)
}
fn source<'a>(
    state: &'a State,
    session: &SessionId,
    run: &crabber_core::RunId,
    phase: &crabber_core::PhaseId,
    terminal: bool,
    proof: &mut Proof,
) -> Result<(&'a PhaseReceipt, &'a EventRecord), Failure> {
    let event = state
        .events
        .iter()
        .rev()
        .find(|event| {
            event.session_id == *session
                && event.run_id == *run
                && event.kind == EventKind::ToolPhaseChanged
                && event
                    .payload
                    .pointer("/phase/target/id")
                    .and_then(serde_json::Value::as_str)
                    == Some(&phase.0)
                && (!terminal
                    || matches!(
                        event
                            .payload
                            .get("operation")
                            .and_then(serde_json::Value::as_str),
                        Some("complete" | "resolve_success" | "resolve_failure")
                    ))
        })
        .ok_or_else(|| unknown("phase source"))?;
    proof.measure(event)?;
    let operation = event
        .correlation
        .as_ref()
        .ok_or_else(|| unknown("phase correlation"))?;
    crate::attempt::validate_identifier(operation).map_err(unknown)?;
    let Some(OperationReceipt::Phase(receipt)) = state
        .operation_receipts
        .get(&(session.clone(), crabber_core::ControlId(operation.clone())))
    else {
        return Err(unknown("phase receipt"));
    };
    proof.measure(receipt)?;
    Ok((receipt, event))
}
/// Called only after every continuation's original and current proofs have been
/// authenticated with the same budget. Native suspended history has no separate
/// phase receipt: the exact compound frozen suspension is its source.
pub(super) fn suspended(state: &State, session: &SessionId) -> Result<(), Failure> {
    for record in state
        .tool_continuations
        .values()
        .filter(|record| &record.session_id == session)
    {
        for frozen in &record.batch.phases {
            let native = state
                .phases
                .get(&frozen.previous.id)
                .ok_or_else(|| unknown("erased frozen native phase"))?;
            if native.snapshot.target != frozen.previous
                || native.snapshot.call_binding != frozen.call_binding
                || native.snapshot.session_id != *session
                || native.snapshot.run_id != record.run_id
                || native.snapshot.state != PhaseState::Suspended
            {
                return Err(unknown("frozen native phase ancestry"));
            }
        }
    }
    for phase in state.phases.values().filter(|phase| {
        &phase.snapshot.session_id == session && phase.snapshot.state == PhaseState::Suspended
    }) {
        let snapshot = &phase.snapshot;
        let mut matches = 0usize;
        for record in state
            .tool_continuations
            .values()
            .filter(|record| &record.session_id == session && record.run_id == snapshot.run_id)
        {
            for frozen in &record.batch.phases {
                if frozen.previous == snapshot.target
                    && frozen.call_binding == snapshot.call_binding
                    && record.state == crate::ToolContinuationState::Released
                {
                    matches += 1;
                }
            }
        }
        if matches != 1 {
            return Err(unknown("suspended phase compound ancestry"));
        }
    }
    Ok(())
}

pub(super) fn membership(
    state: &State,
    session: &SessionId,
    kind: &EventKind,
    proof: &mut Proof,
) -> Result<(), Failure> {
    for event in state
        .events
        .iter()
        .filter(|event| &event.session_id == session && &event.kind == kind)
    {
        let path = if *kind == EventKind::ToolPhaseChanged {
            "/phase/target/id"
        } else {
            "/attempt/target/id"
        };
        let id = event
            .payload
            .pointer(path)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| unknown("native membership source"))?;
        proof.measure(&(event.cursor, &event.session_id, &event.run_id, id))?;
        crate::attempt::validate_identifier(id).map_err(unknown)?;
        let present = if *kind == EventKind::ToolPhaseChanged {
            state
                .phases
                .get(&crabber_core::PhaseId(id.into()))
                .is_some_and(|record| {
                    record.snapshot.session_id == *session && record.snapshot.run_id == event.run_id
                })
        } else {
            state
                .attempts
                .get(&crabber_core::AttemptId(id.into()))
                .is_some_and(|record| {
                    record.snapshot.session_id == *session && record.snapshot.run_id == event.run_id
                })
        };
        if !present {
            return Err(unknown("erased native history"));
        }
    }
    Ok(())
}
