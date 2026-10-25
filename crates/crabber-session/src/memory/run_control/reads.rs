use super::{Error, OperationReceipt, State};
use crate::run_control::{
    MAX_CONTROL_BYTES, MAX_CONTROLS, MAX_RUN_CONTROL_BYTES, Record, RunControlReceipt,
    RunControlSummary, RunControlTarget, accepted, verify_event,
};
use crabber_core::{ControlId, SessionId, SessionRevision};
pub(super) fn revision(state: &State, session: &SessionId) -> SessionRevision {
    state
        .public_revisions
        .get(session)
        .copied()
        .unwrap_or(SessionRevision::INITIAL)
}
pub(super) fn receipt(
    state: &State,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<RunControlReceipt>, Error> {
    match state.operation_receipts.get(&(session.clone(), id.clone())) {
        None => Ok(None),
        Some(OperationReceipt::RunControl(receipt)) => {
            if receipt.target.session_id != *session || receipt.operation_id != *id {
                return Err(Error::UnknownStoreOutcome);
            }
            verify_event(
                &accepted(receipt),
                state
                    .events
                    .get(receipt.event_cursor)
                    .ok_or(Error::UnknownStoreOutcome)?,
            )?;
            Ok(Some(receipt.clone()))
        }
        Some(_) => Err(Error::SemanticConflict),
    }
}
fn validate(state: &State, record: &Record) -> Result<(), Error> {
    record.validate()?;
    let saved = &record.summary.receipt;
    if receipt(state, &saved.target.session_id, &saved.operation_id)?.as_ref() != Some(saved) {
        return Err(Error::UnknownStoreOutcome);
    }
    let latest = state
        .run_control_audits
        .range(
            (
                saved.target.session_id.clone(),
                saved.operation_id.clone(),
                0,
            )
                ..=(
                    saved.target.session_id.clone(),
                    saved.operation_id.clone(),
                    u64::MAX,
                ),
        )
        .next_back()
        .map(|(_, summary)| summary)
        .ok_or(Error::UnknownStoreOutcome)?;
    if *latest != record.summary {
        return Err(Error::UnknownStoreOutcome);
    }
    verify_event(
        latest,
        state
            .events
            .get(latest.event_cursor)
            .ok_or(Error::UnknownStoreOutcome)?,
    )
}
pub(super) fn summaries(
    state: &State,
    target: &RunControlTarget,
) -> Result<Vec<RunControlSummary>, Error> {
    let mut summaries = Vec::new();
    let mut bytes = 0;
    for ((session, id), saved) in &state.operation_receipts {
        let OperationReceipt::RunControl(saved) = saved else {
            continue;
        };
        if session != &target.session_id {
            continue;
        }
        let source = state
            .events
            .get(saved.event_cursor)
            .ok_or(Error::UnknownStoreOutcome)?;
        if source.session_id != *session
            || source.correlation.as_deref() != Some(&id.0)
            || source.kind != crabber_core::EventKind::RunControlChanged
        {
            return Err(Error::UnknownStoreOutcome);
        }
        if source.run_id != target.run_id {
            if saved.target.run_id == target.run_id {
                return Err(Error::UnknownStoreOutcome);
            }
            continue;
        }
        if summaries.len() == MAX_CONTROLS {
            return Err(Error::UnknownStoreOutcome);
        }
        if saved.target != *target || receipt(state, session, id)?.as_ref() != Some(saved) {
            return Err(Error::UnknownStoreOutcome);
        }
        let record = state
            .run_controls
            .get(&(session.clone(), id.clone()))
            .ok_or(Error::UnknownStoreOutcome)?;
        let latest = state
            .run_control_audits
            .range((session.clone(), id.clone(), 0)..=(session.clone(), id.clone(), u64::MAX))
            .next_back()
            .map(|(_, summary)| summary)
            .ok_or(Error::UnknownStoreOutcome)?;
        let latest_event = state
            .events
            .iter()
            .rfind(|event| {
                event.session_id == *session
                    && event.run_id == target.run_id
                    && event.kind == crabber_core::EventKind::RunControlChanged
                    && event.correlation.as_deref() == Some(&id.0)
            })
            .and_then(|event| event.cursor);
        if record.summary.receipt != *saved
            || record.summary != *latest
            || latest_event != Some(latest.event_cursor)
        {
            return Err(Error::UnknownStoreOutcome);
        }
        verify_event(
            latest,
            state
                .events
                .get(latest.event_cursor)
                .ok_or(Error::UnknownStoreOutcome)?,
        )?;
        bytes += crate::snapshot::encoded_bytes(latest, MAX_RUN_CONTROL_BYTES - bytes)
            .ok_or(Error::UnknownStoreOutcome)?;
        summaries.push(latest.clone());
    }
    let origins = state
        .events
        .iter()
        .filter(|event| {
            event.session_id == target.session_id
                && event.run_id == target.run_id
                && event.kind == crabber_core::EventKind::RunControlChanged
                && event
                    .payload
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    == Some("Accepted")
        })
        .count();
    if origins != summaries.len() {
        return Err(Error::UnknownStoreOutcome);
    }
    summaries.sort_by_key(|s| s.receipt.sequence);
    if summaries
        .iter()
        .enumerate()
        .any(|(i, s)| s.receipt.sequence != i as u64 + 1)
        || crate::snapshot::encoded_bytes(&summaries, MAX_RUN_CONTROL_BYTES).is_none()
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(summaries)
}
pub(super) fn records(state: &State, target: &RunControlTarget) -> Result<Vec<Record>, Error> {
    records_from_summaries(
        state,
        target,
        summaries(state, target)?,
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
}
pub(super) fn records_from_summaries(
    state: &State,
    target: &RunControlTarget,
    summaries: Vec<RunControlSummary>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Vec<Record>, Error> {
    let mut records = Vec::new();
    let mut bytes = 0;
    for summary in summaries {
        let record = state
            .run_controls
            .get(&(target.session_id.clone(), summary.receipt.operation_id))
            .ok_or(Error::UnknownStoreOutcome)?;
        bytes += crate::snapshot::encoded_bytes(&record.request, MAX_CONTROL_BYTES)
            .ok_or(Error::UnknownStoreOutcome)?;
        if bytes > MAX_RUN_CONTROL_BYTES {
            return Err(Error::UnknownStoreOutcome);
        }
        charge_record(state, record, proof)?;
        validate(state, record)?;
        records.push(record.clone());
    }
    Ok(records)
}

fn charge_record(
    state: &State,
    record: &Record,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let charge = |_: crate::public_session::v2::proof::Failure| Error::UnknownStoreOutcome;
    proof.measure(&record.request).map_err(charge)?;
    proof.measure(&record.summary).map_err(charge)?;
    let saved = &record.summary.receipt;
    let Some(OperationReceipt::RunControl(receipt)) = state
        .operation_receipts
        .get(&(saved.target.session_id.clone(), saved.operation_id.clone()))
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    proof.measure(receipt).map_err(charge)?;
    proof
        .measure(
            state
                .events
                .get(receipt.event_cursor)
                .ok_or(Error::UnknownStoreOutcome)?,
        )
        .map_err(charge)?;
    let latest = state
        .run_control_audits
        .range(
            (
                saved.target.session_id.clone(),
                saved.operation_id.clone(),
                0,
            )
                ..=(
                    saved.target.session_id.clone(),
                    saved.operation_id.clone(),
                    u64::MAX,
                ),
        )
        .next_back()
        .map(|(_, summary)| summary)
        .ok_or(Error::UnknownStoreOutcome)?;
    proof.measure(latest).map_err(charge)?;
    proof
        .measure(
            state
                .events
                .get(latest.event_cursor)
                .ok_or(Error::UnknownStoreOutcome)?,
        )
        .map_err(charge)?;
    Ok(())
}
