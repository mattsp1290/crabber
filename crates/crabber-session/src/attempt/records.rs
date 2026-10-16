#[path = "pristine.rs"]
pub(crate) mod pristine;
#[path = "terminal.rs"]
pub(crate) mod terminal;
use crate::{
    AttemptCleanup, AttemptCommand, AttemptError, AttemptMutation, AttemptReceipt, AttemptSnapshot,
    AttemptState,
};
use crabber_core::{EventKind, EventRecord, Run, SessionRevision};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
#[path = "transition.rs"]
mod transition;
pub(crate) use transition::{AttemptRecord, verify_run};

pub(crate) fn validate_request(request: &AttemptMutation) -> Result<(), AttemptError> {
    validate_identifier(&request.id.0)?;
    validate_identifier(&request.command.target().id.0)
}
pub(crate) fn validate_identifier(id: &str) -> Result<(), AttemptError> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(AttemptError::InvalidRequest);
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AttemptEvent {
    version: u32,
    command: AttemptCommand,
    attempt: AttemptSnapshot,
}
pub(crate) fn event(
    run: &Run,
    request: &AttemptMutation,
    snapshot: &AttemptSnapshot,
    now: OffsetDateTime,
) -> Result<EventRecord, AttemptError> {
    Ok(EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind: EventKind::ExecutionAttemptChanged,
        payload: serde_json::to_value(AttemptEvent {
            version: 1,
            command: request.command.clone(),
            attempt: snapshot.clone(),
        })
        .map_err(|_| AttemptError::InvalidRequest)?,
        correlation: Some(request.id.0.clone()),
        live_only: false,
        created_at: now,
    })
}
pub(crate) fn verify_receipt(
    receipt: &AttemptReceipt,
    event: &EventRecord,
) -> Result<(), AttemptError> {
    if event.kind != EventKind::ExecutionAttemptChanged
        || event.live_only
        || event.session_id != receipt.attempt.session_id
        || event.run_id != receipt.attempt.run_id
        || event.correlation.as_deref() != Some(&receipt.operation_id.0)
        || event.cursor.is_some_and(|v| v != receipt.event_cursor)
    {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    if event
        .payload
        .get("version")
        .and_then(serde_json::Value::as_u64)
        == Some(5)
    {
        return crate::attempt_control::terminal::verify_event(receipt, event);
    }
    if event
        .payload
        .get("version")
        .and_then(serde_json::Value::as_u64)
        == Some(4)
    {
        return crate::attempt_conclusion::terminal::verify_event(receipt, event);
    }
    if event
        .payload
        .get("version")
        .and_then(serde_json::Value::as_u64)
        == Some(2)
    {
        return crate::attempt_conclusion::verify_event(receipt, event);
    }
    if event
        .payload
        .get("version")
        .and_then(serde_json::Value::as_u64)
        == Some(3)
    {
        return crate::attempt_control::verify_control(receipt, event);
    }
    let saved: AttemptEvent = serde_json::from_value(event.payload.clone())
        .map_err(|_| AttemptError::UnknownStoreOutcome)?;
    if saved.version != 1
        || saved.attempt != receipt.attempt
        || *saved.command.target() != saved.attempt.target
    {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    let valid = match &saved.command {
        AttemptCommand::ReserveBound { .. }
        | AttemptCommand::Reserve { .. }
        | AttemptCommand::ReclaimUnstarted { .. } => {
            saved.attempt.state == AttemptState::Unstarted
                && saved.attempt.cleanup == AttemptCleanup::Pending
        }
        AttemptCommand::Begin { .. } => {
            saved.attempt.state == AttemptState::Started
                && saved.attempt.cleanup == AttemptCleanup::Pending
        }
        AttemptCommand::Finish { outcome, .. } => {
            saved.attempt.state == (*outcome).into()
                && saved.attempt.cleanup == AttemptCleanup::Pending
        }
        AttemptCommand::RecordCleanup { result, .. } => {
            !matches!(
                saved.attempt.state,
                AttemptState::Unstarted | AttemptState::Started
            ) && saved.attempt.cleanup == (*result).into()
        }
    };
    if !valid {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    let expected_revision = receipt
        .revision
        .get()
        .checked_sub(1)
        .and_then(|v| SessionRevision::new(v).ok())
        .ok_or(AttemptError::UnknownStoreOutcome)?;
    let request = AttemptMutation {
        id: receipt.operation_id.clone(),
        expected_revision,
        command: saved.command,
    };
    if request
        .fingerprint(&receipt.attempt.session_id, &receipt.attempt.run_id)
        .map_err(|_| AttemptError::UnknownStoreOutcome)?
        != receipt.semantic_digest
    {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    Ok(())
}
