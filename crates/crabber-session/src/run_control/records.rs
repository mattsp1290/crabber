use crate::run_control::{
    RunControlAction, RunControlError, RunControlKind, RunControlReceipt, RunControlRequest,
    RunControlStatus, RunControlSummary, RunControlTarget,
};
use crabber_core::{ControlId, SessionId};
use crabber_core::{EventKind, EventRecord, Run, RunStatus};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
pub(crate) const MAX_CONTROLS: usize = 64;
pub(crate) const MAX_CONTROL_BYTES: usize = 65_536;
pub(crate) const MAX_RUN_CONTROL_BYTES: usize = 262_144;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    pub request: RunControlRequest,
    pub summary: RunControlSummary,
}
pub(crate) fn unknown<T>(_: T) -> RunControlError {
    RunControlError::UnknownStoreOutcome
}
pub(crate) fn valid_scope(session: &SessionId, id: &ControlId) -> Result<(), RunControlError> {
    crate::public_state::validate_id(id).map_err(|_| RunControlError::InvalidRequest)?;
    if session.0.is_empty() || session.0.len() > 512 || session.0.chars().any(char::is_control) {
        return Err(RunControlError::InvalidRequest);
    }
    Ok(())
}
pub(crate) fn verify_target(target: &RunControlTarget, run: &Run) -> Result<(), RunControlError> {
    if *target != RunControlTarget::of(run) {
        return Err(RunControlError::SemanticConflict);
    }
    Ok(())
}
pub(crate) fn kind(request: &RunControlRequest) -> RunControlKind {
    match request.action {
        RunControlAction::Cancel { .. } => RunControlKind::Cancel,
        RunControlAction::Steer { .. } => RunControlKind::Steer,
    }
}
impl Record {
    pub(crate) fn validate(&self) -> Result<(), RunControlError> {
        if let RunControlStatus::Incorporated { turn_id } = &self.summary.status {
            validate_turn(Some(turn_id)).map_err(unknown)?;
        }
        let r = &self.summary.receipt;
        if self.request.fingerprint().map_err(unknown)? != r.semantic_digest
            || self.request.target != r.target
            || self.request.id != r.operation_id
            || kind(&self.request) != r.kind
            || r.sequence == 0
            || r.sequence > MAX_CONTROLS as u64
            || r.revision <= self.request.expected_revision
            || r.event_cursor.0 == 0
            || self.summary.revision < r.revision
            || self.summary.event_cursor.0 < r.event_cursor.0
            || matches!(
                (&r.kind, &self.summary.status),
                (
                    RunControlKind::Steer,
                    RunControlStatus::Observed | RunControlStatus::TerminalCommitted { .. }
                ) | (
                    RunControlKind::Cancel,
                    RunControlStatus::Incorporated { .. }
                        | RunControlStatus::UnappliedTerminal { .. }
                )
            )
        {
            return Err(RunControlError::UnknownStoreOutcome);
        }
        Ok(())
    }
}
pub(crate) fn event(
    run: &Run,
    summary: &RunControlSummary,
    now: OffsetDateTime,
) -> Result<EventRecord, RunControlError> {
    Ok(EventRecord {
        cursor: Some(summary.event_cursor),
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: match &summary.status {
            RunControlStatus::Incorporated { turn_id } => Some(turn_id.clone()),
            _ => None,
        },
        kind: EventKind::RunControlChanged,
        payload: serde_json::to_value(summary).map_err(unknown)?,
        correlation: Some(summary.receipt.operation_id.0.clone()),
        live_only: false,
        created_at: now,
    })
}
pub(crate) fn verify_event(
    summary: &RunControlSummary,
    event: &EventRecord,
) -> Result<(), RunControlError> {
    if event.cursor != Some(summary.event_cursor)
        || event.session_id != summary.receipt.target.session_id
        || event.run_id != summary.receipt.target.run_id
        || event.kind != EventKind::RunControlChanged
        || event.turn_id.as_ref()
            != match &summary.status {
                RunControlStatus::Incorporated { turn_id } => Some(turn_id),
                _ => None,
            }
        || event.live_only
        || event.correlation.as_deref() != Some(&summary.receipt.operation_id.0)
        || event.payload != crate::run_control::terminal::canonical_payload(summary, event)?
    {
        return Err(RunControlError::UnknownStoreOutcome);
    }
    Ok(())
}
pub(crate) fn accepted(receipt: &RunControlReceipt) -> RunControlSummary {
    RunControlSummary {
        receipt: receipt.clone(),
        status: RunControlStatus::Accepted,
        revision: receipt.revision,
        event_cursor: receipt.event_cursor,
    }
}
pub(crate) fn terminal_status(record: &Record, status: RunStatus) -> Option<RunControlStatus> {
    match (&record.summary.status, &record.request.action) {
        (RunControlStatus::Accepted, RunControlAction::Steer { .. }) => {
            Some(RunControlStatus::UnappliedTerminal { status })
        }
        (
            RunControlStatus::Accepted | RunControlStatus::Observed,
            RunControlAction::Cancel { .. },
        ) => Some(RunControlStatus::TerminalCommitted { status }),
        _ => None,
    }
}

pub(crate) fn store_error(_: RunControlError) -> crate::StoreError {
    crate::StoreError::Validation("durable run control persistence outcome is unknown".into())
}
/// Supplied IDs reserve their existing global persistence namespace at acceptance.
pub(crate) fn supplied_ids(
    request: &RunControlRequest,
) -> Result<
    (
        std::collections::BTreeSet<&str>,
        std::collections::BTreeSet<&str>,
    ),
    RunControlError,
> {
    let mut messages = std::collections::BTreeSet::new();
    let mut parts = std::collections::BTreeSet::new();
    if let RunControlAction::Steer { input } = &request.action {
        for message in input.messages() {
            if let Some(id) = &message.id
                && !messages.insert(id.as_str())
            {
                return Err(RunControlError::InvalidRequest);
            }
            for part in &message.parts {
                if let Some(id) = &part.id
                    && !parts.insert(id.as_str())
                {
                    return Err(RunControlError::InvalidRequest);
                }
            }
        }
    }
    Ok((messages, parts))
}
pub(crate) fn reserve_ids<'a>(
    request: &'a RunControlRequest,
    previous: &[Record],
) -> Result<
    (
        std::collections::BTreeSet<&'a str>,
        std::collections::BTreeSet<&'a str>,
    ),
    RunControlError,
> {
    let (messages, parts) = supplied_ids(request)?;
    for record in previous {
        let (old_messages, old_parts) = supplied_ids(&record.request)?;
        if messages.iter().any(|id| old_messages.contains(id))
            || parts.iter().any(|id| old_parts.contains(id))
        {
            return Err(RunControlError::SemanticConflict);
        }
    }
    Ok((messages, parts))
}

pub(crate) fn verify_terminal(
    summary: &RunControlSummary,
    status: RunStatus,
) -> Result<(), RunControlError> {
    let selected = match summary.status {
        RunControlStatus::TerminalCommitted { status }
        | RunControlStatus::UnappliedTerminal { status } => Some(status),
        RunControlStatus::Accepted | RunControlStatus::Observed => None,
        RunControlStatus::Incorporated { .. } => return Ok(()),
    };
    if selected.is_some_and(|selected| selected != status || !status.is_terminal())
        || (selected.is_none() && status.is_terminal())
    {
        return Err(RunControlError::UnknownStoreOutcome);
    }
    Ok(())
}

pub(crate) fn validate_target(target: &RunControlTarget) -> Result<(), RunControlError> {
    valid_scope(&target.session_id, &ControlId("target".into()))?;
    crate::attempt::validate_identifier(&target.run_id.0)
        .map_err(|_| RunControlError::InvalidRequest)?;
    if [&target.config_hash, &target.plan_fingerprint]
        .iter()
        .any(|s| s.is_empty() || s.len() > 512 || s.chars().any(char::is_control))
    {
        return Err(RunControlError::InvalidRequest);
    }
    Ok(())
}
/// Reserve metadata growth through incorporation, including a bounded turn ID.
pub(crate) fn reserved_bytes(request: &RunControlRequest) -> Result<usize, RunControlError> {
    crate::snapshot::encoded_bytes(request, MAX_CONTROL_BYTES)
        .and_then(|n| n.checked_add(2048))
        .ok_or(RunControlError::InvalidRequest)
}
pub(crate) fn validate_turn(turn: Option<&crabber_core::TurnId>) -> Result<(), RunControlError> {
    if turn
        .is_some_and(|id| id.0.is_empty() || id.0.len() > 512 || id.0.chars().any(char::is_control))
    {
        return Err(RunControlError::InvalidRequest);
    }
    Ok(())
}
