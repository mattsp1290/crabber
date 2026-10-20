use crate::{AttemptCleanup, PhaseQuiescence, PhaseState, phase::PhaseRecord};
use crate::{AttemptError, AttemptRetirement, PendingCallDisposition, attempt_control::Control};
use crabber_core::{
    ContentBlock, EventKind, EventRecord, MessageId, PartId, Run, ToolCallId, ToolCallRecord,
    ToolCallStatus, ToolResultStatus,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use time::OffsetDateTime;
pub(crate) fn validate_logical(call: &ToolCallRecord) -> Result<bool, AttemptError> {
    if call.id.0.is_empty() || call.id.0.len() > 512 || call.id.0.chars().any(char::is_control) {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    crate::ensure_storable(call).map_err(|_| AttemptError::UnknownStoreOutcome)?;
    if crate::snapshot::encoded_bytes(call, 4 * 1024 * 1024).is_none() {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    match (call.status, &call.result) {
        (ToolCallStatus::Pending, None) => Ok(true),
        (ToolCallStatus::Completed, Some(r)) if r.status == ToolResultStatus::Completed => {
            Ok(false)
        }
        (ToolCallStatus::Failed, Some(r)) if r.status == ToolResultStatus::Failed => Ok(false),
        (ToolCallStatus::Interrupted, Some(r)) if r.status == ToolResultStatus::Interrupted => {
            Ok(false)
        }
        (ToolCallStatus::Running, _) => Err(AttemptError::CleanupBlocked),
        _ => Err(AttemptError::UnknownStoreOutcome),
    }
}
pub(crate) fn validate_phase(
    run: &Run,
    call: &ToolCallRecord,
    phase: &PhaseRecord,
) -> Result<(), AttemptError> {
    validate_phase_scope(&run.session_id, &run.id, call, phase)
}
pub(crate) fn validate_phase_scope(
    session: &crabber_core::SessionId,
    run_id: &crabber_core::RunId,
    call: &ToolCallRecord,
    phase: &PhaseRecord,
) -> Result<(), AttemptError> {
    let quiescence = validate_phase_history_scope(session, run_id, call, phase)?;
    if quiescence != PhaseQuiescence::Complete {
        return Err(AttemptError::CleanupBlocked);
    }
    Ok(())
}
/// Authenticate closed presentation content while reporting actual cleanup state.
/// This read proof grants no retirement or effect authority.
pub(crate) fn validate_phase_history_scope(
    session: &crabber_core::SessionId,
    run_id: &crabber_core::RunId,
    call: &ToolCallRecord,
    phase: &PhaseRecord,
) -> Result<PhaseQuiescence, AttemptError> {
    phase
        .validate()
        .map_err(|_| AttemptError::UnknownStoreOutcome)?;
    if phase.snapshot.session_id != *session
        || phase.snapshot.run_id != *run_id
        || phase.snapshot.target.call_id != call.id
        || phase.snapshot.call_binding
            != crate::phase::call_binding(call).map_err(|_| AttemptError::UnknownStoreOutcome)?
    {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    if !matches!(
        phase.snapshot.state,
        PhaseState::Completed | PhaseState::Failed
    ) {
        return Err(AttemptError::CleanupBlocked);
    }
    let output = phase
        .output
        .as_ref()
        .ok_or(AttemptError::UnknownStoreOutcome)?;
    let result = call.result.as_ref().ok_or(AttemptError::CleanupBlocked)?;
    if result.content != output.to_content_blocks()
        || (phase.snapshot.state == PhaseState::Completed)
            != (call.status == ToolCallStatus::Completed)
        || (output.is_error() != (result.status != ToolResultStatus::Completed))
    {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    Ok(phase.snapshot.quiescence)
}
pub(crate) fn order_pending(
    run: &Run,
    calls: Vec<ToolCallRecord>,
    approval: Option<Vec<ToolCallId>>,
) -> Result<Vec<ToolCallRecord>, AttemptError> {
    if calls.len() > 64 {
        return Err(AttemptError::CleanupBlocked);
    }
    let order = if let Some(order) = approval {
        Some(order)
    } else if let Some(value) = run.checkpoint.as_ref().and_then(|v| v.get("pending_calls")) {
        Some(
            serde_json::from_value::<Vec<ToolCallId>>(value.clone())
                .map_err(|_| AttemptError::UnknownStoreOutcome)?,
        )
    } else {
        None
    };
    let mut calls: BTreeMap<_, _> = calls
        .into_iter()
        .map(|call| (call.id.clone(), call))
        .collect();
    let Some(order) = order else {
        return Ok(calls.into_values().collect());
    };
    if order.len() > 64 || order.iter().collect::<BTreeSet<_>>().len() != order.len() {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    let mut result = Vec::new();
    for id in order {
        if let Some(call) = calls.remove(&id) {
            result.push(call);
        }
    }
    if !calls.is_empty() {
        return Err(AttemptError::CleanupBlocked);
    }
    Ok(result)
}
pub(crate) fn cancel_records(
    control: &Control,
    run: &Run,
    call: &ToolCallRecord,
    now: OffsetDateTime,
    turn: Option<crabber_core::TurnId>,
) -> crate::PhaseSettlementRecords {
    let content = vec![ContentBlock::Text {
        text: "cancelled before phase start".into(),
    }];
    let digest = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                "crabber.attempt-cancel.v1",
                control.scope().0,
                control.scope().1,
                control.scope().2,
                &call.id
            ))
            .expect("serializable IDs")
        )
    );
    let mut records = crate::abandonment::interrupted_tool(run, call, now);
    records.0.content.clone_from(&content);
    records.1.id = MessageId::from(format!("attempt-retire-{digest}"));
    records.1.parts[0].id = PartId::from(format!("attempt-retire-part-{digest}"));
    records.1.parts[0].message_id = records.1.id.clone();
    records.1.parts[0].content = ContentBlock::ToolResult {
        call_id: call.id.clone(),
        content: content.clone(),
        is_error: true,
    };
    records.2.turn_id = turn;
    records.2.payload = serde_json::json!({"call_id":call.id,"name":call.name,"tool":call.name,"tool_id":call.id,"message_id":records.1.id,"status":ToolResultStatus::Interrupted,"content":content,"is_error":true,"duration_ms":0});
    crate::PhaseSettlementRecords {
        result: records.0,
        message: records.1,
        event: records.2,
        call_status: ToolCallStatus::Interrupted,
    }
}
pub(crate) fn terminal_event(
    run: &Run,
    now: OffsetDateTime,
    turn: Option<crabber_core::TurnId>,
) -> EventRecord {
    terminal_event_scope(&run.session_id, &run.id, run.status, &run.usage, now, turn)
}
pub(crate) fn terminal_event_scope(
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    status: crabber_core::RunStatus,
    usage: &crabber_core::Usage,
    now: OffsetDateTime,
    turn: Option<crabber_core::TurnId>,
) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: session.clone(),
        run_id: run.clone(),
        turn_id: turn,
        kind: EventKind::RunSettled,
        payload: serde_json::json!({"status":status,"usage":usage,"input_tokens":usage.input_tokens,"output_tokens":usage.output_tokens,"duration_ms":0,"duration_ns":0}),
        correlation: Some(run.0.clone()),
        live_only: false,
        created_at: now,
    }
}
pub(crate) fn check_pending(
    request: &AttemptRetirement,
    calls: &[ToolCallRecord],
) -> Result<(), AttemptError> {
    if calls.len() > 64
        || (!calls.is_empty() && request.pending == PendingCallDisposition::RequireNone)
    {
        return Err(AttemptError::CleanupBlocked);
    }
    Ok(())
}
pub(crate) fn finish_record(record: &crate::attempt::AttemptRecord) -> Result<(), AttemptError> {
    if record.snapshot.cleanup != AttemptCleanup::Complete {
        return Err(AttemptError::CleanupBlocked);
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct RetirementContext {
    pub order: Option<Vec<ToolCallId>>,
    pub source: Option<crabber_core::TurnId>,
}
