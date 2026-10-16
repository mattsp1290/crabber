//! Versioned public proof excludes all private checkpoint and error bodies.
use crate::{
    AttemptCleanup, AttemptError, AttemptFinish, AttemptReceipt, AttemptSnapshot, AttemptState,
};
use crabber_core::{EventKind, EventRecord, InputFingerprint, Run};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Audit {
    version: u32,
    operation: String,
    outcome: AttemptFinish,
    semantic_digest: InputFingerprint,
    attempt: AttemptSnapshot,
}
pub(crate) fn event(
    run: &Run,
    outcome: AttemptFinish,
    snapshot: &AttemptSnapshot,
    operation: &crabber_core::ControlId,
    digest: InputFingerprint,
    now: OffsetDateTime,
) -> Result<EventRecord, AttemptError> {
    Ok(EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind: EventKind::ExecutionAttemptChanged,
        payload: serde_json::to_value(Audit {
            version: 2,
            operation: "conclude_run".into(),
            outcome,
            semantic_digest: digest,
            attempt: snapshot.clone(),
        })
        .map_err(|_| AttemptError::InvalidRequest)?,
        correlation: Some(operation.0.clone()),
        live_only: false,
        created_at: now,
    })
}
pub(crate) fn verify_event(
    receipt: &AttemptReceipt,
    event: &EventRecord,
) -> Result<(), AttemptError> {
    let saved: Audit = serde_json::from_value(event.payload.clone())
        .map_err(|_| AttemptError::UnknownStoreOutcome)?;
    if saved.version != 2
        || saved.operation != "conclude_run"
        || saved.semantic_digest != receipt.semantic_digest
        || saved.attempt != receipt.attempt
        || saved.attempt.state != saved.outcome.into()
        || saved.attempt.cleanup != AttemptCleanup::Complete
        || matches!(
            saved.attempt.state,
            AttemptState::Unstarted | AttemptState::Started | AttemptState::OutcomeUnknown
        )
        || receipt.revision == crabber_core::SessionRevision::INITIAL
        || receipt.event_cursor.0 == 0
    {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    Ok(())
}

/// Preserve suspension v2 while terminal boundaries retain their exact source.
pub(crate) fn publication(
    run: &Run,
    request: &crate::AttemptMutation,
    conclusion: Option<&crate::AttemptConclusion>,
    snapshot: &AttemptSnapshot,
    digest: InputFingerprint,
    settlement: Option<EventRecord>,
    now: OffsetDateTime,
) -> Result<EventRecord, AttemptError> {
    let Some(conclusion) = conclusion else {
        return crate::attempt::event(run, request, snapshot, now);
    };
    if matches!(conclusion.boundary, crate::RunBoundary::Terminal { .. }) {
        crate::attempt_conclusion::terminal::event(
            run,
            conclusion,
            snapshot,
            digest,
            settlement.ok_or(AttemptError::UnknownStoreOutcome)?,
            now,
        )
    } else {
        event(
            run,
            conclusion.validate()?,
            snapshot,
            &request.id,
            digest,
            now,
        )
    }
}
