//! Cursor-independent public audit data: databases allocate the cursor at insert.
use super::{ApprovalError as Error, ApprovalOutcome, ApprovalReceipt};
use crabber_core::{ControlId, EventKind, EventRecord, InputFingerprint, Run, SessionRevision};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Audit {
    version: u32,
    operation_id: ControlId,
    semantic_digest: InputFingerprint,
    revision: SessionRevision,
    outcome: ApprovalOutcome,
}
fn kind(outcome: &ApprovalOutcome) -> EventKind {
    if matches!(outcome, ApprovalOutcome::Answered { .. }) {
        EventKind::InteractionChanged
    } else {
        EventKind::ContinuationChanged
    }
}
pub(crate) fn event(
    run: &Run,
    id: &ControlId,
    digest: &InputFingerprint,
    revision: SessionRevision,
    outcome: &ApprovalOutcome,
    now: OffsetDateTime,
) -> Result<EventRecord, Error> {
    Ok(EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind: kind(outcome),
        payload: serde_json::to_value(Audit {
            version: 1,
            operation_id: id.clone(),
            semantic_digest: digest.clone(),
            revision,
            outcome: outcome.clone(),
        })
        .map_err(|_| Error::InvalidRequest)?,
        correlation: Some(id.0.clone()),
        live_only: false,
        created_at: now,
    })
}
pub(crate) fn verify_receipt(receipt: &ApprovalReceipt, event: &EventRecord) -> Result<(), Error> {
    let saved: Audit =
        serde_json::from_value(event.payload.clone()).map_err(|_| Error::UnknownStoreOutcome)?;
    if saved.version != 1
        || saved.operation_id != receipt.operation_id
        || saved.semantic_digest != receipt.semantic_digest
        || saved.revision != receipt.revision
        || saved.outcome != receipt.outcome
        || receipt.revision == SessionRevision::INITIAL
        || receipt.event_cursor.0 == 0
        || event.session_id != receipt.session_id
        || event.run_id != receipt.run_id
        || event.cursor != Some(receipt.event_cursor)
        || event.correlation.as_deref() != Some(&receipt.operation_id.0)
        || event.live_only
        || event.turn_id.is_some()
        || event.kind != kind(&receipt.outcome)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
