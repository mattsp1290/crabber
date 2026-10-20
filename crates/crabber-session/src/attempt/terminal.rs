//! Source-domain dispatch; version numbers never establish physical cleanup.
pub(crate) use crate::attempt_conclusion::terminal::TerminalFacts;
use crate::{AttemptError, AttemptReceipt};
use crabber_core::EventRecord;
pub(crate) fn candidate(event: &EventRecord) -> bool {
    matches!(
        event
            .payload
            .get("version")
            .and_then(serde_json::Value::as_u64),
        Some(4 | 5)
    )
}
pub(crate) fn settlement(
    receipt: &AttemptReceipt,
    event: &EventRecord,
) -> Result<EventRecord, AttemptError> {
    match event
        .payload
        .get("version")
        .and_then(serde_json::Value::as_u64)
    {
        Some(4) => crate::attempt_conclusion::terminal::settlement(receipt, event),
        Some(5) => crate::attempt_control::terminal::settlement(receipt, event),
        _ => Err(AttemptError::UnknownStoreOutcome),
    }
}
pub(crate) fn verify_scope(
    facts: TerminalFacts<'_>,
    receipt: &AttemptReceipt,
    event: &EventRecord,
    settlement: &EventRecord,
) -> Result<(), AttemptError> {
    match event
        .payload
        .get("version")
        .and_then(serde_json::Value::as_u64)
    {
        Some(4) => {
            crate::attempt_conclusion::terminal::verify_scope(facts, receipt, event, settlement)
        }
        Some(5) => {
            crate::attempt_control::terminal::verify_scope(facts, receipt, event, settlement)
        }
        _ => Err(AttemptError::UnknownStoreOutcome),
    }
}
