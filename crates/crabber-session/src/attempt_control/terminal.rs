//! Original host Retire outcome and generated public terminal publication.
//! This authenticates a terminal domain, never the host's physical join claim.
use crate::attempt_conclusion::terminal::TerminalFacts;
use crate::attempt_control::Control;
use crate::{
    AttemptCleanup, AttemptError as Error, AttemptReceipt, AttemptRetirementOutcome,
    AttemptSnapshot,
};
use crabber_core::{EventKind, EventRecord, Run, RunStatus};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
pub(crate) const ERROR: &str = "host retired the reconciled execution attempt";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Audit {
    version: u32,
    request: Control,
    attempt: AttemptSnapshot,
    settlement: EventRecord,
}
fn invalid() -> Error {
    Error::UnknownStoreOutcome
}
impl Control {
    pub(crate) fn publication(
        &self,
        run: &Run,
        snapshot: &AttemptSnapshot,
        settlement: Option<EventRecord>,
        now: OffsetDateTime,
    ) -> Result<EventRecord, Error> {
        if !matches!(self, Self::Retire(_)) {
            return self.event(run, snapshot, now);
        }
        self.fingerprint()?;
        let audit = Audit {
            version: 5,
            request: self.clone(),
            attempt: snapshot.clone(),
            settlement: settlement.ok_or_else(invalid)?,
        };
        crate::snapshot::encoded_bytes(&audit, 32768).ok_or(Error::InvalidRequest)?;
        Ok(EventRecord {
            cursor: None,
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            turn_id: None,
            kind: EventKind::ExecutionAttemptChanged,
            payload: serde_json::to_value(audit).map_err(|_| Error::InvalidRequest)?,
            correlation: Some(self.scope().2.0.clone()),
            live_only: false,
            created_at: now,
        })
    }
}
fn parse(receipt: &AttemptReceipt, event: &EventRecord) -> Result<Audit, Error> {
    if crate::snapshot::encoded_bytes(&event.payload, 32768).is_none() {
        return Err(invalid());
    }
    let saved: Audit = serde_json::from_value(event.payload.clone()).map_err(|_| invalid())?;
    let Control::Retire(request) = &saved.request else {
        return Err(invalid());
    };
    let status = match request.outcome {
        AttemptRetirementOutcome::Failed => RunStatus::Failed,
        AttemptRetirementOutcome::Interrupted => RunStatus::Interrupted,
    };
    if saved.version != 5
        || saved.attempt != receipt.attempt
        || request.session_id != receipt.attempt.session_id
        || request.run_id != receipt.attempt.run_id
        || request.id != receipt.operation_id
        || request.target != receipt.attempt.target
        || request.expected_revision >= receipt.revision
        || saved.request.fingerprint().map_err(|_| invalid())? != receipt.semantic_digest
        || receipt.attempt.cleanup != AttemptCleanup::Complete
        || receipt.attempt.state
            != match status {
                RunStatus::Failed => crate::AttemptState::Failed,
                _ => crate::AttemptState::Interrupted,
            }
        || saved.settlement.session_id != request.session_id
        || saved.settlement.run_id != request.run_id
        || saved.settlement.kind != EventKind::RunSettled
        || saved.settlement.live_only
        || saved.settlement.payload.get("status")
            != Some(&serde_json::to_value(status).map_err(|_| invalid())?)
        || saved
            .settlement
            .cursor
            .is_none_or(|cursor| cursor.0 == 0 || cursor >= receipt.event_cursor)
    {
        return Err(invalid());
    }
    Ok(saved)
}
pub(crate) fn verify_event(receipt: &AttemptReceipt, event: &EventRecord) -> Result<(), Error> {
    parse(receipt, event).map(|_| ())
}
pub(crate) fn settlement(
    receipt: &AttemptReceipt,
    event: &EventRecord,
) -> Result<EventRecord, Error> {
    Ok(parse(receipt, event)?.settlement)
}
pub(crate) fn verify_scope(
    facts: TerminalFacts<'_>,
    receipt: &AttemptReceipt,
    event: &EventRecord,
    settlement: &EventRecord,
) -> Result<(), Error> {
    crate::attempt::verify_receipt(receipt, event)?;
    let saved = parse(receipt, event)?;
    if facts.session != &receipt.attempt.session_id
        || facts.run != &receipt.attempt.run_id
        || facts.error != Some(ERROR)
        || saved.settlement != *settlement
    {
        return Err(invalid());
    }
    let mut expected = crate::attempt_control::terminal_event_scope(
        facts.session,
        facts.run,
        facts.status,
        facts.usage,
        settlement.created_at,
        settlement.turn_id.clone(),
    );
    expected.cursor = settlement.cursor;
    if expected != *settlement {
        return Err(invalid());
    }
    Ok(())
}
