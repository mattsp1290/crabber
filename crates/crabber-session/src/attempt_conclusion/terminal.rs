//! Original terminal-only compound source. Suspensions retain their v2 contract.
use crate::{
    AttemptCleanup, AttemptCommand, AttemptConclusion, AttemptError as Error, AttemptFinish,
    AttemptMutation, AttemptReceipt, AttemptSnapshot, RunBoundary,
};
use crabber_core::{
    EventKind, EventRecord, InputFingerprint, Run, RunId, RunStatus, SessionId, Usage,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Audit {
    version: u32,
    operation: String,
    finish: AttemptMutation,
    semantic_digest: InputFingerprint,
    attempt: AttemptSnapshot,
    settlement: EventRecord,
    elapsed: std::time::Duration,
}
fn invalid() -> Error {
    Error::UnknownStoreOutcome
}
pub(crate) fn event(
    run: &Run,
    conclusion: &AttemptConclusion,
    snapshot: &AttemptSnapshot,
    digest: InputFingerprint,
    settlement: EventRecord,
    now: OffsetDateTime,
) -> Result<EventRecord, Error> {
    if !matches!(conclusion.boundary, RunBoundary::Terminal { .. }) {
        return Err(Error::InvalidRequest);
    }
    conclusion.validate()?;
    let audit = Audit {
        version: 4,
        operation: "conclude_terminal".into(),
        finish: conclusion.finish.clone(),
        semantic_digest: digest,
        attempt: snapshot.clone(),
        settlement,
        elapsed: conclusion.elapsed,
    };
    crate::snapshot::encoded_bytes(&audit, 32768).ok_or(Error::InvalidRequest)?;
    Ok(EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind: EventKind::ExecutionAttemptChanged,
        payload: serde_json::to_value(audit).map_err(|_| Error::InvalidRequest)?,
        correlation: Some(conclusion.finish.id.0.clone()),
        live_only: false,
        created_at: now,
    })
}
fn parse(receipt: &AttemptReceipt, event: &EventRecord) -> Result<Audit, Error> {
    if crate::snapshot::encoded_bytes(&event.payload, 32768).is_none() {
        return Err(invalid());
    }
    let saved: Audit = serde_json::from_value(event.payload.clone()).map_err(|_| invalid())?;
    saved
        .finish
        .fingerprint(&receipt.attempt.session_id, &receipt.attempt.run_id)
        .map_err(|_| invalid())?;
    let AttemptCommand::Finish { target, outcome } = &saved.finish.command else {
        return Err(invalid());
    };
    let status: RunStatus = serde_json::from_value(
        saved
            .settlement
            .payload
            .get("status")
            .cloned()
            .ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    let expected = match status {
        RunStatus::Completed => AttemptFinish::Completed,
        RunStatus::Failed => AttemptFinish::Failed,
        RunStatus::Interrupted => AttemptFinish::Interrupted,
        _ => return Err(invalid()),
    };
    if saved.elapsed.as_nanos() > i64::MAX as u128
        || saved.version != 4
        || saved.operation != "conclude_terminal"
        || saved.semantic_digest != receipt.semantic_digest
        || saved.attempt != receipt.attempt
        || saved.finish.id != receipt.operation_id
        || *target != receipt.attempt.target
        || *outcome != expected
        || receipt.attempt.state != expected.into()
        || receipt.attempt.cleanup != AttemptCleanup::Complete
        || saved.finish.expected_revision >= receipt.revision
        || saved.settlement.session_id != receipt.attempt.session_id
        || saved.settlement.run_id != receipt.attempt.run_id
        || saved.settlement.kind != EventKind::RunSettled
        || saved.settlement.live_only
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
/// Called only after authenticating both original indexed publications and the
/// current terminal tuple. The current error is bounded and charged separately;
/// no checkpoint or fabricated Run participates in the digest.
#[derive(Clone, Copy)]
pub(crate) struct TerminalFacts<'a> {
    pub session: &'a SessionId,
    pub run: &'a RunId,
    pub status: RunStatus,
    pub error: Option<&'a str>,
    pub usage: &'a Usage,
}
pub(crate) fn verify_scope(
    facts: TerminalFacts<'_>,
    receipt: &AttemptReceipt,
    event: &EventRecord,
    settlement: &EventRecord,
) -> Result<(), Error> {
    let TerminalFacts {
        session,
        run,
        status,
        error,
        usage,
    } = facts;
    if error.is_some_and(|value| value.len() > 65536) {
        return Err(invalid());
    }
    crate::attempt::verify_receipt(receipt, event)?;
    let saved = parse(receipt, event)?;
    if receipt.attempt.session_id != *session
        || receipt.attempt.run_id != *run
        || saved.settlement != *settlement
    {
        return Err(invalid());
    }
    let conclusion = AttemptConclusion {
        finish: saved.finish,
        boundary: RunBoundary::Terminal {
            status,
            error: error.map(str::to_owned),
            usage: usage.clone(),
        },
        created_at: saved.settlement.created_at,
        turn_id: saved.settlement.turn_id.clone(),
        elapsed: saved.elapsed,
    };
    conclusion.verify_replay(receipt).map_err(|_| invalid())?;
    let mut expected = crate::attempt_conclusion::materialize_scope(&conclusion, session, run)
        .map_err(|_| invalid())?;
    expected.cursor = settlement.cursor;
    if expected != *settlement {
        return Err(invalid());
    }
    Ok(())
}
