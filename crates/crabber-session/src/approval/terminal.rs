//! Original administrative termination and its actual atomic settlement.
use crate::{
    ApprovalError as Error, ApprovalReceipt, ApprovalTermination, ApprovalTerminationKind,
};
use crabber_core::{EventKind, EventRecord, TurnId, Usage};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
#[cfg(test)]
#[path = "terminal/contract.rs"]
pub(crate) mod contract;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Source {
    pub request: ApprovalTermination,
    pub settlement: EventRecord,
}
impl Source {
    pub(crate) fn verify(
        &self,
        receipt: &ApprovalReceipt,
        event: &EventRecord,
    ) -> Result<(), Error> {
        self.request.verify_receipt(receipt)?;
        let saved = &self.settlement;
        if crate::snapshot::encoded_bytes(self, 32768).is_none()
            || saved.session_id != receipt.session_id
            || saved.run_id != receipt.run_id
            || saved.kind != EventKind::RunSettled
            || saved.live_only
            || saved.created_at != event.created_at
            || saved
                .cursor
                .is_none_or(|cursor| cursor.0 == 0 || cursor >= receipt.event_cursor)
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let usage: Usage = serde_json::from_value(
            saved
                .payload
                .get("usage")
                .cloned()
                .ok_or(Error::UnknownStoreOutcome)?,
        )
        .map_err(|_| Error::UnknownStoreOutcome)?;
        let mut canonical = frame(
            &self.request,
            &usage,
            saved.turn_id.clone(),
            saved.created_at,
        )?;
        canonical.cursor = saved.cursor;
        if canonical != *saved {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}

/// Both the live writer and historical verifier use these exact scope fields.
/// No paused Run or checkpoint is synthesized for historical verification.
pub(crate) fn frame(
    request: &ApprovalTermination,
    usage: &Usage,
    turn_id: Option<TurnId>,
    now: OffsetDateTime,
) -> Result<EventRecord, Error> {
    request.fingerprint()?;
    if turn_id
        .as_ref()
        .is_some_and(|id| id.0.is_empty() || id.0.len() > 512 || id.0.chars().any(char::is_control))
    {
        return Err(Error::InvalidRequest);
    }
    Ok(EventRecord {
        cursor: None,
        session_id: request.session_id.clone(),
        run_id: request.run_id.clone(),
        turn_id,
        kind: EventKind::RunSettled,
        payload: serde_json::json!({"status":request.run_status(),"usage":usage,
            "input_tokens":usage.input_tokens,"output_tokens":usage.output_tokens,
            "duration_ms":0,"duration_ns":0}),
        correlation: Some(request.run_id.to_string()),
        live_only: false,
        created_at: now,
    })
}

pub(crate) fn candidate(event: &EventRecord) -> bool {
    event.kind == EventKind::ContinuationChanged
        && event
            .payload
            .get("version")
            .and_then(serde_json::Value::as_u64)
            == Some(2)
}
fn source(receipt: &ApprovalReceipt, event: &EventRecord) -> Result<Source, Error> {
    if !candidate(event) || crate::snapshot::encoded_bytes(&event.payload, 32768).is_none() {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::approval::verify_receipt(receipt, event)?;
    serde_json::from_value(
        event
            .payload
            .get("terminal")
            .cloned()
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)
}
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "Consumed by the staged Run-history reader.")
)]
pub(crate) fn settlement(
    receipt: &ApprovalReceipt,
    event: &EventRecord,
) -> Result<EventRecord, Error> {
    Ok(source(receipt, event)?.settlement)
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "Consumed by the staged Run-history reader.")
)]
pub(crate) fn verify_scope(
    facts: crate::attempt::terminal::TerminalFacts<'_>,
    receipt: &ApprovalReceipt,
    event: &EventRecord,
    original: &EventRecord,
    batch: &crate::ApprovalSuspension,
) -> Result<(), Error> {
    // The caller authenticates this original batch with verify_origin_at in the
    // same bounded Store snapshot; terminal receipt semantics omit presentation.
    let saved = source(receipt, event)?;
    let expected_error = match saved.request.kind {
        ApprovalTerminationKind::Cancel => None,
        ApprovalTerminationKind::Expire { .. } => Some("approval expired"),
    };
    if facts.session != &receipt.session_id
        || facts.run != &receipt.run_id
        || facts.status != saved.request.run_status()
        || facts.error != expected_error
        || saved.settlement != *original
        || batch.continuation != saved.request.continuation
        || &batch.usage != facts.usage
        || batch.source_turn_id != original.turn_id
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let mut expected = frame(
        &saved.request,
        facts.usage,
        original.turn_id.clone(),
        original.created_at,
    )?;
    expected.cursor = original.cursor;
    if expected != *original {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

/// Metadata prepared only after the actual settlement has been inserted.
#[derive(Clone, Copy)]
pub(crate) enum Extra<'a> {
    Handoff(&'a crate::continuation_handoff::Handoff),
    Terminal(&'a ApprovalTermination, &'a EventRecord),
}
pub(crate) fn attach(event: &mut EventRecord, extra: Option<Extra<'_>>) -> Result<(), Error> {
    match extra {
        None => {}
        Some(Extra::Handoff(handoff)) => {
            event.payload["handoff"] =
                serde_json::to_value(handoff).map_err(|_| Error::InvalidRequest)?;
        }
        Some(Extra::Terminal(request, settlement)) => {
            if event.session_id != request.session_id
                || event.run_id != request.run_id
                || event.correlation.as_deref() != Some(request.id.0.as_str())
                || settlement.cursor.is_none()
            {
                return Err(Error::InvalidRequest);
            }
            let source = Source {
                request: request.clone(),
                settlement: settlement.clone(),
            };
            crate::snapshot::encoded_bytes(&source, 32768).ok_or(Error::InvalidRequest)?;
            event.payload["version"] = serde_json::json!(2);
            event.payload["terminal"] =
                serde_json::to_value(source).map_err(|_| Error::InvalidRequest)?;
        }
    }
    Ok(())
}
