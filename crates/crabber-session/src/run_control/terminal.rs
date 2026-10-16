//! Original suspended-cancellation source; no physical cleanup claim.
use super::{RunControlAction, RunControlError as Error, RunControlRequest, RunControlStatus};
use crate::RunControlSummary;
use crabber_core::{EventKind, EventRecord, RunStatus, Usage};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Source {
    pub request: RunControlRequest,
    pub settlement: EventRecord,
}
impl Source {
    fn verify(&self, summary: &RunControlSummary, audit: &EventRecord) -> Result<(), Error> {
        let receipt = &summary.receipt;
        let saved = &self.settlement;
        if crate::snapshot::encoded_bytes(self, 32768).is_none()
            || !matches!(self.request.action, RunControlAction::Cancel { .. })
            || self.request.id != receipt.operation_id
            || self.request.target != receipt.target
            || self
                .request
                .fingerprint()
                .map_err(crate::run_control::unknown)?
                != receipt.semantic_digest
            || receipt.kind != super::RunControlKind::Cancel
            || receipt.revision <= self.request.expected_revision
            || summary.status
                != (RunControlStatus::TerminalCommitted {
                    status: RunStatus::Interrupted,
                })
            || summary.revision <= receipt.revision
            || saved.session_id != receipt.target.session_id
            || saved.run_id != receipt.target.run_id
            || saved.kind != EventKind::RunSettled
            || saved.turn_id.as_ref().is_some_and(|id| {
                id.0.is_empty() || id.0.len() > 512 || id.0.chars().any(char::is_control)
            })
            || saved.live_only
            || saved.created_at != audit.created_at
            || saved.cursor.is_none_or(|cursor| {
                cursor <= receipt.event_cursor || cursor >= summary.event_cursor
            })
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
        let mut canonical = crate::attempt_control::terminal_event_scope(
            &receipt.target.session_id,
            &receipt.target.run_id,
            RunStatus::Interrupted,
            &usage,
            saved.created_at,
            saved.turn_id.clone(),
        );
        canonical.payload["reason"] = serde_json::json!("host_cancelled_suspension");
        canonical.cursor = saved.cursor;
        if canonical != *saved {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}

pub(crate) fn canonical_payload(
    summary: &RunControlSummary,
    event: &EventRecord,
) -> Result<serde_json::Value, Error> {
    let mut canonical = serde_json::to_value(summary).map_err(|_| Error::UnknownStoreOutcome)?;
    match (
        event.payload.get("terminal_version"),
        event.payload.get("terminal"),
    ) {
        (None, None) => {}
        (Some(version), Some(value)) if version.as_u64() == Some(1) => {
            if crate::snapshot::encoded_bytes(&event.payload, 32768).is_none() {
                return Err(Error::UnknownStoreOutcome);
            }
            let source: Source =
                serde_json::from_value(value.clone()).map_err(|_| Error::UnknownStoreOutcome)?;
            source.verify(summary, event)?;
            canonical["terminal_version"] = serde_json::json!(1);
            canonical["terminal"] =
                serde_json::to_value(source).map_err(|_| Error::UnknownStoreOutcome)?;
        }
        _ => return Err(Error::UnknownStoreOutcome),
    }
    Ok(canonical)
}

pub(crate) fn attach(
    event: &mut EventRecord,
    summary: &RunControlSummary,
    source: Option<(&RunControlRequest, &EventRecord)>,
) -> Result<(), Error> {
    if let Some((request, settlement)) = source {
        let source = Source {
            request: request.clone(),
            settlement: settlement.clone(),
        };
        source.verify(summary, event)?;
        event.payload["terminal_version"] = serde_json::json!(1);
        event.payload["terminal"] =
            serde_json::to_value(source).map_err(|_| Error::UnknownStoreOutcome)?;
        crate::snapshot::encoded_bytes(&event.payload, 32768).ok_or(Error::UnknownStoreOutcome)?;
    }
    Ok(())
}

/// Caller authenticates these original usage/turn facts and target under the
/// same bounded Store proof; an original request alone omits those facts.
#[derive(Clone, Copy)]
pub(crate) struct FrozenFacts<'a> {
    pub usage: &'a Usage,
    pub turn: Option<&'a crabber_core::TurnId>,
}
pub(crate) fn verify_scope(
    facts: crate::attempt::terminal::TerminalFacts<'_>,
    summary: &RunControlSummary,
    audit: &EventRecord,
    original: &EventRecord,
    frozen: FrozenFacts<'_>,
) -> Result<(), Error> {
    crate::run_control::verify_event(summary, audit)?;
    let source: Source = serde_json::from_value(
        audit
            .payload
            .get("terminal")
            .cloned()
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    if facts.session != &summary.receipt.target.session_id
        || facts.run != &summary.receipt.target.run_id
        || facts.status != RunStatus::Interrupted
        || facts.error != Some("host cancelled the suspended run")
        || facts.usage != frozen.usage
        || original.turn_id.as_ref() != frozen.turn
        || original.payload.get("usage")
            != Some(&serde_json::to_value(facts.usage).map_err(|_| Error::UnknownStoreOutcome)?)
        || source.settlement != *original
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
#[cfg(test)]
#[path = "terminal/contract.rs"]
pub(crate) mod contract;

#[cfg(test)]
#[path = "terminal/history_contract.rs"]
pub(crate) mod history_contract;
