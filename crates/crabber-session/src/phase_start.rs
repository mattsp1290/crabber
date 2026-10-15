//! Atomic native dispatch start and its logical tool-call source event.
use crate::{
    PhaseCommand, PhaseError as Error, PhaseMutation, PhaseOperation, PhaseReceipt, PhaseState,
};
use crabber_core::{
    EventKind, EventRecord, InputFingerprint, Run, RunId, SessionId, ToolCallRecord, TurnId,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

/// A fresh acknowledged application commits native Begin and `ToolCallRunning`
/// together. Replays are metadata and never authorize another dispatch.
#[derive(Clone)]
pub struct PhaseStart {
    pub begin: PhaseMutation,
    pub turn_id: Option<TurnId>,
    pub created_at: OffsetDateTime,
}
impl std::fmt::Debug for PhaseStart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PhaseStart([redacted])")
    }
}
impl PhaseStart {
    /// # Errors
    /// Rejects a non-Begin command or invalid presentation identity.
    pub fn validate(&self) -> Result<(), Error> {
        self.begin.validate()?;
        if !matches!(self.begin.command, PhaseCommand::Begin { .. })
            || self.turn_id.as_ref().is_some_and(|id| {
                id.0.is_empty() || id.0.len() > 512 || id.0.chars().any(char::is_control)
            })
        {
            return Err(Error::InvalidRequest);
        }
        Ok(())
    }
    /// Presentation timing/IDs do not change the original operation semantics.
    /// # Errors
    /// Rejects an invalid start operation or scope.
    pub fn fingerprint(&self, session: &SessionId, run: &RunId) -> Result<InputFingerprint, Error> {
        self.validate()?;
        let digest = self.begin.fingerprint(session, run)?;
        InputFingerprint::new(format!(
            "{:x}",
            Sha256::digest(format!("crabber.phase-start.v1:{}", digest.as_str()).as_bytes())
        ))
        .map_err(|_| Error::InvalidRequest)
    }
    /// # Errors
    /// Rejects mismatched authoritative run/call scope or unpersistable source data.
    pub fn materialize(&self, run: &Run, call: &ToolCallRecord) -> Result<EventRecord, Error> {
        self.validate()?;
        if call.run_id != run.id || call.id != self.begin.command.target().call_id {
            return Err(Error::NotFound);
        }
        let event = EventRecord {
            cursor: None,
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            turn_id: self.turn_id.clone(),
            kind: EventKind::ToolCallRunning,
            payload: json!({"call_id":call.id,"name":call.name,"status":"running"}),
            correlation: Some(call.id.0.clone()),
            live_only: false,
            created_at: self.created_at,
        };
        crate::ensure_storable(&event).map_err(|_| Error::InvalidRequest)?;
        Ok(event)
    }
    /// # Errors
    /// Rejects receipt semantics from a different command, target or compound operation.
    pub fn verify_replay(&self, receipt: &PhaseReceipt) -> Result<(), Error> {
        if receipt.operation != PhaseOperation::Begin
            || receipt.operation_id != self.begin.id
            || receipt.phase.target != *self.begin.command.target()
            || receipt.phase.state != PhaseState::Started
            || receipt.semantic_digest
                != self.fingerprint(&receipt.phase.session_id, &receipt.phase.run_id)?
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}
