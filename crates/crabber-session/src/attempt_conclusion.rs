//! Atomic logical run boundary plus owned-attempt outcome and cleanup.
mod materialize;
pub(crate) mod terminal;
pub(crate) use materialize::scoped as materialize_scope;
mod audit;
use crate::{
    AttemptCleanup, AttemptCommand, AttemptError, AttemptFinish, AttemptMutation, AttemptReceipt,
};
pub(crate) use audit::{publication, verify_event};
use crabber_core::{
    EventRecord, InputFingerprint, Run, RunId, RunStatus, SessionId, TurnId, Usage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

/// Maximum encoded logical boundary, including its private checkpoint.
pub const MAX_RUN_BOUNDARY_BYTES: usize = 4 * 1024 * 1024;

/// The private checkpoint is never placed in a public attempt audit event.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "boundary", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunBoundary {
    Terminal {
        status: RunStatus,
        error: Option<String>,
        usage: Usage,
    },
    Suspended {
        checkpoint: Value,
        usage: Usage,
    },
}
impl std::fmt::Debug for RunBoundary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RunBoundary([redacted])")
    }
}
/// The owner has joined every attempt-owned resource before making this claim.
/// The Store also checks native phase/call readiness; it cannot inspect resources.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptConclusion {
    pub finish: AttemptMutation,
    pub boundary: RunBoundary,
    /// Generated presentation metadata, excluded from operation semantics.
    pub created_at: OffsetDateTime,
    pub turn_id: Option<TurnId>,
    pub elapsed: std::time::Duration,
}
impl std::fmt::Debug for AttemptConclusion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AttemptConclusion([redacted])")
    }
}
impl AttemptConclusion {
    /// Check a genuine finish matching its logical boundary and bounded payload.
    /// # Errors
    /// Rejects unsupported outcomes, contradictory status and oversized/private
    /// checkpoint data that the bundled stores cannot persist.
    pub fn validate(&self) -> Result<AttemptFinish, AttemptError> {
        self.finish.validate()?;
        let AttemptCommand::Finish { outcome, .. } = self.finish.command else {
            return Err(AttemptError::InvalidRequest);
        };
        if self.elapsed.as_nanos() > i64::MAX as u128
            || self.turn_id.as_ref().is_some_and(|id| {
                id.0.is_empty() || id.0.len() > 512 || id.0.chars().any(char::is_control)
            })
        {
            return Err(AttemptError::InvalidRequest);
        }
        let expected = match &self.boundary {
            RunBoundary::Terminal { status, error, .. } => {
                if error.as_ref().is_some_and(|text| text.len() > 65_536) {
                    return Err(AttemptError::InvalidRequest);
                }
                match status {
                    RunStatus::Completed => AttemptFinish::Completed,
                    RunStatus::Failed => AttemptFinish::Failed,
                    RunStatus::Interrupted => AttemptFinish::Interrupted,
                    _ => return Err(AttemptError::InvalidRequest),
                }
            }
            RunBoundary::Suspended { checkpoint, .. } => {
                let mut remaining = MAX_RUN_BOUNDARY_BYTES;
                validate_checkpoint(checkpoint, 0, &mut remaining)?;
                AttemptFinish::Suspended
            }
        };
        if outcome != expected
            || crate::snapshot::encoded_bytes(&self.boundary, MAX_RUN_BOUNDARY_BYTES).is_none()
        {
            return Err(AttemptError::InvalidRequest);
        }
        crate::ensure_storable(&self.boundary).map_err(|_| AttemptError::InvalidRequest)?;
        Ok(outcome)
    }
    /// Canonical outbox semantics, distinct from a metadata-only Finish mutation.
    /// # Errors
    /// Rejects invalid boundary, identity or unpersistable private checkpoint.
    pub fn fingerprint(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<InputFingerprint, AttemptError> {
        self.validate()?;
        let mut value = serde_json::to_value((
            "crabber.attempt-conclusion.v1",
            self.finish.fingerprint(session, run)?,
            &self.boundary,
        ))
        .map_err(|_| AttemptError::InvalidRequest)?;
        value.sort_all_objects();
        InputFingerprint::new(format!(
            "{:x}",
            Sha256::digest(value.to_string().as_bytes())
        ))
        .map_err(|_| AttemptError::InvalidRequest)
    }
    /// Build the exact public source event without exposing checkpoint/error data.
    /// The adapter must separately verify ownership and write it atomically.
    /// # Errors
    /// Rejects invalid boundary or presentation metadata.
    pub fn materialize(&self, run: &Run) -> Result<EventRecord, AttemptError> {
        materialize_scope(self, &run.session_id, &run.id)
    }
    /// Apply the checked logical boundary inside a trusted adapter transaction.
    /// This does not verify ownership or establish resource cleanup.
    /// # Errors
    /// Rejects invalid or unpersistable resulting records.
    pub fn apply_run(&self, run: &mut Run, now: OffsetDateTime) -> Result<(), AttemptError> {
        self.validate()?;
        match &self.boundary {
            RunBoundary::Terminal {
                status,
                error,
                usage,
            } => {
                run.status = *status;
                run.error.clone_from(error);
                run.usage.clone_from(usage);
            }
            RunBoundary::Suspended { checkpoint, usage } => {
                run.usage.clone_from(usage);
                run.status = RunStatus::Paused;
                run.checkpoint = Some(checkpoint.clone());
                run.lease_until = now;
            }
        }
        run.updated_at = now;
        crate::ensure_storable(run).map_err(|_| AttemptError::InvalidRequest)
    }
    /// Validate matching historical conclusion evidence, including after a later
    /// continuation has advanced the logical run. No authority is restored.
    /// # Errors
    /// Returns an unknown outcome for inconsistent receipt/audit semantics.
    pub fn verify_replay(&self, receipt: &AttemptReceipt) -> Result<(), AttemptError> {
        let outcome = self.validate()?;
        if receipt.operation_id != self.finish.id
            || receipt.attempt.target != *self.finish.command.target()
            || receipt.attempt.state != outcome.into()
            || receipt.attempt.cleanup != AttemptCleanup::Complete
            || receipt.semantic_digest
                != self.fingerprint(&receipt.attempt.session_id, &receipt.attempt.run_id)?
        {
            return Err(AttemptError::UnknownStoreOutcome);
        }
        Ok(())
    }
}
fn validate_checkpoint(
    value: &Value,
    depth: usize,
    remaining: &mut usize,
) -> Result<(), AttemptError> {
    *remaining = remaining
        .checked_sub(1)
        .ok_or(AttemptError::InvalidRequest)?;
    if depth > 32 {
        return Err(AttemptError::InvalidRequest);
    }
    match value {
        Value::Array(values) => {
            for value in values {
                validate_checkpoint(value, depth + 1, remaining)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                *remaining = remaining
                    .checked_sub(key.len())
                    .ok_or(AttemptError::InvalidRequest)?;
                validate_checkpoint(value, depth + 1, remaining)?;
            }
        }
        Value::String(text) => {
            *remaining = remaining
                .checked_sub(text.len())
                .ok_or(AttemptError::InvalidRequest)?;
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => (),
    }
    Ok(())
}
