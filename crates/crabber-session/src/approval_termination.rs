//! Explicit cancellation or expiry of a quiescent pre-effect approval batch.
use crate::{
    ApprovalError, ApprovalResolverClaim, ContinuationTarget, PhaseCommand, PhaseMutation,
    PhaseSettlement, PhaseSettlementRecords,
};
use crabber_core::{
    ControlId, EventKind, EventRecord, InputFingerprint, InteractionId, MessageId, PartId, Run,
    RunId, RunStatus, SessionId, SessionRevision, ToolCallRecord, ToolOutput, ToolResultStatus,
    TurnId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

/// Expiry names one required unanswered interaction whose Store-clock deadline
/// has passed. It explicitly fails the whole batch; other unanswered questions
/// are cancelled or expired according to their own deadlines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalTerminationKind {
    Cancel,
    Expire { interaction_id: InteractionId },
}
/// A host-authorized administrative decision, never an approval grant.
/// Only a waiting batch with a fully joined suspended attempt is eligible.
/// Claiming, started or uncertain work needs its separate lifecycle controls.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalTermination {
    pub id: ControlId,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub continuation: ContinuationTarget,
    pub expected_revision: SessionRevision,
    /// The host verifies administrative authority to terminate this run.
    pub resolver: ApprovalResolverClaim,
    pub kind: ApprovalTerminationKind,
}
impl std::fmt::Debug for ApprovalTermination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApprovalTermination([redacted])")
    }
}
impl ApprovalTermination {
    /// Check the original terminal receipt without granting worker authority.
    /// # Errors
    /// Rejects foreign, changed or contradictory persisted outcome evidence.
    pub fn verify_receipt(&self, receipt: &crate::ApprovalReceipt) -> Result<(), ApprovalError> {
        if receipt.operation_id != self.id
            || receipt.session_id != self.session_id
            || receipt.run_id != self.run_id
            || receipt.semantic_digest != self.fingerprint()?
            || receipt.revision <= self.expected_revision
            || receipt.event_cursor.0 == 0
            || receipt.outcome
                != (crate::ApprovalOutcome::Terminated {
                    continuation: self.continuation.clone(),
                    kind: self.kind.clone(),
                    resolver: self.resolver.clone(),
                    status: self.run_status(),
                })
        {
            return Err(ApprovalError::UnknownStoreOutcome);
        }
        Ok(())
    }
    /// Bind exact scope, generation, reason and host authority assertion.
    /// # Errors
    /// Rejects unbounded or invalid identifiers and claims.
    pub fn fingerprint(&self) -> Result<InputFingerprint, ApprovalError> {
        for id in [&self.id.0, &self.continuation.id.0] {
            crate::attempt::validate_identifier(id).map_err(|_| ApprovalError::InvalidRequest)?;
        }
        if let ApprovalTerminationKind::Expire { interaction_id } = &self.kind {
            crate::attempt::validate_identifier(&interaction_id.0)
                .map_err(|_| ApprovalError::InvalidRequest)?;
        }
        for (text, max) in [
            (&self.session_id.0, 512),
            (&self.run_id.0, 128),
            (&self.resolver.resolver_id, 512),
            (&self.resolver.authority_claim, 1024),
        ] {
            if text.is_empty() || text.len() > max || text.chars().any(char::is_control) {
                return Err(ApprovalError::InvalidRequest);
            }
        }
        crate::approval::control_fingerprint(&(
            "crabber.approval.terminate.v1",
            &self.session_id,
            &self.run_id,
            &self.continuation,
            self.expected_revision,
            &self.resolver,
            &self.kind,
        ))
    }
    #[must_use]
    pub fn run_status(&self) -> RunStatus {
        match self.kind {
            ApprovalTerminationKind::Cancel => RunStatus::Interrupted,
            ApprovalTerminationKind::Expire { .. } => RunStatus::Failed,
        }
    }
    /// Canonical empty error result for an unexecuted prepared call. No tool or
    /// middleware is invoked and no external execution outcome is invented.
    /// # Errors
    /// Rejects inconsistent scope or an already executed/settled call.
    pub fn materialize_call(
        &self,
        run: &Run,
        call: &ToolCallRecord,
        target: &crate::PhaseTarget,
        now: OffsetDateTime,
        turn_id: Option<TurnId>,
    ) -> Result<PhaseSettlementRecords, ApprovalError> {
        self.fingerprint()?;
        if self.session_id != run.session_id
            || self.run_id != run.id
            || call.run_id != run.id
            || call.id != target.call_id
            || call.status != crabber_core::ToolCallStatus::Pending
            || call.result.is_some()
        {
            return Err(ApprovalError::InvalidTransition);
        }
        let identity = serde_json::to_vec(&(&self.session_id, &self.run_id, &self.id, &call.id))
            .map_err(|_| ApprovalError::InvalidRequest)?;
        let suffix = format!("{:x}", Sha256::digest(identity));
        PhaseSettlement {
            completion: PhaseMutation {
                id: self.id.clone(),
                expected_revision: self.expected_revision,
                command: PhaseCommand::Complete {
                    target: target.clone(),
                    output: ToolOutput::new(vec![], true)
                        .map_err(|_| ApprovalError::InvalidRequest)?,
                },
            },
            result_status: match self.kind {
                ApprovalTerminationKind::Cancel => ToolResultStatus::Interrupted,
                ApprovalTerminationKind::Expire { .. } => ToolResultStatus::Failed,
            },
            message_id: MessageId(format!("approval-termination-result-{suffix}")),
            part_id: PartId(format!("approval-termination-part-{suffix}")),
            turn_id,
            created_at: now,
            duration_ms: 0,
        }
        .materialize(run, call)
        .map_err(|_| ApprovalError::InvalidRequest)
    }
    /// Canonical terminal source for the selected administrative outcome.
    /// The adapter separately verifies all pending calls and joined ownership.
    /// # Errors
    /// Rejects invalid scope, request or presentation metadata.
    pub fn run_event(
        &self,
        run: &Run,
        turn_id: Option<TurnId>,
        now: OffsetDateTime,
    ) -> Result<EventRecord, ApprovalError> {
        self.check_run(run)?;
        crate::approval::terminal::frame(self, &run.usage, turn_id, now)
    }

    /// Close the original turn when the frozen batch belongs to one.
    /// # Errors
    /// Rejects inconsistent run scope or invalid presentation metadata.
    pub fn turn_event(
        &self,
        run: &Run,
        turn_id: Option<TurnId>,
        now: OffsetDateTime,
    ) -> Result<Option<EventRecord>, ApprovalError> {
        let Some(turn_id) = turn_id else {
            return Ok(None);
        };
        let mut event = self.run_event(run, Some(turn_id), now)?;
        event.kind = EventKind::TurnCompleted;
        event.payload = serde_json::json!({});
        event.correlation = None;
        Ok(Some(event))
    }
    /// Apply terminal selection and revoke old worker fences inside the caller's
    /// already guarded transaction. Does not finish or reopen a native attempt.
    /// # Errors
    /// Rejects a mismatched or non-paused run.
    pub fn apply_run(&self, run: &mut Run, now: OffsetDateTime) -> Result<(), ApprovalError> {
        self.check_run(run)?;
        run.status = self.run_status();
        run.error = match self.kind {
            ApprovalTerminationKind::Cancel => None,
            ApprovalTerminationKind::Expire { .. } => Some("approval expired".into()),
        };
        run.claim_token = uuid::Uuid::new_v4().to_string();
        run.lease_until = now;
        run.updated_at = now;
        crate::ensure_storable(run).map_err(|_| ApprovalError::InvalidRequest)
    }
    fn check_run(&self, run: &Run) -> Result<(), ApprovalError> {
        self.fingerprint()?;
        if run.id != self.run_id
            || run.session_id != self.session_id
            || run.status != RunStatus::Paused
        {
            return Err(ApprovalError::InvalidTransition);
        }
        Ok(())
    }
}
