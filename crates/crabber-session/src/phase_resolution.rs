//! Host reconciliation of an uncertain external outcome; never worker authority.
use crate::{
    PhaseCommand, PhaseError, PhaseMutation, PhaseReceipt, PhaseSettlement, PhaseSettlementRecords,
    PhaseState, UncertainPhaseChoice,
};
use crabber_core::{
    InputFingerprint, MessageId, PartId, Run, RunId, SessionId, ToolCallRecord, ToolCallStatus,
    ToolOutput, ToolResult, ToolResultStatus, TurnId,
};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

/// An exact, host-asserted outcome for an already uncertain native phase.
/// Supplied output must already satisfy the host's result protection policy.
/// This control neither runs middleware nor asserts that cleanup has finished.
#[derive(Clone)]
pub struct PhaseResolution {
    pub session_id: SessionId,
    pub run_id: RunId,
    /// Must contain `PhaseCommand::ResolveUncertain`.
    pub resolution: PhaseMutation,
    pub message_id: MessageId,
    pub part_id: PartId,
    pub turn_id: Option<TurnId>,
    pub created_at: OffsetDateTime,
    pub duration_ms: u64,
}
impl std::fmt::Debug for PhaseResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PhaseResolution([redacted])")
    }
}
/// Metadata only. Neither a fresh outcome nor a replay permits dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhaseResolutionOutcome {
    Applied(PhaseReceipt),
    Replayed(PhaseReceipt),
}
impl PhaseResolution {
    /// Validate the complete bounded request, including generated metadata.
    /// # Errors
    /// Rejects invalid scope, presentation identifiers, or non-resolution commands.
    pub fn validate(&self) -> Result<(), PhaseError> {
        self.resolution
            .fingerprint(&self.session_id, &self.run_id)?;
        if !matches!(
            self.resolution.command,
            PhaseCommand::ResolveUncertain { .. }
        ) {
            return Err(PhaseError::InvalidRequest);
        }
        for id in std::iter::once(self.message_id.0.as_str())
            .chain(std::iter::once(self.part_id.0.as_str()))
            .chain(self.turn_id.as_ref().map(|id| id.0.as_str()))
        {
            if id.is_empty() || id.len() > 512 || id.chars().any(char::is_control) {
                return Err(PhaseError::InvalidRequest);
            }
        }
        Ok(())
    }
    /// Hash original choice/output and authority claim, excluding presentation.
    /// # Errors
    /// Rejects an invalid resolution request.
    pub fn fingerprint(&self) -> Result<InputFingerprint, PhaseError> {
        self.validate()?;
        let digest = self
            .resolution
            .fingerprint(&self.session_id, &self.run_id)?;
        InputFingerprint::new(format!(
            "{:x}",
            Sha256::digest(format!("crabber.phase-resolution.v1:{}", digest.as_str()).as_bytes())
        ))
        .map_err(|_| PhaseError::InvalidRequest)
    }
    /// Normalize a failure without changing its original retry semantics.
    /// # Errors
    /// Rejects an invalid resolution request.
    pub fn output(&self) -> Result<Option<ToolOutput>, PhaseError> {
        self.validate()?;
        let PhaseCommand::ResolveUncertain { choice, .. } = &self.resolution.command else {
            return Err(PhaseError::InvalidRequest);
        };
        match choice {
            UncertainPhaseChoice::Success { output } => Ok(Some(output.clone())),
            UncertainPhaseChoice::Failure { output } => Ok(Some(match output {
                Some(output) => output.clone().with_error(),
                None => ToolOutput::new(vec![], true).map_err(|_| PhaseError::InvalidRequest)?,
            })),
            UncertainPhaseChoice::Refuse => Ok(None),
        }
    }
    /// Build canonical logical result records; refusal produces none.
    /// The adapter must atomically commit these with the native resolution,
    /// preserving its existing quiescence and parent ownership unchanged.
    /// # Errors
    /// Rejects inconsistent scope or an already settled logical call.
    pub fn materialize(
        &self,
        run: &Run,
        call: &ToolCallRecord,
    ) -> Result<Option<PhaseSettlementRecords>, PhaseError> {
        if run.session_id != self.session_id
            || run.id != self.run_id
            || call.run_id != run.id
            || call.id != self.resolution.command.target().call_id
        {
            return Err(PhaseError::NotFound);
        }
        if !matches!(
            call.status,
            ToolCallStatus::Pending | ToolCallStatus::Running
        ) || call.result.is_some()
        {
            return Err(PhaseError::InvalidTransition);
        }
        let Some(output) = self.output()? else {
            return Ok(None);
        };
        // Reuse only canonical record construction. Never route this descriptor
        // through worker settlement, which additionally asserts cleanup Complete.
        let records = PhaseSettlement {
            completion: PhaseMutation {
                command: PhaseCommand::Complete {
                    target: self.resolution.command.target().clone(),
                    output: output.clone(),
                },
                ..self.resolution.clone()
            },
            result_status: if output.is_error() {
                ToolResultStatus::Failed
            } else {
                ToolResultStatus::Completed
            },
            message_id: self.message_id.clone(),
            part_id: self.part_id.clone(),
            turn_id: self.turn_id.clone(),
            created_at: self.created_at,
            duration_ms: self.duration_ms,
        }
        .materialize(run, call)?;
        Ok(Some(records))
    }
    /// Verify original receipt semantics and immutable committed logical output.
    /// Receipt quiescence is historical; later cleanup does not invalidate replay.
    /// Refusal replay also remains valid after a later independently keyed outcome.
    /// # Errors
    /// Rejects inconsistent persisted evidence. Adapters also verify audit records.
    pub fn verify_replay(
        &self,
        receipt: &PhaseReceipt,
        call: &ToolCallRecord,
    ) -> Result<(), PhaseError> {
        if receipt.operation_id != self.resolution.id
            || receipt.phase.session_id != self.session_id
            || receipt.phase.run_id != self.run_id
            || receipt.phase.target != *self.resolution.command.target()
            || receipt.operation != crate::phase::operation(&self.resolution.command)
            || receipt.resolver != crate::phase::resolver(&self.resolution.command)
            || receipt.semantic_digest != self.fingerprint()?
            || call.run_id != self.run_id
            || call.id != receipt.phase.target.call_id
            || crate::phase::call_binding(call)? != receipt.phase.call_binding
        {
            return Err(PhaseError::UnknownStoreOutcome);
        }
        let Some(output) = self.output()? else {
            return if receipt.phase.state == PhaseState::Uncertain {
                Ok(())
            } else {
                Err(PhaseError::UnknownStoreOutcome)
            };
        };
        let (phase, status, call_status) = if output.is_error() {
            (
                PhaseState::Failed,
                ToolResultStatus::Failed,
                ToolCallStatus::Failed,
            )
        } else {
            (
                PhaseState::Completed,
                ToolResultStatus::Completed,
                ToolCallStatus::Completed,
            )
        };
        let result = ToolResult {
            status,
            content: output.to_content_blocks(),
        };
        if receipt.phase.state != phase
            || call.status != call_status
            || call.result.as_ref() != Some(&result)
        {
            return Err(PhaseError::UnknownStoreOutcome);
        }
        Ok(())
    }
}
