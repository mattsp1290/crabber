//! Compound native completion of a phase and its logical tool call.
use crate::{PhaseCommand, PhaseError, PhaseMutation};
use crabber_core::{
    ContentBlock, EventKind, EventRecord, InputFingerprint, Message, MessageId, Part, PartId,
    PartKind, Role, Run, SessionId, ToolCallRecord, ToolCallStatus, ToolOutput, ToolResult,
    ToolResultStatus, TurnId,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

/// A genuine terminal output whose phase-owned resources the worker has joined.
/// The Store records that assertion; it cannot inspect native resources.
///
/// `completion` must be `PhaseCommand::Complete`. It binds the exact original
/// operation key, revision, phase/generation/call/attempt and protected output.
/// Presentation IDs/timing are generated metadata: retries may regenerate them,
/// but a replay never appends a second message or republishes a terminal event.
#[derive(Clone)]
pub struct PhaseSettlement {
    pub completion: PhaseMutation,
    pub message_id: MessageId,
    pub part_id: PartId,
    pub turn_id: Option<TurnId>,
    pub created_at: OffsetDateTime,
    pub duration_ms: u64,
}
impl std::fmt::Debug for PhaseSettlement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PhaseSettlement([redacted])")
    }
}
/// Canonical protected result records for a trusted custom Store transaction.
/// Constructing these records grants no fence or permission to dispatch work.
pub struct PhaseSettlementRecords {
    pub result: ToolResult,
    pub message: Message,
    pub event: EventRecord,
    pub call_status: ToolCallStatus,
}
impl PhaseSettlement {
    /// Validate the bounded request and obtain its protected output.
    /// # Errors
    /// Rejects non-completion commands and invalid generated presentation IDs.
    pub fn output(&self) -> Result<&ToolOutput, PhaseError> {
        self.completion.validate()?;
        for id in std::iter::once(self.message_id.0.as_str())
            .chain(std::iter::once(self.part_id.0.as_str()))
            .chain(self.turn_id.as_ref().map(|id| id.0.as_str()))
        {
            if id.is_empty() || id.len() > 512 || id.chars().any(char::is_control) {
                return Err(PhaseError::InvalidRequest);
            }
        }
        let PhaseCommand::Complete { output, .. } = &self.completion.command else {
            return Err(PhaseError::InvalidRequest);
        };
        Ok(output)
    }
    /// Domain-separated semantics for the complete compound operation. The old
    /// metadata-only Complete operation cannot replay as logical settlement.
    /// # Errors
    /// Rejects an invalid completion operation or target.
    pub fn fingerprint(
        &self,
        session: &SessionId,
        run: &crabber_core::RunId,
    ) -> Result<InputFingerprint, PhaseError> {
        self.output()?;
        let digest = self.completion.fingerprint(session, run)?;
        InputFingerprint::new(format!(
            "{:x}",
            Sha256::digest(format!("crabber.phase-settlement.v1:{}", digest.as_str()).as_bytes())
        ))
        .map_err(|_| PhaseError::InvalidRequest)
    }
    /// Verify original compound semantics and the committed logical result.
    /// Adapters must also validate the receipt's persisted audit event.
    /// # Errors
    /// Returns unknown outcome for inconsistent receipt/result evidence.
    pub fn verify_replay(
        &self,
        receipt: &crate::PhaseReceipt,
        call: &ToolCallRecord,
    ) -> Result<(), PhaseError> {
        let output = self.output()?;
        let expected = ToolResult {
            status: if output.is_error() {
                ToolResultStatus::Failed
            } else {
                ToolResultStatus::Completed
            },
            content: output.to_content_blocks(),
        };
        if receipt.operation != crate::PhaseOperation::Complete
            || receipt.operation_id != self.completion.id
            || receipt.phase.target != *self.completion.command.target()
            || receipt.semantic_digest
                != self.fingerprint(&receipt.phase.session_id, &receipt.phase.run_id)?
            || receipt.phase.state
                != if output.is_error() {
                    crate::PhaseState::Failed
                } else {
                    crate::PhaseState::Completed
                }
            || receipt.phase.quiescence != crate::PhaseQuiescence::Complete
            || call.run_id != receipt.phase.run_id
            || call.id != receipt.phase.target.call_id
            || call.status
                != if output.is_error() {
                    ToolCallStatus::Failed
                } else {
                    ToolCallStatus::Completed
                }
            || call.result.as_ref() != Some(&expected)
        {
            return Err(PhaseError::UnknownStoreOutcome);
        }
        Ok(())
    }
    /// Build canonical records using the authoritative run and staged call.
    /// The adapter must separately verify native ownership and atomically write
    /// these records with phase completion/quiescence and its receipt.
    /// # Errors
    /// Rejects mismatched stored scope or unpersistable result records.
    pub fn materialize(
        &self,
        run: &Run,
        call: &ToolCallRecord,
    ) -> Result<PhaseSettlementRecords, PhaseError> {
        let output = self.output()?;
        if call.run_id != run.id || call.id != self.completion.command.target().call_id {
            return Err(PhaseError::NotFound);
        }
        let content = output.to_content_blocks();
        let (status, call_status) = if output.is_error() {
            (ToolResultStatus::Failed, ToolCallStatus::Failed)
        } else {
            (ToolResultStatus::Completed, ToolCallStatus::Completed)
        };
        let result = ToolResult {
            status,
            content: content.clone(),
        };
        let message = Message {
            id: self.message_id.clone(),
            session_id: run.session_id.clone(),
            run_id: Some(run.id.clone()),
            role: Role::Tool,
            parent_id: None,
            parts: vec![Part {
                id: self.part_id.clone(),
                message_id: self.message_id.clone(),
                ordinal: 0,
                kind: PartKind::FunctionToolResult,
                content: ContentBlock::ToolResult {
                    call_id: call.id.clone(),
                    content: content.clone(),
                    is_error: output.is_error(),
                },
            }],
            created_at: self.created_at,
        };
        let event = EventRecord {
            cursor: None,
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            turn_id: self.turn_id.clone(),
            kind: EventKind::ToolCallSettled,
            payload: json!({"call_id":call.id,"message_id":self.message_id,"content":content,"is_error":output.is_error(),
                "name":call.name,"status":status,"tool":call.name,"tool_id":call.id.to_string(),"duration_ms":self.duration_ms}),
            correlation: Some(call.id.to_string()),
            live_only: false,
            created_at: self.created_at,
        };
        crate::ensure_storable(&message).map_err(|_| PhaseError::InvalidRequest)?;
        crate::ensure_storable(&event).map_err(|_| PhaseError::InvalidRequest)?;
        Ok(PhaseSettlementRecords {
            result,
            message,
            event,
            call_status,
        })
    }
}
