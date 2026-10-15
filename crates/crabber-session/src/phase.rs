//! Experimental native tool-phase ownership, separate from logical call settlement.
mod records;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) mod sql;
use crate::{AttemptAuthority, AttemptTarget};
use crabber_core::{
    ControlId, EventCursor, InputFingerprint, PhaseGeneration, PhaseId, RunId, SessionId,
    SessionRevision, ToolCallId, ToolOutput,
};
pub(crate) use records::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PhaseError {
    #[error("tool phases are unsupported")]
    Unsupported,
    #[error("tool phase target not found")]
    NotFound,
    #[error("invalid tool phase operation")]
    InvalidRequest,
    #[error("tool phase semantics conflict")]
    SemanticConflict,
    #[error("public session revision changed")]
    RevisionConflict,
    #[error("public session revision exhausted")]
    RevisionExhausted,
    #[error("tool phase generation changed")]
    GenerationConflict,
    #[error("tool phase generation exhausted")]
    GenerationExhausted,
    #[error("tool phase still has an owner")]
    Busy,
    #[error("tool phase owner changed")]
    StaleOwner,
    #[error("tool phase owner still has authority")]
    LiveOwner,
    #[error("invalid tool phase transition")]
    InvalidTransition,
    #[error("tool phase requires domain reconciliation")]
    Uncertain,
    #[error("tool phase storage outcome is unknown")]
    UnknownStoreOutcome,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhaseState {
    Claimed,
    Started,
    Suspended,
    Completed,
    Failed,
    Uncertain,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhaseQuiescence {
    Pending,
    Complete,
    Incomplete,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhaseQuiescenceResult {
    Complete,
    Incomplete,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseTarget {
    pub id: PhaseId,
    pub generation: PhaseGeneration,
    pub call_id: ToolCallId,
    pub attempt: AttemptTarget,
}
/// Host-asserted identity/authority, never a human credential or model instruction.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseResolverClaim {
    pub resolver_id: String,
    pub authority_claim: String,
}
impl std::fmt::Debug for PhaseResolverClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PhaseResolverClaim([redacted])")
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "choice", rename_all = "snake_case", deny_unknown_fields)]
pub enum UncertainPhaseChoice {
    Success {
        output: ToolOutput,
    },
    Failure {
        #[serde(default)]
        output: Option<ToolOutput>,
    },
    Refuse,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum PhaseCommand {
    Claim {
        target: PhaseTarget,
    },
    Begin {
        target: PhaseTarget,
    },
    Complete {
        target: PhaseTarget,
        output: ToolOutput,
    },
    MarkExpiredUncertain {
        target: PhaseTarget,
    },
    MarkStoppedUncertain {
        target: PhaseTarget,
        claim: PhaseResolverClaim,
    },
    ResolveUncertain {
        target: PhaseTarget,
        claim: PhaseResolverClaim,
        choice: UncertainPhaseChoice,
    },
    RecordQuiescence {
        target: PhaseTarget,
        result: PhaseQuiescenceResult,
        host_claim: Option<PhaseResolverClaim>,
    },
}
impl PhaseCommand {
    #[must_use]
    pub fn target(&self) -> &PhaseTarget {
        match self {
            Self::Claim { target }
            | Self::Begin { target }
            | Self::Complete { target, .. }
            | Self::MarkExpiredUncertain { target }
            | Self::MarkStoppedUncertain { target, .. }
            | Self::ResolveUncertain { target, .. }
            | Self::RecordQuiescence { target, .. } => target,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseMutation {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub command: PhaseCommand,
}
impl PhaseMutation {
    /// Hash the original request for an outbox. IDs and claimed authority never
    /// substitute for a current store fence or native worker authority.
    /// # Errors
    /// Rejects unbounded identity/claims and a contradictory success choice.
    pub fn fingerprint(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<InputFingerprint, PhaseError> {
        self.validate()?;
        if session.0.is_empty()
            || session.0.len() > 512
            || session.0.chars().any(char::is_control)
            || run.0.is_empty()
            || run.0.len() > 128
            || run.0.chars().any(char::is_control)
        {
            return Err(PhaseError::InvalidRequest);
        }
        let mut value = serde_json::to_value((
            "crabber.phase.v1",
            session,
            run,
            self.expected_revision,
            &self.command,
        ))
        .map_err(|_| PhaseError::InvalidRequest)?;
        value.sort_all_objects();
        InputFingerprint::new(format!(
            "{:x}",
            Sha256::digest(value.to_string().as_bytes())
        ))
        .map_err(|_| PhaseError::InvalidRequest)
    }
    pub(crate) fn validate(&self) -> Result<(), PhaseError> {
        records::validate_request(self)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseSnapshot {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub target: PhaseTarget,
    pub call_binding: InputFingerprint,
    pub state: PhaseState,
    pub quiescence: PhaseQuiescence,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionedPhase {
    pub revision: SessionRevision,
    pub phase: PhaseSnapshot,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseOperation {
    Claim,
    Begin,
    Complete,
    MarkExpiredUncertain,
    MarkStoppedUncertain,
    ResolveSuccess,
    ResolveFailure,
    ResolveRefuse,
    QuiescenceComplete,
    QuiescenceIncomplete,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseReceipt {
    pub operation: PhaseOperation,
    pub resolver: Option<PhaseResolverClaim>,
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub revision: SessionRevision,
    pub event_cursor: EventCursor,
    pub phase: PhaseSnapshot,
}
/// Native worker authority; never serialized or returned by receipt lookup/replay.
#[derive(Clone)]
pub struct PhaseAuthority {
    snapshot: Box<PhaseSnapshot>,
    token: String,
    attempt: Box<AttemptAuthority>,
}
impl std::fmt::Debug for PhaseAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PhaseAuthority([redacted])")
    }
}
impl PhaseAuthority {
    /// Construct only after a trusted Store adapter commits a fresh phase claim.
    /// # Errors
    /// Rejects invalid opaque-token representation.
    pub fn from_store(
        snapshot: PhaseSnapshot,
        token: String,
        attempt: AttemptAuthority,
    ) -> Result<Self, PhaseError> {
        if token.is_empty() || token.len() > 512 || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(PhaseError::InvalidRequest);
        }
        Ok(Self {
            snapshot: Box::new(snapshot),
            token,
            attempt: Box::new(attempt),
        })
    }
    #[must_use]
    pub fn matches_store(&self, snapshot: &PhaseSnapshot, token: &str) -> bool {
        self.snapshot.session_id == snapshot.session_id
            && self.snapshot.run_id == snapshot.run_id
            && self.snapshot.target == snapshot.target
            && self.snapshot.call_binding == snapshot.call_binding
            && self.token == token
    }
    /// Validate the adapter's private parent record without exposing the
    /// broader attempt capability to a phase owner.
    #[must_use]
    pub fn matches_attempt_store(
        &self,
        snapshot: &crate::AttemptSnapshot,
        token: &str,
        run_token: &str,
    ) -> bool {
        self.attempt.matches_store(snapshot, token, run_token)
    }
}
#[derive(Debug, Clone)]
pub enum PhaseWriteAuthority {
    Attempt(AttemptAuthority),
    Phase(PhaseAuthority),
}
#[derive(Debug)]
pub enum PhaseMutationOutcome {
    Applied {
        receipt: PhaseReceipt,
        authority: Option<PhaseAuthority>,
    },
    Replayed(PhaseReceipt),
}
