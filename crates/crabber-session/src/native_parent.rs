//! Private token issuance evidence for a genuine native Begin. Reads never
//! produce authority, and absence is not reconstructed from mutable records.
#[path = "native_parent/completion.rs"]
mod completion;
#[path = "native_parent/completion_history.rs"]
pub(crate) mod completion_history;
#[path = "native_parent/historical.rs"]
mod historical;
#[path = "native_parent/run.rs"]
mod run;
#[path = "native_parent/source_protocol.rs"]
pub(crate) mod source_protocol;
use crate::{
    AttemptCleanup, AttemptReceipt, AttemptState, AttemptTarget, PhaseOperation, PhaseReceipt,
    PhaseTarget,
};
pub(crate) use completion::{CompletionOperation, CompletionSource};
use crabber_core::{
    ControlId, EventCursor, EventRecord, InputFingerprint, Run, RunId, RunStatus, SessionId,
};
pub(crate) use run::{RunIssuance, RunState, advance};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("native parent evidence unavailable")]
pub(crate) struct Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Kind {
    Attempt,
    Phase,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Source {
    pub kind: Kind,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub operation_id: ControlId,
    pub event_cursor: EventCursor,
    pub semantic_digest: InputFingerprint,
    pub attempt: AttemptTarget,
    pub phase: Option<PhaseTarget>,
    pub call_binding: Option<InputFingerprint>,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Witness {
    pub version: u32,
    pub run_issuance: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_issuance_digest: Option<InputFingerprint>,
    pub source: Source,
    pub run_token: InputFingerprint,
    pub attempt_token: InputFingerprint,
    pub attempt_run_token: InputFingerprint,
    pub phase_token: Option<InputFingerprint>,
}
impl Witness {
    pub fn attempt(
        run: &Run,
        attempt: &crate::attempt::AttemptRecord,
        receipt: &AttemptReceipt,
        issuance: &RunIssuance,
    ) -> Result<Self, Error> {
        if crate::snapshot::encoded_bytes(receipt, 8192).is_none()
            || receipt.attempt != attempt.snapshot
            || receipt.attempt.state != AttemptState::Started
        {
            return Err(Error);
        }
        Self::new(
            run,
            issuance,
            attempt,
            None,
            Source {
                kind: Kind::Attempt,
                session_id: receipt.attempt.session_id.clone(),
                run_id: receipt.attempt.run_id.clone(),
                operation_id: receipt.operation_id.clone(),
                event_cursor: receipt.event_cursor,
                semantic_digest: receipt.semantic_digest.clone(),
                attempt: receipt.attempt.target.clone(),
                phase: None,
                call_binding: None,
            },
        )
    }
    pub fn phase(
        run: &Run,
        attempt: &crate::attempt::AttemptRecord,
        phase: &crate::phase::PhaseRecord,
        receipt: &PhaseReceipt,
        issuance: &RunIssuance,
    ) -> Result<Self, Error> {
        if crate::snapshot::encoded_bytes(receipt, 8192).is_none() {
            return Err(Error);
        }
        phase.validate().map_err(|_| Error)?;
        if receipt.phase != phase.snapshot
            || receipt.operation != PhaseOperation::Begin
            || phase.snapshot.state != crate::PhaseState::Started
            || phase.snapshot.quiescence != crate::PhaseQuiescence::Pending
        {
            return Err(Error);
        }
        Self::new(
            run,
            issuance,
            attempt,
            Some(&phase.token),
            Source {
                kind: Kind::Phase,
                session_id: receipt.phase.session_id.clone(),
                run_id: receipt.phase.run_id.clone(),
                operation_id: receipt.operation_id.clone(),
                event_cursor: receipt.event_cursor,
                semantic_digest: receipt.semantic_digest.clone(),
                attempt: receipt.phase.target.attempt.clone(),
                phase: Some(receipt.phase.target.clone()),
                call_binding: Some(receipt.phase.call_binding.clone()),
            },
        )
    }
    fn new(
        run: &Run,
        issuance: &RunIssuance,
        attempt: &crate::attempt::AttemptRecord,
        phase_token: Option<&str>,
        source: Source,
    ) -> Result<Self, Error> {
        issuance.verify(run)?;
        attempt.validate().map_err(|_| Error)?;
        source.validate()?;
        if run.status != RunStatus::Running
            || source.session_id != run.session_id
            || source.run_id != run.id
            || source.attempt != attempt.snapshot.target
            || attempt.snapshot.session_id != run.session_id
            || attempt.snapshot.run_id != run.id
            || attempt.snapshot.state != AttemptState::Started
            || attempt.snapshot.cleanup != AttemptCleanup::Pending
            || attempt.run_token != run.claim_token
        {
            return Err(Error);
        }
        Ok(Self {
            version: 2,
            run_issuance: issuance.sequence,
            run_issuance_digest: Some(issuance_digest(issuance)?),
            run_token: token_hash(&source, "run", &run.claim_token)?,
            attempt_token: token_hash(&source, "attempt", &attempt.token)?,
            attempt_run_token: token_hash(&source, "attempt-run", &attempt.run_token)?,
            phase_token: phase_token
                .map(|token| token_hash(&source, "phase", token))
                .transpose()?,
            source,
        })
    }
    pub fn validate(&self) -> Result<(), Error> {
        self.source.validate()?;
        if !matches!(
            (self.version, self.run_issuance_digest.is_some()),
            (1, false) | (2, true)
        ) || self.run_issuance == 0
            || self.run_issuance > i64::MAX as u64
            || (self.source.kind == Kind::Phase) != self.phase_token.is_some()
            || crate::snapshot::encoded_bytes(self, 8192).is_none()
        {
            return Err(Error);
        }
        Ok(())
    }

    pub fn verify_attempt(
        &self,
        run: &Run,
        attempt: &crate::attempt::AttemptRecord,
        receipt: &AttemptReceipt,
        event: &EventRecord,
        issuance: &RunIssuance,
    ) -> Result<(), Error> {
        self.validate()?;
        attempt_source(receipt, event)?;
        self.verify_creation(Self::attempt(run, attempt, receipt, issuance)?)
    }

    pub fn verify_phase(
        &self,
        run: &Run,
        attempt: &crate::attempt::AttemptRecord,
        phase: &crate::phase::PhaseRecord,
        receipt: &PhaseReceipt,
        event: &EventRecord,
        issuance: &RunIssuance,
    ) -> Result<(), Error> {
        self.validate()?;
        phase_source(receipt, event)?;
        self.verify_creation(Self::phase(run, attempt, phase, receipt, issuance)?)
    }

    fn verify_creation(&self, mut expected: Self) -> Result<(), Error> {
        // Legacy live-parent checks remain usable, but no historical coverage
        // upgrades an old witness without its original independently bound issuer.
        if self.version == 1 {
            expected.version = 1;
            expected.run_issuance_digest = None;
        }
        if *self != expected {
            return Err(Error);
        }
        Ok(())
    }
}
pub(crate) fn issuance_digest(issuance: &RunIssuance) -> Result<InputFingerprint, Error> {
    crate::session_owner::validation::digest(&(
        "crabber.native-parent.original-issuer.v2",
        issuance,
    ))
    .map_err(|_| Error)
}
impl Source {
    fn validate(&self) -> Result<(), Error> {
        if self.session_id.0.is_empty()
            || self.session_id.0.len() > 512
            || self.session_id.0.chars().any(char::is_control)
        {
            return Err(Error);
        }
        for id in [&self.run_id.0, &self.operation_id.0, &self.attempt.id.0] {
            crate::attempt::validate_identifier(id).map_err(|_| Error)?;
        }
        if self.event_cursor.0 == 0
            || (self.kind == Kind::Phase) != self.phase.is_some()
            || self.phase.is_some() != self.call_binding.is_some()
        {
            return Err(Error);
        }
        if let Some(phase) = &self.phase {
            crate::attempt::validate_identifier(&phase.id.0).map_err(|_| Error)?;
            if phase.call_id.0.is_empty()
                || phase.call_id.0.len() > 512
                || phase.call_id.0.chars().any(char::is_control)
                || phase.attempt != self.attempt
            {
                return Err(Error);
            }
        }
        Ok(())
    }
}
fn token_hash(source: &Source, role: &str, token: &str) -> Result<InputFingerprint, Error> {
    if token.is_empty() || token.len() > 512 || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(Error);
    }
    let value = ("crabber.native-parent-issuance.v1", role, source, token);
    if crate::snapshot::encoded_bytes(&value, 16384).is_none() {
        return Err(Error);
    }
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).map_err(|_| Error)?)
    ))
    .map_err(|_| Error)
}

pub(crate) fn attempt_source(receipt: &AttemptReceipt, event: &EventRecord) -> Result<(), Error> {
    if crate::snapshot::encoded_bytes(receipt, 8192).is_none()
        || crate::snapshot::encoded_bytes(event, 65536).is_none()
    {
        return Err(Error);
    }
    crate::attempt::verify_receipt(receipt, event).map_err(|_| Error)?;
    if event
        .payload
        .pointer("/command/operation")
        .and_then(serde_json::Value::as_str)
        != Some("begin")
        || receipt.attempt.state != AttemptState::Started
        || receipt.attempt.cleanup != AttemptCleanup::Pending
    {
        return Err(Error);
    }
    Ok(())
}

pub(crate) fn phase_source(receipt: &PhaseReceipt, event: &EventRecord) -> Result<(), Error> {
    if crate::snapshot::encoded_bytes(receipt, 8192).is_none()
        || crate::snapshot::encoded_bytes(event, 65536).is_none()
    {
        return Err(Error);
    }
    crate::phase::verify_receipt(receipt, event).map_err(|_| Error)?;
    if receipt.operation != PhaseOperation::Begin
        || receipt.phase.state != crate::PhaseState::Started
        || receipt.phase.quiescence != crate::PhaseQuiescence::Pending
    {
        return Err(Error);
    }
    Ok(())
}
