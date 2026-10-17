//! Exact queue controls anchored only by their actual native publication.
use crate::queue::{
    Audit, QueueAction, QueueId, QueueReceipt, SessionId, StoreError, corrupt, verify_audit,
};
use crabber_core::{ControlId, InputFingerprint, SessionRevision};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[path = "source/ledger.rs"]
mod ledger;
pub(crate) use ledger::Ledger;
pub(crate) const MAX_SOURCE: usize = 4096;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    Enqueue,
    Claim,
    Prepare,
    Cancel,
    Admit,
}
impl Kind {
    pub(crate) fn of(action: &QueueAction) -> Self {
        match action {
            QueueAction::Enqueue { .. } => Self::Enqueue,
            QueueAction::Claim { .. } => Self::Claim,
            QueueAction::Prepare { .. } => Self::Prepare,
            QueueAction::Cancel { .. } => Self::Cancel,
            QueueAction::Admit { .. } => Self::Admit,
        }
    }
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Enqueue => "enqueue",
            Self::Claim => "claim",
            Self::Prepare => "prepare",
            Self::Cancel => "cancel",
            Self::Admit => "admit",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Scope {
    pub session: SessionId,
    pub queue: QueueId,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Source {
    pub version: u32,
    pub scope: Scope,
    pub queue_sequence: u64,
    pub sequence: u64,
    pub operation: ControlId,
    pub kind: Kind,
    pub revision: SessionRevision,
    pub receipt_digest: InputFingerprint,
    pub previous: Option<InputFingerprint>,
    pub digest: InputFingerprint,
}
impl Source {
    /// Caller precharges the borrowed full audit under its enclosing Proof first.
    pub(crate) fn new(
        audit: &Audit,
        sequence: u64,
        previous: Option<InputFingerprint>,
    ) -> Result<Self, StoreError> {
        verify_audit(audit, &audit.receipt)?;
        let receipt = &audit.receipt;
        let value = Self {
            version: 1,
            scope: Scope {
                session: receipt.session_id.clone(),
                queue: receipt.summary.queue_id.clone(),
            },
            queue_sequence: receipt.summary.sequence,
            sequence,
            operation: receipt.operation_id.clone(),
            kind: Kind::of(&audit.request.action),
            revision: receipt.revision,
            receipt_digest: receipt_digest(receipt)?,
            digest: digest(audit, sequence, previous.as_ref())?,
            previous,
        };
        value.validate()?;
        Ok(value)
    }
    pub(crate) fn validate(&self) -> Result<(), StoreError> {
        crate::queue::scope(&self.scope.session, &self.operation)?;
        crate::queue::identifier(&self.scope.queue.0)?;
        if self.version != 1
            || self.queue_sequence == 0
            || self.sequence == 0
            || self.queue_sequence > i64::MAX as u64
            || self.sequence > i64::MAX as u64
            || (self.sequence == 1) != self.previous.is_none()
            || (self.sequence == 1) != (self.kind == Kind::Enqueue)
            || crate::snapshot::encoded_bytes(self, MAX_SOURCE).is_none()
        {
            return Err(corrupt());
        }
        Ok(())
    }
    pub(crate) fn verify(&self, audit: &Audit) -> Result<(), StoreError> {
        self.validate()?;
        if self != &Self::new(audit, self.sequence, self.previous.clone())? {
            return Err(corrupt());
        }
        Ok(())
    }
    pub(crate) fn follows(&self, previous: &Self) -> Result<(), StoreError> {
        if self.scope != previous.scope
            || self.queue_sequence != previous.queue_sequence
            || self.sequence != previous.sequence.checked_add(1).ok_or_else(corrupt)?
            || self.previous.as_ref() != Some(&previous.digest)
            || self.revision <= previous.revision
        {
            return Err(corrupt());
        }
        Ok(())
    }
    /// Independently authenticates the safe full original receipt, without
    /// materializing the private original `QueueInput` or preparation capsule.
    pub(crate) fn verify_receipt(&self, receipt: &QueueReceipt) -> Result<(), StoreError> {
        self.validate()?;
        if !self.matches_receipt(receipt) || self.receipt_digest != receipt_digest(receipt)? {
            return Err(corrupt());
        }
        Ok(())
    }
    pub(crate) fn matches_receipt(&self, receipt: &QueueReceipt) -> bool {
        self.scope.session == receipt.session_id
            && self.scope.queue == receipt.summary.queue_id
            && self.operation == receipt.operation_id
            && self.queue_sequence == receipt.summary.sequence
            && self.revision == receipt.revision
    }
}
fn digest(
    audit: &Audit,
    sequence: u64,
    previous: Option<&InputFingerprint>,
) -> Result<InputFingerprint, StoreError> {
    if crate::snapshot::encoded_bytes(audit, crate::queue::MAX_RECORD).is_none() {
        return Err(corrupt());
    }
    let mut value =
        serde_json::to_value(("crabber.queue-publication.v1", sequence, previous, audit))
            .map_err(|_| corrupt())?;
    value.sort_all_objects();
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).map_err(|_| corrupt())?)
    ))
    .map_err(|_| corrupt())
}

fn receipt_digest(receipt: &QueueReceipt) -> Result<InputFingerprint, StoreError> {
    if crate::snapshot::encoded_bytes(receipt, 16384).is_none() {
        return Err(corrupt());
    }
    let mut value = serde_json::to_value(("crabber.queue-publication-receipt.v1", receipt))
        .map_err(|_| corrupt())?;
    value.sort_all_objects();
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).map_err(|_| corrupt())?)
    ))
    .map_err(|_| corrupt())
}
