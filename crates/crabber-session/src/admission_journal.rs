//! Independent execution-start sources. Mutable execution state is never its own proof.
use crate::native_parent::RunIssuance;
use crate::{AdmissionExecutionRecord, AdmissionExecutionState, StoreError};
use crabber_core::{AdmissionKey, AdmissionReceipt, InputFingerprint, RunStatus};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const MAX_BYTES: usize = 8192;
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) enum Kind {
    Init,
    Begin,
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::Begin => "begin",
        }
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Init {
    version: u32,
    receipt: AdmissionReceipt,
    key: AdmissionKey,
    capsule_digest: String,
    pub issuance: RunIssuance,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Begin {
    version: u32,
    init_digest: InputFingerprint,
    pub issuance: RunIssuance,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "source",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum Source {
    Init(Init),
    Begin(Begin),
}
fn invalid() -> StoreError {
    StoreError::AdmissionConflict
}
impl Init {
    pub fn new(
        record: &AdmissionExecutionRecord,
        issuance: RunIssuance,
    ) -> Result<Self, StoreError> {
        let value = Self {
            version: 1,
            receipt: record.receipt.clone(),
            key: record.key.clone(),
            capsule_digest: record.capsule.digest().map_err(|_| invalid())?,
            issuance,
        };
        value.verify(record)?;
        Ok(value)
    }
    pub fn verify(&self, record: &AdmissionExecutionRecord) -> Result<(), StoreError> {
        self.issuance.validate().map_err(|_| invalid())?;
        if self.version != 1
            || self.issuance.sequence != 1
            || self.issuance.state.status != RunStatus::Running
            || self.issuance.state.session_id != record.receipt.session_id
            || self.issuance.state.run_id != record.receipt.run_id
            || self.receipt != record.receipt
            || self.key != record.key
            || self.capsule_digest != record.capsule.digest().map_err(|_| invalid())?
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn digest(&self) -> Result<InputFingerprint, StoreError> {
        let bytes = serde_json::to_vec(&("crabber.admission-execution-init.v1", self))
            .map_err(|_| invalid())?;
        InputFingerprint::new(format!("{:x}", Sha256::digest(bytes))).map_err(|_| invalid())
    }
}
impl Begin {
    pub fn new(init: &Init, issuance: RunIssuance) -> Result<Self, StoreError> {
        let value = Self {
            version: 1,
            init_digest: init.digest()?,
            issuance,
        };
        value.verify(init)?;
        Ok(value)
    }
    pub fn verify(&self, init: &Init) -> Result<(), StoreError> {
        self.issuance.validate().map_err(|_| invalid())?;
        if self.version != 1
            || self.init_digest != init.digest()?
            || self.issuance.state.status != RunStatus::Running
            || self.issuance.sequence < init.issuance.sequence
            || self.issuance.state.session_id != init.issuance.state.session_id
            || self.issuance.state.run_id != init.issuance.state.run_id
        {
            return Err(invalid());
        }
        Ok(())
    }
}
impl Source {
    pub fn kind(&self) -> Kind {
        match self {
            Self::Init(_) => Kind::Init,
            Self::Begin(_) => Kind::Begin,
        }
    }
    pub fn issuance(&self) -> &RunIssuance {
        match self {
            Self::Init(v) => &v.issuance,
            Self::Begin(v) => &v.issuance,
        }
    }
    pub fn encode(&self) -> Result<String, StoreError> {
        if crate::snapshot::encoded_bytes(self, MAX_BYTES).is_none() {
            return Err(invalid());
        }
        serde_json::to_string(self).map_err(|_| invalid())
    }
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub fn decode(text: &str) -> Result<Self, StoreError> {
        if text.len() > MAX_BYTES {
            return Err(invalid());
        }
        serde_json::from_str(text).map_err(|_| invalid())
    }
}
/// Presence is dictated by the independent relational cohort, never by absence
/// of rows or a mutable state label. Callers authenticate each referenced issuer.
pub(crate) fn verify(
    record: &AdmissionExecutionRecord,
    protocol: bool,
    init: Option<&Source>,
    begin: Option<&Source>,
) -> Result<(), StoreError> {
    if !protocol {
        return if init.is_none() && begin.is_none() {
            Ok(())
        } else {
            Err(invalid())
        };
    }
    let Some(Source::Init(init)) = init else {
        return Err(invalid());
    };
    init.verify(record)?;
    if let Some(Source::Begin(begin)) = begin {
        begin.verify(init)?;
    } else if begin.is_some() {
        return Err(invalid());
    }
    if (record.state == AdmissionExecutionState::Started) != begin.is_some() {
        return Err(invalid());
    }
    Ok(())
}

/// Original, versioned assertion minted only by the actual abandonment writer
/// after authenticating the protected Unstarted cohort and its complete input.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoEffect {
    version: u32,
    init_digest: InputFingerprint,
    pub issuance: RunIssuance,
}
impl NoEffect {
    pub fn new(init: &Init, issuance: RunIssuance) -> Result<Self, StoreError> {
        let value = Self {
            version: 1,
            init_digest: init.digest()?,
            issuance,
        };
        value.verify_init(init)?;
        Ok(value)
    }
    fn verify_init(&self, init: &Init) -> Result<(), StoreError> {
        self.issuance.validate().map_err(|_| invalid())?;
        if self.version != 1
            || self.init_digest != init.digest()?
            || self.issuance.sequence < init.issuance.sequence
            || self.issuance.state.status != RunStatus::Running
            || self.issuance.state.session_id != init.issuance.state.session_id
            || self.issuance.state.run_id != init.issuance.state.run_id
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn verify(
        &self,
        init: &Init,
        request: &crabber_core::AbandonRequest,
    ) -> Result<(), StoreError> {
        self.verify_init(init)?;
        let expected = crate::native_parent::RunState::new(
            &self.issuance.state.session_id,
            &request.expected.run_id,
            RunStatus::Running,
            &request.expected.claim_token,
        )
        .map_err(|_| invalid())?;
        if expected != self.issuance.state {
            return Err(invalid());
        }
        Ok(())
    }
}

impl Init {
    /// Original bounded source verification, independent of mutable execution
    /// state and current Run ownership. Used only for metadata-only recovery replay.
    pub(crate) fn verify_recovery(
        &self,
        request: &crate::ClaimUnstartedAdmissionRequest,
        initial: &RunIssuance,
        receipt: &AdmissionReceipt,
    ) -> Result<(), StoreError> {
        self.issuance.validate().map_err(|_| invalid())?;
        initial.validate().map_err(|_| invalid())?;
        if self.version != 1
            || self.issuance.sequence != 1
            || self.issuance.state.status != RunStatus::Running
            || self.issuance != *initial
            || self.receipt != *receipt
            || self.key != request.key
            || receipt.session_id != request.session_id
            || receipt.run_id != request.expected_fence.run_id
            || self.issuance.state.session_id != request.session_id
            || self.issuance.state.run_id != request.expected_fence.run_id
            || receipt.fingerprint != request.fingerprint
            || receipt.semantic_digest != request.semantic_digest
            || self.capsule_digest != request.capsule_digest
            || crate::snapshot::encoded_bytes(self, MAX_BYTES).is_none()
        {
            return Err(invalid());
        }
        Ok(())
    }
}
