//! Private, retained evidence for initial keyed execution. Never part of a receipt.
use crate::{AdmitRequest, KeyedAdmitRequest, StoreError};
use crabber_core::{AdmissionKey, AdmissionReceipt, InputFingerprint, Run, RunFence, SessionId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::Duration;

/// Finite, sanitized eligibility failures. Unknown transport/commit responses
/// grant no authority and must be reconciled on the original key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionExecutionError {
    #[error("admission execution is unsupported")]
    Unsupported,
    #[error("admission execution evidence is missing")]
    MissingEvidence,
    #[error("admission owner has a live lease")]
    LiveLease,
    #[error("admission owner changed")]
    StaleOwner,
    #[error("admission execution already started")]
    AlreadyStarted,
    #[error("admission is terminal")]
    AlreadyTerminal,
    #[error("admission semantics conflict")]
    SemanticConflict,
    #[error("admission store outcome is unknown")]
    UnknownStoreFailure,
}

/// Session-owned wire request; excludes credentials and transient trace identity.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionRequestData {
    pub session_id: SessionId,
    pub workspace_id: String,
    pub directory: String,
    pub title: String,
    pub text: String,
    pub provider_id: String,
    pub model_id: String,
    pub system_prompt: Option<String>,
    pub max_output_tokens: Option<u32>,
}

/// Version 1 runtime semantics are the existing admission.v1 ordered array:
/// domain, provider, model, system, output cap, execution mode, compaction ratio/tail,
/// turn limit, tools, prompts, restrictions, components, guards, providers.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct AdmissionExecutionCapsule {
    pub version: u32,
    pub request: AdmissionRequestData,
    pub runtime_semantics: Value,
    pub config_hash: String,
    pub plan_fingerprint: String,
    pub fingerprint: InputFingerprint,
    pub behavior_fingerprint: InputFingerprint,
    pub semantic_digest: InputFingerprint,
}

macro_rules! redacted_debug {
    ($($ty:ty),* $(,)?) => {$(impl std::fmt::Debug for $ty {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(concat!(stringify!($ty), "([redacted])"))
        }
    })*};
}
redacted_debug!(
    AdmissionRequestData,
    AdmissionExecutionCapsule,
    AdmissionExecutionRecord,
    ClaimUnstartedAdmissionRequest,
    ClaimedAdmission
);

/// Shared canonicalization preserves the existing runtime fingerprint domain.
#[must_use]
pub fn admission_config_hash(value: &Value) -> String {
    let mut canonical = value.clone();
    canonical.sort_all_objects();
    format!("{:x}", Sha256::digest(canonical.to_string().as_bytes()))
}

impl AdmissionExecutionCapsule {
    /// Validate before replay as well as fresh insertion. No partial writes.
    /// # Errors
    /// Returns a sanitized conflict for inconsistent or unsupported evidence.
    pub fn validate(&self, keyed: &KeyedAdmitRequest) -> Result<(), StoreError> {
        use crabber_core::{ContentBlock, PartKind, Role};
        let r: &AdmitRequest = &keyed.request;
        let data = &self.request;
        let semantics = self.runtime_semantics.as_array();
        let text_matches = r.user_message.role == Role::User
            && r.user_message.parent_id.is_none()
            && r.user_message.parts.len() == 1
            && r.user_message.parts[0].ordinal == 0
            && r.user_message.parts[0].kind == PartKind::UserInputText
            && r.user_message.parts[0].content
                == ContentBlock::Text {
                    text: data.text.clone(),
                };
        if self.version != 1
            || r.session_id.as_ref() != Some(&data.session_id)
            || r.user_message.session_id != data.session_id
            || r.workspace_id != data.workspace_id
            || r.directory != data.directory
            || r.title != data.title
            || !text_matches
            || self.config_hash != r.config_hash
            || self.plan_fingerprint != r.plan_fingerprint
            || self.fingerprint != keyed.options.fingerprint
            || self.behavior_fingerprint != keyed.options.behavior_fingerprint
            || self.semantic_digest != keyed.semantic_digest()?
            || admission_config_hash(&self.runtime_semantics) != self.config_hash
            || !semantics.is_some_and(|s| {
                s.len() == 15
                    && s[0] == "crabber.runtime.admission.v1"
                    && s[1] == data.provider_id
                    && s[2] == data.model_id
                    && s[3] == serde_json::json!(data.system_prompt)
                    && s[4] == serde_json::json!(data.max_output_tokens)
            })
        {
            return Err(StoreError::AdmissionConflict);
        }
        Ok(())
    }
    /// Digest binds the entire immutable capsule at claim time.
    /// # Errors
    /// Returns a sanitized error on serialization failure.
    pub fn digest(&self) -> Result<String, AdmissionExecutionError> {
        serde_json::to_value(self)
            .map(|v| admission_config_hash(&v))
            .map_err(|_| AdmissionExecutionError::SemanticConflict)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionExecutionState {
    Unstarted,
    Started,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AdmissionExecutionRecord {
    pub receipt: AdmissionReceipt,
    pub key: AdmissionKey,
    pub capsule: AdmissionExecutionCapsule,
    pub state: AdmissionExecutionState,
}

#[derive(Clone)]
pub struct ClaimUnstartedAdmissionRequest {
    pub session_id: SessionId,
    pub key: AdmissionKey,
    pub expected_fence: RunFence,
    pub expected_owner: String,
    pub fingerprint: InputFingerprint,
    pub behavior_fingerprint: InputFingerprint,
    pub semantic_digest: InputFingerprint,
    pub capsule_digest: String,
    pub owner: String,
    pub lease: Duration,
}

#[derive(Clone)]
pub struct ClaimedAdmission {
    pub record: AdmissionExecutionRecord,
    pub run: Run,
    pub fence: RunFence,
}

impl AdmissionExecutionRecord {
    /// Validate retained capsule, run and original user-message bindings.
    /// # Errors
    /// Returns sanitized `SemanticConflict` for inconsistent evidence.
    pub fn validate_binding(
        &self,
        run: &Run,
        user: &crabber_core::Message,
    ) -> Result<(), AdmissionExecutionError> {
        if self.receipt.run_id != run.id
            || self.receipt.session_id != run.session_id
            || self.receipt.user_message_id != user.id
            || user.run_id.as_ref() != Some(&run.id)
            || self.receipt.semantic_digest_version != 1
            || self.receipt.fingerprint != self.capsule.fingerprint
            || self.receipt.semantic_digest != self.capsule.semantic_digest
            || run.config_hash != self.capsule.config_hash
            || run.plan_fingerprint != self.capsule.plan_fingerprint
        {
            return Err(AdmissionExecutionError::SemanticConflict);
        }
        let data = &self.capsule.request;
        let mut user = user.clone();
        user.run_id = None;
        let keyed = KeyedAdmitRequest {
            request: AdmitRequest {
                session_id: Some(run.session_id.clone()),
                workspace_id: data.workspace_id.clone(),
                directory: data.directory.clone(),
                title: data.title.clone(),
                user_message: user,
                config_hash: run.config_hash.clone(),
                plan_fingerprint: run.plan_fingerprint.clone(),
                owner: run.owner.clone(),
                lease: Duration::from_secs(30),
            },
            options: crabber_core::AdmissionOptions {
                key: self.key.clone(),
                fingerprint: self.capsule.fingerprint.clone(),
                behavior_fingerprint: self.capsule.behavior_fingerprint.clone(),
            },
            execution: None,
        };
        self.capsule
            .validate(&keyed)
            .map_err(|_| AdmissionExecutionError::SemanticConflict)
    }
    /// Validate immutable claim semantics and observed ownership/state. Stores
    /// must call this under their ownership lock and additionally check expiry.
    /// # Errors
    /// Returns a finite semantic, ownership or state denial.
    pub fn verify_claim(
        &self,
        claim: &ClaimUnstartedAdmissionRequest,
        run: &Run,
    ) -> Result<(), AdmissionExecutionError> {
        if self.receipt.session_id != claim.session_id
            || self.key != claim.key
            || self.receipt.run_id != run.id
            || run.session_id != claim.session_id
            || self.capsule.request.session_id != claim.session_id
            || self.receipt.fingerprint != claim.fingerprint
            || self.capsule.fingerprint != claim.fingerprint
            || self.capsule.behavior_fingerprint != claim.behavior_fingerprint
            || self.receipt.semantic_digest != claim.semantic_digest
            || self.capsule.semantic_digest != claim.semantic_digest
            || self.capsule.digest()? != claim.capsule_digest
        {
            return Err(AdmissionExecutionError::SemanticConflict);
        }
        if run.claim_token != claim.expected_fence.claim_token
            || run.id != claim.expected_fence.run_id
            || run.owner != claim.expected_owner
        {
            return Err(AdmissionExecutionError::StaleOwner);
        }
        if run.status.is_terminal() {
            return Err(AdmissionExecutionError::AlreadyTerminal);
        }
        if self.state != AdmissionExecutionState::Unstarted {
            return Err(AdmissionExecutionError::AlreadyStarted);
        }
        Ok(())
    }
}
