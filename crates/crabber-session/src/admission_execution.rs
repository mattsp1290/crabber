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
    #[serde(
        default,
        skip_serializing_if = "crate::AdmissionSelectionPolicy::is_explicit"
    )]
    pub selection_policy: crate::AdmissionSelectionPolicy,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_envelope: Option<crabber_core::InputEnvelope>,
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
        r.validate_messages()?;
        let data = &self.request;
        data.selection_policy
            .verify_components(&self.runtime_semantics)?;
        let semantics = self.runtime_semantics.as_array();
        if let Some(input) = &self.input_envelope
            && !self.request.text.is_empty()
            && input.legacy_text() != Some(self.request.text.as_str())
        {
            return Err(StoreError::AdmissionConflict);
        }

        let text_matches = r.user_message.role == Role::User
            && r.user_message.parent_id.is_none()
            && r.user_message.parts.len() == 1
            && r.user_message.parts[0].ordinal == 0
            && r.user_message.parts[0].kind == PartKind::UserInputText
            && r.user_message.parts[0].content
                == ContentBlock::Text {
                    text: data.text.clone(),
                };
        let typed = self.version == 2 && self.input_envelope.is_some();
        if !matches!(
            (self.version, self.input_envelope.is_some()),
            (1, false) | (2, true)
        ) || self.input_envelope != r.input_envelope
            || r.session_id.as_ref() != Some(&data.session_id)
            || r.user_message.session_id != data.session_id
            || r.workspace_id != data.workspace_id
            || r.directory != data.directory
            || r.title != data.title
            || (!typed && (!text_matches || !r.additional_messages.is_empty()))
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
    /// Encode bounded private evidence as lossless JSON text.
    /// # Errors
    /// Rejects oversized or malformed evidence without echoing input.
    pub fn encode(&self) -> Result<String, StoreError> {
        crate::execution_binding::encode_bounded(self)
    }
    /// Decode private evidence; authoritative row validation remains required.
    /// # Errors
    /// Rejects oversized or malformed evidence.
    pub fn decode(encoded: &str) -> Result<Self, StoreError> {
        if encoded.len() > 16 * 1024 * 1024 {
            return Err(StoreError::AdmissionConflict);
        }
        serde_json::from_str(encoded).map_err(|_| StoreError::AdmissionConflict)
    }

    /// Validate retained capsule, run and original user-message bindings.
    /// # Errors
    /// Returns sanitized `SemanticConflict` for inconsistent evidence.
    pub fn validate_binding(
        &self,
        run: &Run,
        user: &crabber_core::Message,
    ) -> Result<(), AdmissionExecutionError> {
        self.validate_messages(run, std::slice::from_ref(user))
    }
    /// Validate original keyed input without treating replay as execution authority.
    /// # Errors
    /// Rejects a missing/altered message or inconsistent receipt/capsule.
    pub fn validate_messages(
        &self,
        run: &Run,
        messages: &[crabber_core::Message],
    ) -> Result<(), AdmissionExecutionError> {
        self.validate_messages_scope(
            &run.session_id,
            &run.id,
            &run.config_hash,
            &run.plan_fingerprint,
            &run.owner,
            messages,
        )
    }
    /// Authenticate retained input against actual narrow Run facts. This read
    /// grants no execution authority and never materializes a private checkpoint.
    pub(crate) fn validate_messages_scope(
        &self,
        session: &SessionId,
        run_id: &crabber_core::RunId,
        config: &str,
        plan: &str,
        owner: &str,
        messages: &[crabber_core::Message],
    ) -> Result<(), AdmissionExecutionError> {
        if let Some(input) = &self.capsule.input_envelope {
            input
                .validate_message_projection(session, Some(run_id), messages)
                .map_err(|_| AdmissionExecutionError::SemanticConflict)?;
        } else if messages.len() != 1 {
            return Err(AdmissionExecutionError::SemanticConflict);
        }
        let user = messages
            .first()
            .ok_or(AdmissionExecutionError::SemanticConflict)?;
        if self.receipt.run_id != *run_id
            || self.receipt.session_id != *session
            || self.receipt.user_message_id != user.id
            || user.run_id.as_ref() != Some(run_id)
            || self.receipt.semantic_digest_version
                != if self.capsule.input_envelope.is_some() {
                    2
                } else {
                    1
                }
            || self.receipt.fingerprint != self.capsule.fingerprint
            || self.receipt.semantic_digest != self.capsule.semantic_digest
            || config != self.capsule.config_hash
            || plan != self.capsule.plan_fingerprint
        {
            return Err(AdmissionExecutionError::SemanticConflict);
        }
        let data = &self.capsule.request;
        let mut user = user.clone();
        user.run_id = None;
        let additional_messages = messages
            .iter()
            .skip(1)
            .cloned()
            .map(|mut record| {
                record.run_id = None;
                record
            })
            .collect();
        let keyed = KeyedAdmitRequest {
            request: AdmitRequest {
                input_envelope: self.capsule.input_envelope.clone(),
                additional_messages,
                expected_revision: self
                    .capsule
                    .input_envelope
                    .as_ref()
                    .map(crabber_core::InputEnvelope::expected_revision),
                session_id: Some(session.clone()),
                workspace_id: data.workspace_id.clone(),
                directory: data.directory.clone(),
                title: data.title.clone(),
                user_message: user,
                config_hash: config.to_owned(),
                plan_fingerprint: plan.to_owned(),
                owner: owner.to_owned(),
                lease: Duration::from_secs(30),
            },
            options: crabber_core::AdmissionOptions {
                key: self.key.clone(),
                fingerprint: self.capsule.fingerprint.clone(),
                behavior_fingerprint: self.capsule.behavior_fingerprint.clone(),
            },
            execution: Some(self.capsule.clone()),
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
