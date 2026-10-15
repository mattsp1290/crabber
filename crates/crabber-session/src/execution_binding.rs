//! Immutable provider-binding evidence, retained in the admission transaction.
//! Typed v3 evidence is independent of the keyed execution capsule carrier.
mod codec;
use crate::{
    AdmissionRequestData, AdmitRequest, KeyedAdmitRequest, StoreError, admission_config_hash,
};
pub(crate) use codec::encode_bounded;
use codec::validate_depth;
use crabber_core::{
    ContentBlock, InputFingerprint, Message, MessageId, PartKind, ProviderBindingDescriptor, Role,
    RunId, SessionId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Reserved component in the actual runtime configuration. Its version is the
/// JSON encoding of the complete provider binding descriptor.
pub const PROVIDER_BINDING_COMPONENT: &str = "contract:crabber/provider-binding.v1";
/// Reserved component for the host's opaque policy/callback behavior version.
pub const HOST_BEHAVIOR_COMPONENT: &str = "contract:crabber/host-behavior.v1";
pub(crate) const MAX_BINDING_BYTES: usize = 16 << 20;

/// Private execution evidence, not public receipt/snapshot data or authority.
/// Version 2 identifies this independent binding capsule, not a new input codec.
/// Text input and runtime semantics retain their explicit version-1 contract.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBindingCapsule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_fingerprint: Option<InputFingerprint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_envelope: Option<crabber_core::InputEnvelope>,
    pub version: u32,
    pub provider: ProviderBindingDescriptor,
    pub request: AdmissionRequestData,
    pub runtime_semantics: Value,
    pub config_hash: InputFingerprint,
    pub plan_fingerprint: InputFingerprint,
    pub behavior_fingerprint: InputFingerprint,
}

/// An unkeyed admission that must retain its exact provider binding atomically.
#[derive(Clone)]
pub struct BoundAdmitRequest {
    pub request: AdmitRequest,
    pub binding: ExecutionBindingCapsule,
}
/// A keyed admission that must retain its exact provider binding atomically.
#[derive(Clone)]
pub struct BoundKeyedAdmitRequest {
    pub keyed: KeyedAdmitRequest,
    pub binding: ExecutionBindingCapsule,
}

/// Retained private evidence. Reading it never restores an implementation,
/// grants a fence, or authorizes repeating an effect.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedExecutionBinding {
    pub run_id: RunId,
    pub session_id: SessionId,
    pub user_message_id: MessageId,
    pub capsule: ExecutionBindingCapsule,
}
macro_rules! redacted {
    ($($ty:ty),*) => {$(impl std::fmt::Debug for $ty {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(concat!(stringify!($ty), "([redacted])"))
        }
    })*};
}
redacted!(
    ExecutionBindingCapsule,
    BoundAdmitRequest,
    BoundKeyedAdmitRequest,
    RetainedExecutionBinding
);

impl ExecutionBindingCapsule {
    /// Independently recompute typed input semantics, excluding generated IDs.
    /// # Errors
    /// Rejects invalid scope/configuration; legacy text capsules have no v2 digest.
    pub fn computed_input_fingerprint(&self) -> Result<Option<InputFingerprint>, StoreError> {
        let Some(input) = &self.input_envelope else {
            return Ok(None);
        };
        let data = &self.request;
        let context = crabber_core::SessionContext::new(
            data.session_id.clone(),
            data.workspace_id.clone(),
            data.directory.clone(),
        );
        let runtime = crate::typed_admission::runtime_digest(
            self.config_hash.as_str(),
            self.plan_fingerprint.as_str(),
            &data.title,
        )?;
        input
            .fingerprint(
                &context,
                &self.provider,
                &runtime,
                &self.behavior_fingerprint,
            )
            .map(Some)
    }
    /// Validate all independently inspectable configuration before admission.
    /// No caller hash alone proves the descriptor or original input.
    /// # Errors
    /// Rejects unsupported, unbounded or inconsistent binding evidence.
    pub fn validate(&self, request: &AdmitRequest) -> Result<(), StoreError> {
        self.validate_configuration()?;
        request.validate_messages()?;
        if self.input_envelope != request.input_envelope {
            return Err(StoreError::AdmissionConflict);
        }
        let input_matches = if self.input_envelope.is_some() {
            true
        } else {
            request.additional_messages.is_empty()
                && matches_input(&self.request, &request.user_message)
        };
        if !input_matches
            || request.user_message.session_id != self.request.session_id
            || request
                .session_id
                .as_ref()
                .is_some_and(|id| *id != self.request.session_id)
            || request.user_message.run_id.is_some()
            || request
                .user_message
                .parts
                .iter()
                .any(|p| p.message_id != request.user_message.id)
            || request.workspace_id != self.request.workspace_id
            || request.directory != self.request.directory
            || request.title != self.request.title
            || request.config_hash != self.config_hash.as_str()
            || request.plan_fingerprint != self.plan_fingerprint.as_str()
        {
            return Err(StoreError::AdmissionConflict);
        }
        Ok(())
    }
    /// Verify both original keyed execution evidence and independently bound configuration.
    /// # Errors
    /// Rejects missing or inconsistent original semantics before any write.
    pub fn validate_keyed(&self, keyed: &KeyedAdmitRequest) -> Result<(), StoreError> {
        self.validate(&keyed.request)?;
        let original = keyed
            .execution
            .as_ref()
            .ok_or(StoreError::AdmissionConflict)?;
        keyed.validated_digest()?;
        if original.request != self.request
            || original.runtime_semantics != self.runtime_semantics
            || original.behavior_fingerprint != self.behavior_fingerprint
            || keyed.options.behavior_fingerprint != self.behavior_fingerprint
        {
            return Err(StoreError::AdmissionConflict);
        }
        Ok(())
    }
    fn validate_configuration(&self) -> Result<(), StoreError> {
        self.request
            .selection_policy
            .verify_components(&self.runtime_semantics)?;
        let data = &self.request;
        if let Some(input) = &self.input_envelope
            && !self.request.text.is_empty()
            && input.legacy_text() != Some(self.request.text.as_str())
        {
            return Err(StoreError::AdmissionConflict);
        }

        if data.session_id.0.is_empty()
            || data.session_id.0.len() > 512
            || data.session_id.0.chars().any(char::is_control)
            || data.workspace_id.len() > 512
            || data.directory.len() > 16384
            || data.workspace_id.contains('\0')
            || data.directory.contains('\0')
        {
            return Err(StoreError::AdmissionConflict);
        }
        validate_depth(&self.runtime_semantics, 0)?;
        if self.input_fingerprint != self.computed_input_fingerprint()? {
            return Err(StoreError::AdmissionConflict);
        }

        let bytes = encode_bounded(self)?;
        let values = self
            .runtime_semantics
            .as_array()
            .ok_or(StoreError::AdmissionConflict)?;
        if !matches!(
            (self.version, self.input_envelope.is_some()),
            (2, false) | (3, true)
        ) || values.len() != 15
            || values[0] != "crabber.runtime.admission.v1"
            || values[1] != self.provider.provider_id()
            || values[2] != self.provider.model_id()
            || values[1] != self.request.provider_id
            || values[2] != self.request.model_id
            || values[3] != serde_json::json!(self.request.system_prompt)
            || values[4] != serde_json::json!(self.request.max_output_tokens)
            || self.request.max_output_tokens == Some(0)
            || matches!((self.request.max_output_tokens, self.provider.limits().output_tokens), (Some(requested), Some(limit)) if requested > limit)
            || admission_config_hash(&self.runtime_semantics) != self.config_hash.as_str()
        {
            return Err(StoreError::AdmissionConflict);
        }
        let components = values[12].as_array().ok_or(StoreError::AdmissionConflict)?;
        let mut bindings = components
            .iter()
            .filter(|v| v.get("id").and_then(Value::as_str) == Some(PROVIDER_BINDING_COMPONENT));
        let encoded = bindings
            .next()
            .and_then(|v| v.get("version"))
            .and_then(Value::as_str)
            .ok_or(StoreError::AdmissionConflict)?;
        if bindings.next().is_some()
            || serde_json::from_str::<ProviderBindingDescriptor>(encoded)
                .ok()
                .as_ref()
                != Some(&self.provider)
        {
            return Err(StoreError::AdmissionConflict);
        }
        let mut behavior = components
            .iter()
            .filter(|v| v.get("id").and_then(Value::as_str) == Some(HOST_BEHAVIOR_COMPONENT));
        if behavior
            .next()
            .and_then(|v| v.get("version"))
            .and_then(Value::as_str)
            != Some(self.behavior_fingerprint.as_str())
            || behavior.next().is_some()
        {
            return Err(StoreError::AdmissionConflict);
        }
        // Every accepted capsule must also fit the reader's depth and codec.
        serde_json::from_str::<Self>(&bytes).map_err(|_| StoreError::AdmissionConflict)?;
        Ok(())
    }
}
fn matches_input(data: &AdmissionRequestData, user: &Message) -> bool {
    user.session_id == data.session_id
        && user.role == Role::User
        && user.parent_id.is_none()
        && user.parts.len() == 1
        && user.parts[0].ordinal == 0
        && user.parts[0].kind == PartKind::UserInputText
        && matches!(&user.parts[0].content, ContentBlock::Text { text } if text == &data.text)
}
impl RetainedExecutionBinding {
    /// Construct private storage metadata after validating and admitting the request.
    /// This does not validate the caller's rows or grant execution authority.
    #[must_use]
    pub fn new(
        run: &crabber_core::Run,
        message: &Message,
        capsule: ExecutionBindingCapsule,
    ) -> Self {
        Self {
            run_id: run.id.clone(),
            session_id: run.session_id.clone(),
            user_message_id: message.id.clone(),
            capsule,
        }
    }
    /// Verify scoped immutable run fields and the original committed input.
    /// # Errors
    /// Rejects corrupt or cross-bound storage evidence.
    pub fn validate(
        &self,
        run: &RunId,
        session: &SessionId,
        config: &str,
        plan: &str,
        user: &Message,
    ) -> Result<(), StoreError> {
        self.validate_messages(run, session, config, plan, std::slice::from_ref(user))
    }
    /// Validate all original input messages from one coherent scoped read.
    /// # Errors
    /// Rejects altered provenance, content or incomplete original evidence.
    pub fn validate_messages(
        &self,
        run: &RunId,
        session: &SessionId,
        config: &str,
        plan: &str,
        messages: &[Message],
    ) -> Result<(), StoreError> {
        self.capsule.validate_configuration()?;
        let user = messages.first().ok_or(StoreError::AdmissionConflict)?;
        let input_matches = if let Some(input) = &self.capsule.input_envelope {
            input
                .validate_message_projection(session, Some(run), messages)
                .is_ok()
        } else {
            messages.len() == 1 && matches_input(&self.capsule.request, user)
        };
        if self.run_id != *run
            || self.session_id != *session
            || self.capsule.request.session_id != *session
            || self.capsule.config_hash.as_str() != config
            || self.capsule.plan_fingerprint.as_str() != plan
            || self.user_message_id != user.id
            || user.run_id.as_ref() != Some(run)
            || user.parts.iter().any(|p| p.message_id != user.id)
            || !input_matches
        {
            return Err(StoreError::AdmissionConflict);
        }
        Ok(())
    }
    /// Encode the bounded private storage record, never a public receipt payload.
    /// # Errors
    /// Rejects records that exceed the codec bounds or cannot round-trip.
    pub fn encode(&self) -> Result<String, StoreError> {
        let encoded = encode_bounded(self)?;
        Self::decode(&encoded)?;
        Ok(encoded)
    }
    /// Decode bounded private storage. Call `validate` against authoritative rows.
    /// # Errors
    /// Rejects oversized or malformed records; grants no execution authority.
    pub fn decode(encoded: &str) -> Result<Self, StoreError> {
        if encoded.len() > MAX_BINDING_BYTES {
            return Err(StoreError::AdmissionConflict);
        }
        serde_json::from_str(encoded).map_err(|_| StoreError::AdmissionConflict)
    }
}

#[cfg(test)]
mod tests;
