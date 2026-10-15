//! Validated semantic input, separate from raw persisted messages and authority.
//! Unstable until the durable-host contract is qualified.
mod validation;
pub(crate) use validation::{
    bounded as validate_serialized_bound, content as validate_content, json as validate_json,
};

use crate::{
    ContinuationGeneration, CoreError, InputFingerprint, ProviderBindingDescriptor, Role, RunId,
    SessionContext, SessionRevision, ToolCallId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Serialized envelope limit; ingress must also bound its parser/read buffer.
pub const MAX_INPUT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_INPUT_MESSAGES: usize = 64;
pub const MAX_INPUT_PARTS: usize = 256;
pub const MAX_INPUT_JSON_DEPTH: usize = 32;
pub const MAX_INLINE_MEDIA_BYTES: usize = 1024 * 1024;

/// Explicit media category. A URL/file is never automatically fetched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Image,
    Audio,
    Video,
    Document,
}

/// Reference interpretation is explicit; strings are never guessed to be URLs
/// or inline data. Providers must reject representations they cannot preserve.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MediaSource {
    Data { base64: String },
    Url { url: String },
    File { file_id: String },
}

/// Ordered model/result content without tool-call or control authority.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessageContent {
    Text {
        text: String,
    },
    Media {
        kind: MediaKind,
        mime_type: String,
        source: MediaSource,
    },
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputContent {
    Content {
        content: MessageContent,
    },
    ToolCall {
        call_id: ToolCallId,
        name: String,
        arguments: Value,
    },
    ToolResult {
        call_id: ToolCallId,
        content: Vec<MessageContent>,
        is_error: bool,
    },
}

/// Supplied IDs are semantic provenance. None means a later internal ID may be
/// generated; that generated ID never changes this envelope's fingerprint.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputPart {
    pub id: Option<String>,
    pub content: InputContent,
}

/// Roles are host-authorized. Constructing this type does not authorize a
/// client's privileged history, tool results, workspace or selected model.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputMessage {
    pub id: Option<String>,
    pub parent_id: Option<String>,
    pub role: Role,
    pub parts: Vec<InputPart>,
}

/// Frontend-only tool declaration. It never grants a backend executor.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontendTool {
    pub name: String,
    pub description: String,
    pub input_parameters: Value,
    /// Optional, separately named output contract. Input parameters are never
    /// used to validate returned frontend results.
    pub result_contract: Option<Value>,
}

/// Data contributing to model input, separate from UI activity events.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputContext {
    pub description: String,
    pub value: Value,
}

/// Preserve explicit JSON null independently of absence.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputState {
    Absent,
    Present { value: Value },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdmissionKind {
    Ordinary,
    Continuation {
        run_id: RunId,
        generation: ContinuationGeneration,
    },
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEnvelope {
    version: u32,
    messages: Vec<InputMessage>,
    frontend_tools: Vec<FrontendTool>,
    context: Vec<InputContext>,
    state: InputState,
    expected_revision: SessionRevision,
    admission: AdmissionKind,
}

/// Bounded immutable semantic input. Unstable until durable-host qualification.
/// Deserialization performs the same validation as construction. Runtime still
/// must verify host authority, schemas, pending results, revision and binding.
#[derive(Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct InputEnvelope(WireEnvelope);
impl InputEnvelope {
    /// Validate input without assigning internal persistence identities.
    /// # Errors
    /// Returns fixed validation errors without echoing input content.
    pub fn new(
        messages: Vec<InputMessage>,
        frontend_tools: Vec<FrontendTool>,
        context: Vec<InputContext>,
        state: InputState,
        expected_revision: SessionRevision,
        admission: AdmissionKind,
    ) -> Result<Self, CoreError> {
        WireEnvelope {
            version: 1,
            messages,
            frontend_tools,
            context,
            state,
            expected_revision,
            admission,
        }
        .try_into()
    }
    #[must_use]
    pub fn messages(&self) -> &[InputMessage] {
        &self.0.messages
    }
    #[must_use]
    pub fn frontend_tools(&self) -> &[FrontendTool] {
        &self.0.frontend_tools
    }
    #[must_use]
    pub fn context(&self) -> &[InputContext] {
        &self.0.context
    }
    #[must_use]
    pub fn state(&self) -> &InputState {
        &self.0.state
    }
    #[must_use]
    pub fn expected_revision(&self) -> SessionRevision {
        self.0.expected_revision
    }
    #[must_use]
    pub fn admission_kind(&self) -> &AdmissionKind {
        &self.0.admission
    }

    /// Canonical v2 input semantics: ordered messages/parts and supplied IDs,
    /// explicit null/absence, frontend schema/origin, continuation generation,
    /// revision, exact context, frozen binding and declared runtime/host behavior.
    /// Generated persistence IDs, credentials and transport correlation are absent.
    /// # Errors
    /// Rejects unbounded routing data or serialization failure.
    pub fn fingerprint(
        &self,
        context: &SessionContext,
        binding: &ProviderBindingDescriptor,
        runtime_config: &InputFingerprint,
        behavior: &InputFingerprint,
    ) -> Result<InputFingerprint, CoreError> {
        if !validation::identity(&context.session_id().0, 512)
            || context.workspace_id().len() > 512
            || context.directory().len() > 16384
            || context.workspace_id().contains('\0')
            || context.directory().contains('\0')
        {
            return Err(validation::invalid());
        }
        let mut semantic = serde_json::to_value((
            "crabber.input.v2",
            &self.0,
            (
                &context.session_id().0,
                context.workspace_id(),
                context.directory(),
            ),
            binding,
            runtime_config,
            behavior,
        ))
        .map_err(|_| validation::invalid())?;
        semantic.sort_all_objects();
        let bytes = serde_json::to_vec(&semantic).map_err(|_| validation::invalid())?;
        InputFingerprint::new(format!("{:x}", Sha256::digest(bytes)))
    }
}
impl TryFrom<WireEnvelope> for InputEnvelope {
    type Error = CoreError;
    fn try_from(value: WireEnvelope) -> Result<Self, Self::Error> {
        validation::envelope(&value)?;
        Ok(Self(value))
    }
}
impl<'de> Deserialize<'de> for InputEnvelope {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let invalid = || serde::de::Error::custom("invalid typed input");
        let wire = WireEnvelope::deserialize(deserializer).map_err(|_| invalid())?;
        wire.try_into().map_err(|_| invalid())
    }
}

macro_rules! redacted_debug {
    ($($ty:ty),* $(,)?) => {$(impl std::fmt::Debug for $ty {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(concat!(stringify!($ty), "([redacted])"))
        }
    })*};
}
redacted_debug!(
    InputEnvelope,
    InputMessage,
    InputPart,
    InputContent,
    MessageContent,
    MediaSource,
    InputState,
    InputContext,
    FrontendTool
);
