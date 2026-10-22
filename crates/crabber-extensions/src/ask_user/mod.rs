//! Durable, host-mediated multiple-choice interaction.
//!
//! Supplies no UI. Hosts inspect, answer and continue the persisted interaction
//! using Crabber's continuation API. Expiry requires an explicit host operation.
mod input;
mod schema;

use async_trait::async_trait;
use crabber::{
    core::{
        AcceptedToolResponse, AuthorityRequirement, InputFingerprint, InteractionDescription,
        InteractionKind, InteractionState, ResumableToolIdentity, ToolCheckpoint, ToolInfo,
        ToolOutput, ToolPhaseOutcome, ToolSuspension,
    },
    extension::{Extension, ExtensionError, Registrar, ResumableTool, ToolContext, ToolDefinition},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

/// Model-visible tool name. Do not register another input tool under this name.
pub const TOOL_NAME: &str = "ask_user";
/// Permission metadata; the host decides policy before the tool starts.
pub const PERMISSION_ASK: &str = "interaction.ask";
/// Automatic free-form choice, additional to the model's options.
pub const CUSTOM_OPTION_LABEL: &str = "Other (write your own answer)";
/// Smallest model-supplied choice count.
pub const MIN_OPTIONS: usize = 2;
/// Largest model-supplied choice count.
pub const MAX_OPTIONS: usize = 5;
/// Ordinary result statuses. Expiry never fabricates an answer.
pub const STATUSES: &[&str] = &["selected", "custom", "dismissed", "timed_out"];
const SCHEMA_ID: &str = "crabber-extensions/ask-user";
const SCHEMA_VERSION: &str = "1";
// Worst-case sixfold JSON escaping plus {"custom":""} fits Store's 64 KiB value cap.
const MAX_CUSTOM_ANSWER_BYTES: usize = 10 * 1024;

/// Finite bounds for durable questions and responses.
#[derive(Clone, Serialize)]
pub struct Limits {
    /// UTF-8 bytes, at most 8192.
    pub max_question_bytes: usize,
    /// UTF-8 bytes, at most 512.
    pub max_option_label_bytes: usize,
    /// UTF-8 bytes, at most 1024.
    pub max_option_description_bytes: usize,
    /// UTF-8 bytes, at most 10240 (including worst-case Store JSON escaping).
    pub max_custom_answer_bytes: usize,
    /// Declared expiry, from one second through 24 hours. Hosts submit Expire.
    pub max_wait: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_question_bytes: 4096,
            max_option_label_bytes: 256,
            max_option_description_bytes: 512,
            max_custom_answer_bytes: 4096,
            max_wait: Duration::from_secs(600),
        }
    }
}
/// One normalized, model-authored choice.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AskOption {
    pub label: String,
    pub description: Option<String>,
}
/// Host authority and finite bounds, sealed into the continuation identity.
pub struct Options {
    pub authority: AuthorityRequirement,
    pub limits: Limits,
}
/// Stateless shared extension implementing an explicit durable state machine.
pub struct AskUser {
    tool: Arc<Tool>,
    hash: String,
}
struct Tool {
    authority: AuthorityRequirement,
    limits: Limits,
    identity: ResumableToolIdentity,
}
impl AskUser {
    /// Validate bounds without creating an interaction or executing host work.
    pub fn new(options: Options) -> Result<Self, ExtensionError> {
        let l = &options.limits;
        if l.max_question_bytes == 0
            || l.max_question_bytes > 8192
            || l.max_option_label_bytes == 0
            || l.max_option_label_bytes > 512
            || l.max_option_description_bytes == 0
            || l.max_option_description_bytes > 1024
            || l.max_custom_answer_bytes == 0
            || l.max_custom_answer_bytes > MAX_CUSTOM_ANSWER_BYTES
            || l.max_wait < Duration::from_secs(1)
            || l.max_wait > Duration::from_secs(86400)
            || 2 * MAX_OPTIONS * (l.max_option_label_bytes + l.max_option_description_bytes) + 1024
                > 16384
        {
            return Err(crate::config_error("ask-user-limits"));
        }
        let hash = crate::config_hash(&(
            "ask-user-v2",
            TOOL_NAME,
            PERMISSION_ASK,
            CUSTOM_OPTION_LABEL,
            options.authority.policy_id(),
            options.authority.policy_version(),
            l,
        ));
        let identity = ResumableToolIdentity {
            component_id: SCHEMA_ID.into(),
            component_version: "2".into(),
            config_fingerprint: InputFingerprint::new(hash.clone())
                .map_err(|_| crate::config_error("ask-user-identity"))?,
            checkpoint_schema_id: SCHEMA_ID.into(),
            checkpoint_schema_version: SCHEMA_VERSION.into(),
        };
        Ok(Self {
            tool: Arc::new(Tool {
                authority: options.authority,
                limits: options.limits,
                identity,
            }),
            hash,
        })
    }
}
fn failure(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("ask user failed: {code}"))
}
fn invalid(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("ask user input invalid: {code}"))
}
#[async_trait]
impl ResumableTool for Tool {
    fn identity(&self) -> ResumableToolIdentity {
        self.identity.clone()
    }
    async fn start(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        if context.cancel.is_cancelled() {
            return Err(failure("cancelled"));
        }
        let (question, options) = input::normalize(arguments, &self.limits)?;
        let response_schema = schema::response(&options, self.limits.max_custom_answer_bytes);
        if serde_json::to_vec(&response_schema)
            .map_err(|_| invalid("options"))?
            .len()
            > 16384
        {
            return Err(invalid("options"));
        }
        let checkpoint = ToolCheckpoint::new(
            SCHEMA_ID,
            SCHEMA_VERSION,
            json!({"question": question, "options": options}),
        )
        .map_err(|_| failure("checkpoint"))?;
        let description = InteractionDescription::new(
            "answer",
            InteractionKind::UserInput,
            question,
            response_schema,
            self.authority.clone(),
            Some(self.limits.max_wait.as_millis() as u64),
        )
        .map_err(|_| invalid("options"))?;
        Ok(ToolPhaseOutcome::Suspended(
            ToolSuspension::new(checkpoint, vec![description])
                .map_err(|_| failure("checkpoint"))?,
        ))
    }
    async fn resume(
        &self,
        context: ToolContext,
        _: Value,
        checkpoint: ToolCheckpoint,
        responses: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        if context.cancel.is_cancelled() {
            return Err(failure("cancelled"));
        }
        if !self.identity.accepts(&checkpoint) {
            return Err(failure("checkpoint"));
        }
        let (_, options) = input::normalize(checkpoint.value().clone(), &self.limits)
            .map_err(|_| failure("checkpoint"))?;
        let [response] = responses.as_slice() else {
            return Err(failure("answer"));
        };
        if response.name != "answer" {
            return Err(failure("answer"));
        }
        let result = match response.state {
            InteractionState::Answered => {
                schema::answer(response.value.as_ref(), &options, &self.limits)?
            }
            InteractionState::Denied | InteractionState::Cancelled if response.value.is_none() => {
                json!({"status":"dismissed"})
            }
            InteractionState::Expired if response.value.is_none() => json!({"status":"timed_out"}),
            _ => return Err(failure("answer")),
        };
        Ok(ToolPhaseOutcome::Completed(
            ToolOutput::from_json(&result).map_err(|_| failure("answer"))?,
        ))
    }
}
#[async_trait]
impl Extension for AskUser {
    fn id(&self) -> &str {
        SCHEMA_ID
    }
    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    fn config_hash(&self) -> String {
        self.hash.clone()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(Arc::new(ToolDefinition::resumable(ToolInfo {
            name: TOOL_NAME.into(),
            description: "Ask one bounded multiple-choice question with an automatic free-form choice. Suspends for host input; results are selected, custom, dismissed, or timed_out.".into(),
            parameters: json!({"type":"object","additionalProperties":false,"required":["question","options"],"properties":{"question":{"type":"string"},"options":{"type":"array","minItems":2,"maxItems":5,"items":{"type":"object","additionalProperties":false,"required":["label"],"properties":{"label":{"type":"string"},"description":{"type":"string"}}}}}}),
            retry_safe: false, required_permissions: vec![PERMISSION_ASK.into()],
        }, self.tool.clone())?));
        Ok(())
    }
}
