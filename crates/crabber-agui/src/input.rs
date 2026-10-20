//! Bounded AG-UI ingress normalization without host or execution authority.
mod content;
mod messages;

use ag_ui_core::{
    processing::{Diagnostic, DiagnosticCategory, normalize_run_input},
    types::{Message, ResumeEntry, RunAgentInput},
};
use crabber_core::{
    AdmissionKind, FrontendTool, FrontendToolResult, InputContext, InputEnvelope, InputState,
    PublicActivity, SessionRevision,
    input::{MAX_INPUT_BYTES, MAX_INPUT_JSON_DEPTH, MAX_INPUT_MESSAGES},
};
use serde_json::{Value, json};
use std::collections::BTreeSet;

/// Finite ingress classifications. Payloads and SDK diagnostic strings are not
/// included in errors. Unstable until durable-host consumer qualification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WireInputError {
    #[error("malformed AG-UI input")]
    Malformed,
    #[error("AG-UI input limit exceeded")]
    Limit,
    #[error("AG-UI input has no supported runtime representation")]
    Unsupported,
}

/// Normalized wire data, separate from host-approved runtime input.
///
/// Wire thread/run/parent IDs are transport correlations. This object supplies
/// no workspace, selected model, session identity, expected revision, resolver
/// identity or continuation authority. The host must validate those separately.
/// Opaque wire metadata is retained in `wire()` and never implicitly becomes
/// model input. Unstable until durable-host consumer qualification.
pub struct NormalizedRunInput {
    wire: RunAgentInput,
    state: Value,
    diagnostics: Vec<Diagnostic>,
}
impl std::fmt::Debug for NormalizedRunInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NormalizedRunInput([redacted])")
    }
}

/// Normalize an already parsed value with the pinned SDK's ingress primitive.
/// The transport must bound its read buffer and parser before constructing it.
/// Whole input-state null/absence becomes `{}`; nested null stays intact.
///
/// # Errors
/// Rejects oversized/deep values before SDK copying, malformed known protocol
/// fields, invalid wire identities or duplicate resume identities.
pub fn convert_run_input(value: Value) -> Result<NormalizedRunInput, WireInputError> {
    check_depth(&value, 0)?;
    crate::sse::json_bytes(&value, MAX_INPUT_BYTES).map_err(|error| match error {
        crate::ProjectionError::Limit => WireInputError::Limit,
        _ => WireInputError::Malformed,
    })?;
    let processed = normalize_run_input(value).map_err(|_| WireInputError::Malformed)?;
    identity(processed.input.thread_id.as_str())?;
    identity(processed.input.run_id.as_str())?;
    if let Some(parent) = &processed.input.parent_run_id {
        identity(parent.as_str())?;
    }
    if processed.input.messages.len() > MAX_INPUT_MESSAGES
        || processed
            .input
            .tools
            .as_ref()
            .is_some_and(|tools| tools.len() > 128)
        || processed
            .input
            .context
            .as_ref()
            .is_some_and(|context| context.len() > 128)
    {
        return Err(WireInputError::Limit);
    }
    let mut message_ids = BTreeSet::new();
    for message in &processed.input.messages {
        identity(message.id().as_str())?;
        if !message_ids.insert(message.id().as_str()) {
            return Err(WireInputError::Malformed);
        }
    }
    let mut resumes = BTreeSet::new();
    let entries = processed.input.resume.as_deref().unwrap_or_default();
    if entries.len() > 128 {
        return Err(WireInputError::Limit);
    }
    for entry in entries {
        identity(&entry.interrupt_id)?;
        if !resumes.insert(&entry.interrupt_id) {
            return Err(WireInputError::Malformed);
        }
    }
    Ok(NormalizedRunInput {
        wire: processed.input,
        state: processed.effective_state,
        diagnostics: processed.diagnostics,
    })
}

impl NormalizedRunInput {
    fn require_complete_content(&self) -> Result<(), WireInputError> {
        if self.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.category,
                DiagnosticCategory::UnknownVariant | DiagnosticCategory::UnknownEnumValue
            )
        }) {
            Err(WireInputError::Unsupported)
        } else {
            Ok(())
        }
    }
    /// Canonical SDK data including transport aliases, forwarded properties,
    /// metadata and typed resume entries. These values grant no authority.
    #[must_use]
    pub fn wire(&self) -> &RunAgentInput {
        &self.wire
    }
    /// SDK normalization diagnostics; callers may expose bounded categories and
    /// paths under their own policy rather than silently ignoring warnings.
    #[must_use]
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }
    /// Effective input state. Output-state null is unrelated and stays null.
    #[must_use]
    pub fn effective_state(&self) -> &Value {
        &self.state
    }
    /// Untrusted typed response data. A host correlates these IDs against the
    /// authoritative pending set and records its own resolver/authority claim.
    #[must_use]
    pub fn resume_entries(&self) -> &[ResumeEntry] {
        self.wire.resume.as_deref().unwrap_or_default()
    }
    /// UI activity remains separate from provider messages. It is not persisted
    /// or authorized by conversion; only a bound runtime writer may publish it.
    /// # Errors
    /// Rejects invalid, duplicate or oversized activity identities/content.
    pub fn activities(&self) -> Result<Vec<PublicActivity>, WireInputError> {
        self.require_complete_content()?;
        let mut ids = BTreeSet::new();
        self.wire
            .messages
            .iter()
            .filter_map(|message| match message {
                Message::Activity {
                    id,
                    activity_type,
                    content,
                    subagent_run_id,
                    ..
                } => Some((id, activity_type, content, subagent_run_id)),
                _ => None,
            })
            .map(|(id, activity_type, content, subagent_run_id)| {
                if subagent_run_id.is_some() {
                    return Err(WireInputError::Unsupported);
                }
                if !ids.insert(id.as_str()) {
                    return Err(WireInputError::Malformed);
                }
                PublicActivity::new(id.as_str(), activity_type, Value::Object(content.clone()))
                    .map_err(|_| WireInputError::Malformed)
            })
            .collect()
    }
    /// Convert supported model content using the host's captured revision and
    /// selected admission kind. Client roles/results still require host checks.
    ///
    /// Activity is deliberately excluded. Resume entries and forwarded props
    /// stay available separately; they are not conversation text or authority.
    /// Developer/reasoning history, named messages, encrypted continuity and
    /// subagent attribution need an explicit host representation and are refused.
    /// # Errors
    /// Returns finite validation/unsupported failures without private input.
    /// Unknown variants/enums dropped by SDK normalization are refused; a host
    /// must explicitly approve warnings and construct any lossy input itself.
    pub fn envelope(
        &self,
        expected_revision: SessionRevision,
        admission: AdmissionKind,
    ) -> Result<InputEnvelope, WireInputError> {
        if !self.resume_entries().is_empty() && matches!(admission, AdmissionKind::Ordinary) {
            return Err(WireInputError::Malformed);
        }
        self.activities()?;
        let messages = self
            .wire
            .messages
            .iter()
            .filter(|message| !matches!(message, Message::Activity { .. }))
            .map(messages::message)
            .collect::<Result<Vec<_>, _>>()?;
        let tools = self
            .wire
            .tools
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|tool| FrontendTool {
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_parameters: tool.parameters.clone().unwrap_or_else(|| json!({})),
                // A client tool's parameter schema is never an output schema.
                result_contract: None,
            })
            .collect();
        let context = self
            .wire
            .context
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|entry| InputContext {
                description: entry.description.clone(),
                value: Value::String(entry.value.clone()),
            })
            .collect();
        InputEnvelope::new(
            messages,
            tools,
            context,
            InputState::Present {
                value: self.state.clone(),
            },
            expected_revision,
            admission,
        )
        .map_err(|_| WireInputError::Malformed)
    }
    /// Convert only tool-result messages selected by the host's authoritative
    /// pending set. This produces response data, never continuation permission.
    /// SDK result-message IDs remain in `wire()`/`envelope()` for host canonical
    /// history validation; the response is bound by its native call ID. Result
    /// part IDs have no native representation and are refused.
    /// # Errors
    /// Rejects a missing/duplicate selected result or unsupported typed content.
    pub fn frontend_results(
        &self,
        pending: &[crabber_core::ToolCallId],
    ) -> Result<Vec<FrontendToolResult>, WireInputError> {
        self.require_complete_content()?;
        if pending.len() > 128 {
            return Err(WireInputError::Limit);
        }
        let mut seen = BTreeSet::new();
        pending.iter().map(|call| {
            identity(&call.0)?;
            if !seen.insert(call) {
                return Err(WireInputError::Malformed);
            }
            let mut results = self.wire.messages.iter().filter(|message| {
                matches!(message, Message::Tool { tool_call_id, .. } if tool_call_id.as_str() == call.0)
            });
            let result = results.next().ok_or(WireInputError::Malformed)?;
            if results.next().is_some() {
                return Err(WireInputError::Malformed);
            }
            messages::frontend_result(result)
        }).collect()
    }
}

fn identity(text: &str) -> Result<(), WireInputError> {
    if text.is_empty() || text.len() > 512 || text.chars().any(char::is_control) {
        Err(WireInputError::Malformed)
    } else {
        Ok(())
    }
}
fn check_depth(value: &Value, depth: usize) -> Result<(), WireInputError> {
    if depth > MAX_INPUT_JSON_DEPTH {
        return Err(WireInputError::Limit);
    }
    match value {
        Value::Array(values) => values
            .iter()
            .try_for_each(|value| check_depth(value, depth + 1)),
        Value::Object(values) => values
            .values()
            .try_for_each(|value| check_depth(value, depth + 1)),
        _ => Ok(()),
    }
}
