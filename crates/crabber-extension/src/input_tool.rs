//! A side-effect-free input request built on the native continuation contract.
use crate::{ExtensionError, ResumableTool, ToolContext, ToolDefinition};
use async_trait::async_trait;
use crabber_core::{
    AcceptedToolResponse, AuthorityRequirement, ContentBlock, InputFingerprint,
    InteractionDescription, InteractionKind, InteractionState, ResumableToolIdentity,
    ToolCheckpoint, ToolInfo, ToolOutput, ToolPhaseOutcome, ToolSuspension,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

const COMPONENT: &str = "crabber.input";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Question {
    name: String,
    prompt: String,
    #[serde(default)]
    choices: Option<Vec<String>>,
}
#[derive(Deserialize)]
struct Arguments {
    questions: Vec<Question>,
}
struct InputTool {
    identity: ResumableToolIdentity,
    authority: AuthorityRequirement,
}
fn invalid() -> ExtensionError {
    ExtensionError::Tool("invalid native input request or response".into())
}
impl ToolDefinition {
    /// Request durable user input without keeping a tool callback alive.
    ///
    /// The host declares the required answer policy. Runtime supplies all
    /// interaction IDs; Store validates and records answers before resume.
    /// Questions remain separate from tool results until responses are recorded.
    /// ```
    /// use crabber_extension::ToolDefinition;
    /// use crabber_core::AuthorityRequirement;
    /// let tool = ToolDefinition::input("request_input",
    ///     AuthorityRequirement::new("signed-in-user", "1")?)?;
    /// # let _ = tool;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    /// # Errors
    /// Rejects invalid tool names or behavior identity declarations.
    pub fn input(
        name: impl Into<String>,
        authority: AuthorityRequirement,
    ) -> Result<Self, ExtensionError> {
        let name = name.into();
        if name.is_empty() || name.len() > 128 || name.chars().any(char::is_control) {
            return Err(invalid());
        }
        let config = serde_json::to_vec(&(COMPONENT, &name, &authority)).map_err(|_| invalid())?;
        let identity = ResumableToolIdentity {
            component_id: COMPONENT.into(),
            component_version: "1".into(),
            config_fingerprint: InputFingerprint::new(format!("{:x}", Sha256::digest(config)))
                .map_err(|_| invalid())?,
            checkpoint_schema_id: COMPONENT.into(),
            checkpoint_schema_version: "1".into(),
        };
        Self::resumable(
            ToolInfo {
                name,
                description: "Ask the user questions and wait for their recorded answers.".into(),
                parameters: json!({
                    "type":"object", "additionalProperties":false, "required":["questions"],
                    "properties":{"questions":{"type":"array","minItems":1,"maxItems":16,
                        "items":{"type":"object","additionalProperties":false,
                            "required":["name","prompt"],"properties":{
                                "name":{"type":"string","minLength":1,"maxLength":128},
                                "prompt":{"type":"string","minLength":1,"maxLength":2048},
                                "choices":{"type":"array","minItems":1,"maxItems":16,
                                    "uniqueItems":true,"items":{"type":"string","minLength":1,"maxLength":256}}
                            }}}}
                }),
                retry_safe: false,
                required_permissions: vec![],
            },
            Arc::new(InputTool {
                identity,
                authority,
            }),
        )
    }
}
fn questions(arguments: Value) -> Result<Vec<Question>, ExtensionError> {
    let object = arguments.as_object().ok_or_else(invalid)?;
    if object.len() != 1 {
        return Err(invalid());
    }
    let entries = object
        .get("questions")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?;
    if entries.is_empty() || entries.len() > 16 {
        return Err(invalid());
    }
    let mut bytes = 0;
    for entry in entries {
        let entry = entry.as_object().ok_or_else(invalid)?;
        if entry
            .keys()
            .any(|key| !matches!(key.as_str(), "name" | "prompt" | "choices"))
        {
            return Err(invalid());
        }
        for (key, limit) in [("name", 128), ("prompt", 2048)] {
            let text = entry.get(key).and_then(Value::as_str).ok_or_else(invalid)?;
            if text.is_empty() || text.len() > limit {
                return Err(invalid());
            }
            bytes += text.len();
        }
        if let Some(choices) = entry.get("choices") {
            let choices = choices.as_array().ok_or_else(invalid)?;
            if choices.is_empty() || choices.len() > 16 {
                return Err(invalid());
            }
            for choice in choices {
                let text = choice.as_str().ok_or_else(invalid)?;
                if text.is_empty() || text.len() > 256 {
                    return Err(invalid());
                }
                bytes += text.len();
            }
        }
        if bytes > 32_768 {
            return Err(invalid());
        }
    }
    Ok(serde_json::from_value::<Arguments>(arguments)
        .map_err(|_| invalid())?
        .questions)
}
impl InputTool {
    fn proposal(&self, questions: &[Question]) -> Result<ToolSuspension, ExtensionError> {
        let descriptions = questions
            .iter()
            .map(|question| {
                let schema = if let Some(choices) = &question.choices {
                    if choices
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        != choices.len()
                    {
                        return Err(invalid());
                    }
                    json!({"type":"string","enum":choices})
                } else {
                    json!({"type":"string","maxLength":8192})
                };
                InteractionDescription::new(
                    &question.name,
                    InteractionKind::UserInput,
                    &question.prompt,
                    schema,
                    self.authority.clone(),
                    None,
                )
                .map_err(|_| invalid())
            })
            .collect::<Result<Vec<_>, _>>()?;
        ToolSuspension::new(
            ToolCheckpoint::new(
                COMPONENT,
                "1",
                serde_json::to_value(questions).map_err(|_| invalid())?,
            )
            .map_err(|_| invalid())?,
            descriptions,
        )
        .map_err(|_| invalid())
    }
    fn finish(
        &self,
        questions: &[Question],
        checkpoint: &ToolCheckpoint,
        responses: Vec<AcceptedToolResponse>,
    ) -> Result<ToolOutput, ExtensionError> {
        if checkpoint != self.proposal(questions)?.checkpoint()
            || responses.len() != questions.len()
        {
            return Err(invalid());
        }
        let mut by_name = BTreeMap::new();
        for response in responses {
            if response.name.len() > 128 {
                return Err(invalid());
            }
            if by_name.insert(response.name.clone(), response).is_some() {
                return Err(invalid());
            }
        }
        let mut ordered = Vec::with_capacity(questions.len());
        let mut refused = false;
        for question in questions {
            let response = by_name.remove(&question.name).ok_or_else(invalid)?;
            match response.state {
                InteractionState::Answered => {
                    let value = response
                        .value
                        .as_ref()
                        .and_then(Value::as_str)
                        .ok_or_else(invalid)?;
                    if value.len() > 32_768
                        || value.chars().count() > 8192
                        || question
                            .choices
                            .as_ref()
                            .is_some_and(|choices| !choices.iter().any(|choice| choice == value))
                    {
                        return Err(invalid());
                    }
                }
                InteractionState::Denied
                | InteractionState::Cancelled
                | InteractionState::Expired => {
                    if response.value.is_some() {
                        return Err(invalid());
                    }
                    refused = true;
                }
                InteractionState::Pending => return Err(invalid()),
            }
            ordered.push(response);
        }
        ToolOutput::from_content_blocks(
            vec![ContentBlock::Text {
                text: serde_json::to_string(&ordered).map_err(|_| invalid())?,
            }],
            refused,
        )
        .map_err(|_| invalid())
    }
}
#[async_trait]
impl ResumableTool for InputTool {
    fn identity(&self) -> ResumableToolIdentity {
        self.identity.clone()
    }
    async fn start(
        &self,
        _: ToolContext,
        arguments: Value,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        Ok(ToolPhaseOutcome::Suspended(
            self.proposal(&questions(arguments)?)?,
        ))
    }
    async fn resume(
        &self,
        _: ToolContext,
        arguments: Value,
        checkpoint: ToolCheckpoint,
        responses: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        Ok(ToolPhaseOutcome::Completed(self.finish(
            &questions(arguments)?,
            &checkpoint,
            responses,
        )?))
    }
}

#[cfg(test)]
#[path = "input_tool/tests.rs"]
mod tests;
