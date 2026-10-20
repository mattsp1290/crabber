use super::{WireInputError, content, identity};
use ag_ui_core::types::Message;
use crabber_core::{
    FrontendResultStatus, FrontendToolResult, InputContent, InputMessage, InputPart, Role,
    ToolCallId, ToolOutput,
};

pub(super) fn message(message: &Message) -> Result<InputMessage, WireInputError> {
    // The SDK preserves these fields, but the strict runtime input has no
    // representation for their attribution/private semantics. Reject instead
    // of silently assigning a privileged source or reclassifying reasoning.
    let (id, role, parts) = match message {
        Message::System {
            id,
            content,
            name,
            encrypted_value,
            subagent_run_id,
            ..
        } => {
            extras(
                name.as_deref(),
                encrypted_value.as_deref(),
                subagent_run_id.as_deref(),
            )?;
            (id, Role::System, text(content))
        }
        Message::User {
            id,
            content: value,
            name,
            encrypted_value,
            subagent_run_id,
            ..
        } => {
            extras(
                name.as_deref(),
                encrypted_value.as_deref(),
                subagent_run_id.as_deref(),
            )?;
            (id, Role::User, content::parts(value)?)
        }
        Message::Assistant {
            id,
            content,
            name,
            tool_calls,
            encrypted_value,
            subagent_run_id,
            ..
        } => {
            extras(
                name.as_deref(),
                encrypted_value.as_deref(),
                subagent_run_id.as_deref(),
            )?;
            let parts = assistant_parts(
                content.as_deref(),
                tool_calls.as_deref().unwrap_or_default(),
            )?;
            (id, Role::Assistant, parts)
        }
        Message::Tool {
            id,
            tool_call_id,
            content: value,
            error,
            encrypted_value,
            subagent_run_id,
            ..
        } => {
            extras(None, encrypted_value.as_deref(), subagent_run_id.as_deref())?;
            (
                id,
                Role::Tool,
                vec![InputPart {
                    id: None,
                    content: InputContent::ToolResult {
                        call_id: ToolCallId::from(tool_call_id.as_str()),
                        content: content::result_content(value)?,
                        is_error: error.is_some(),
                    },
                }],
            )
        }
        Message::Developer { .. } | Message::Reasoning { .. } | Message::Activity { .. } => {
            return Err(WireInputError::Unsupported);
        }
    };
    identity(id.as_str())?;
    Ok(InputMessage {
        id: Some(id.to_string()),
        parent_id: None,
        role,
        parts,
    })
}

fn assistant_parts(
    content: Option<&str>,
    calls: &[ag_ui_core::types::ToolCall],
) -> Result<Vec<InputPart>, WireInputError> {
    if calls.len() > crabber_core::input::MAX_INPUT_PARTS {
        return Err(WireInputError::Limit);
    }
    let mut parts = content.map_or_else(Vec::new, text);
    for call in calls {
        if call.call_type != "function" || call.encrypted_value.is_some() {
            return Err(WireInputError::Unsupported);
        }
        parts.push(InputPart {
            id: None,
            content: InputContent::ToolCall {
                call_id: ToolCallId::from(call.id.as_str()),
                name: call.function.name.clone(),
                arguments: serde_json::from_str(&call.function.arguments)
                    .map_err(|_| WireInputError::Malformed)?,
            },
        });
    }
    Ok(parts)
}

pub(super) fn frontend_result(message: &Message) -> Result<FrontendToolResult, WireInputError> {
    let Message::Tool {
        tool_call_id,
        content: value,
        error,
        encrypted_value,
        subagent_run_id,
        ..
    } = message
    else {
        return Err(WireInputError::Malformed);
    };
    extras(None, encrypted_value.as_deref(), subagent_run_id.as_deref())?;
    let failed = error.is_some();
    FrontendToolResult::new(
        ToolCallId::from(tool_call_id.as_str()),
        if failed {
            FrontendResultStatus::Failed
        } else {
            FrontendResultStatus::Completed
        },
        ToolOutput::new(content::result_content(value)?, failed)
            .map_err(|_| WireInputError::Malformed)?,
    )
    .map_err(|_| WireInputError::Malformed)
}

fn text(value: &str) -> Vec<InputPart> {
    vec![InputPart {
        id: None,
        content: InputContent::Content {
            content: crabber_core::MessageContent::Text { text: value.into() },
        },
    }]
}
fn extras(
    name: Option<&str>,
    encrypted: Option<&str>,
    subagent: Option<&str>,
) -> Result<(), WireInputError> {
    if name.is_some() || encrypted.is_some() || subagent.is_some() {
        Err(WireInputError::Unsupported)
    } else {
        Ok(())
    }
}
