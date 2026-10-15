use super::{
    AdmissionKind, InputContent, InputState, MAX_INLINE_MEDIA_BYTES, MAX_INPUT_BYTES,
    MAX_INPUT_JSON_DEPTH, MAX_INPUT_MESSAGES, MAX_INPUT_PARTS, MediaKind, MediaSource,
    MessageContent, WireEnvelope,
};
use crate::{CoreError, Role};
use base64::Engine;
use serde::Serialize;
use serde_json::Value;
use std::{collections::BTreeSet, io};

pub(super) fn invalid() -> CoreError {
    CoreError::Validation("invalid typed input".into())
}
pub(super) fn identity(value: &str, bound: usize) -> bool {
    !value.is_empty() && value.len() <= bound && !value.chars().any(char::is_control)
}
fn optional_id(id: Option<&str>, seen: &mut BTreeSet<String>) -> Result<(), CoreError> {
    if let Some(id) = id
        && (!identity(id, 512) || !seen.insert(id.into()))
    {
        return Err(invalid());
    }
    Ok(())
}
fn json(value: &Value, depth: usize) -> Result<(), CoreError> {
    if depth > MAX_INPUT_JSON_DEPTH {
        return Err(invalid());
    }
    match value {
        Value::Array(values) => {
            for value in values {
                json(value, depth + 1)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                json(value, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn content(value: &MessageContent) -> Result<(), CoreError> {
    match value {
        MessageContent::Text { text } => {
            if text.contains('\0') {
                return Err(invalid());
            }
        }
        MessageContent::Media {
            kind,
            mime_type,
            source,
        } => {
            let Some((major, minor)) = mime_type.split_once('/') else {
                return Err(invalid());
            };
            if mime_type.len() > 127
                || major.is_empty()
                || minor.is_empty()
                || minor.contains('/')
                || !mime_type
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"!#$&^_.+-/".contains(&b))
            {
                return Err(invalid());
            }
            let kind_matches = match kind {
                MediaKind::Image => major.eq_ignore_ascii_case("image"),
                MediaKind::Audio => major.eq_ignore_ascii_case("audio"),
                MediaKind::Video => major.eq_ignore_ascii_case("video"),
                MediaKind::Document => true,
            };
            if !kind_matches {
                return Err(invalid());
            }
            match source {
                MediaSource::Data { base64 } => {
                    if base64.len() > MAX_INLINE_MEDIA_BYTES.div_ceil(3) * 4 || base64.is_empty() {
                        return Err(invalid());
                    }
                    let decoded = base64::engine::general_purpose::STANDARD
                        .decode(base64)
                        .map_err(|_| invalid())?;
                    if decoded.is_empty() || decoded.len() > MAX_INLINE_MEDIA_BYTES {
                        return Err(invalid());
                    }
                }
                MediaSource::Url { url } => {
                    if !identity(url, 8192) || url.chars().any(char::is_whitespace) {
                        return Err(invalid());
                    }
                    let url = url::Url::parse(url).map_err(|_| invalid())?;
                    if !matches!(url.scheme(), "http" | "https")
                        || !url.has_host()
                        || !url.username().is_empty()
                        || url.password().is_some()
                    {
                        return Err(invalid());
                    }
                }
                MediaSource::File { file_id } => {
                    if !identity(file_id, 512) {
                        return Err(invalid());
                    }
                }
            }
        }
    }
    Ok(())
}

// Count escaped serialized bytes before constructing a canonical Value/string.
struct BoundedBytes(usize);
impl io::Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|size| *size <= MAX_INPUT_BYTES)
            .ok_or_else(|| io::Error::other("input bound"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn bounded(value: &impl Serialize) -> Result<(), CoreError> {
    serde_json::to_writer(BoundedBytes(0), value).map_err(|_| invalid())
}

pub(super) fn envelope(value: &WireEnvelope) -> Result<(), CoreError> {
    if value.version != 1
        || value.messages.len() > MAX_INPUT_MESSAGES
        || value.frontend_tools.len() > 128
        || value.context.len() > 128
        || (value.messages.is_empty() && matches!(value.admission, AdmissionKind::Ordinary))
    {
        return Err(invalid());
    }
    let mut message_ids = BTreeSet::new();
    let mut part_ids = BTreeSet::new();
    let mut calls = BTreeSet::new();
    let mut results = BTreeSet::new();
    let mut parts = 0;
    for message in &value.messages {
        optional_id(message.id.as_deref(), &mut message_ids)?;
        if message
            .parent_id
            .as_deref()
            .is_some_and(|parent| !identity(parent, 512) || message.id.as_deref() == Some(parent))
        {
            return Err(invalid());
        }
        parts += message.parts.len();
        if message.parts.is_empty() || parts > MAX_INPUT_PARTS {
            return Err(invalid());
        }
        for part in &message.parts {
            optional_id(part.id.as_deref(), &mut part_ids)?;
            match &part.content {
                InputContent::Content { content: block } => {
                    if message.role == Role::Tool {
                        return Err(invalid());
                    }
                    content(block)?;
                }
                InputContent::ToolCall {
                    call_id,
                    name,
                    arguments,
                } => {
                    if message.role != Role::Assistant
                        || !identity(&call_id.0, 512)
                        || !identity(name, 128)
                        || !calls.insert(&call_id.0)
                    {
                        return Err(invalid());
                    }
                    json(arguments, 0)?;
                }
                InputContent::ToolResult {
                    call_id,
                    content: blocks,
                    ..
                } => {
                    if message.role != Role::Tool
                        || !identity(&call_id.0, 512)
                        || !results.insert(&call_id.0)
                        || blocks.len() > MAX_INPUT_PARTS
                    {
                        return Err(invalid());
                    }
                    for block in blocks {
                        content(block)?;
                    }
                }
            }
        }
    }
    let mut names = BTreeSet::new();
    for tool in &value.frontend_tools {
        if !identity(&tool.name, 128)
            || !names.insert(&tool.name)
            || tool.description.contains('\0')
            || !matches!(tool.input_parameters, Value::Object(_) | Value::Bool(_))
        {
            return Err(invalid());
        }
        json(&tool.input_parameters, 0)?;
        if let Some(schema) = &tool.result_contract {
            if !matches!(schema, Value::Object(_) | Value::Bool(_)) {
                return Err(invalid());
            }
            json(schema, 0)?;
        }
    }
    for context in &value.context {
        if context.description.contains('\0') {
            return Err(invalid());
        }
        json(&context.value, 0)?;
    }
    if let InputState::Present { value } = &value.state {
        json(value, 0)?;
    }
    if let AdmissionKind::Continuation { run_id, .. } = &value.admission
        && !identity(&run_id.0, 512)
    {
        return Err(invalid());
    }
    bounded(value)
}
