use super::{Limits, input_error};
use crabber::extension::ExtensionError;
use serde_json::{Map, Value};
use std::path::{Component, Path, PathBuf};

pub(super) struct Start {
    pub(super) command: String,
    pub(super) directory: PathBuf,
    pub(super) timeout: u64,
}
fn object<'a>(value: &'a Value, fields: &[&str]) -> Result<&'a Map<String, Value>, ExtensionError> {
    let object = value.as_object().ok_or_else(|| input_error("shape"))?;
    if object.keys().any(|key| !fields.contains(&key.as_str())) {
        return Err(input_error("shape"));
    }
    Ok(object)
}
pub(super) fn start(value: &Value, limits: &Limits) -> Result<Start, ExtensionError> {
    let object = object(value, &["command", "working_directory", "timeout_seconds"])?;
    let command = object
        .get("command")
        .and_then(Value::as_str)
        .ok_or_else(|| input_error("shape"))?;
    if command.is_empty() || command.len() > limits.max_command_bytes || command.contains('\0') {
        return Err(input_error("command"));
    }
    let directory = match object.get("working_directory") {
        None => ".",
        Some(Value::String(s)) if s.is_empty() => ".",
        Some(Value::String(s)) => s,
        _ => return Err(input_error("shape")),
    };
    if directory.len() > limits.max_working_directory_bytes
        || directory.contains('\0')
        || Path::new(directory).is_absolute()
    {
        return Err(input_error("working-directory"));
    }
    let mut normalized = PathBuf::new();
    for component in Path::new(directory).components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir => {
                if !normalized.pop() {
                    return Err(input_error("working-directory"));
                }
            }
            _ => return Err(input_error("working-directory")),
        }
    }
    if normalized.as_os_str().is_empty() {
        normalized.push(".");
    }
    let timeout = match object.get("timeout_seconds") {
        None => 0,
        Some(value) => value.as_u64().ok_or_else(|| input_error("timeout"))?,
    };
    if timeout > limits.max_timeout.as_secs() {
        return Err(input_error("timeout"));
    }
    Ok(Start {
        command: command.into(),
        directory: normalized,
        timeout: if timeout == 0 {
            limits.default_timeout.as_secs()
        } else {
            timeout
        },
    })
}
pub(super) fn id(value: &Value) -> Result<&str, ExtensionError> {
    let object = object(value, &["id"])?;
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| input_error("id"))?;
    if id.len() != 53
        || !id.starts_with("job_")
        || id.as_bytes()[36] != b'_'
        || !id.as_bytes()[4..].iter().enumerate().all(|(index, byte)| {
            index == 32 || byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)
        })
    {
        return Err(input_error("id"));
    }
    Ok(id)
}
pub(super) fn list(value: &Value) -> Result<(), ExtensionError> {
    object(value, &[]).map(|_| ())
}
