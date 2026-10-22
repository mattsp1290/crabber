use super::{Limits, invalid};
use crabber::ExtensionError;
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    task: String,
    profile: String,
}

pub(super) fn normalize(value: Value, limits: &Limits) -> Result<(String, String), ExtensionError> {
    let input: Input = serde_json::from_value(value).map_err(|_| invalid("shape"))?;
    if input.task.len() > limits.max_task_bytes
        || input.task.contains('\0')
        || input.task.trim().is_empty()
    {
        return Err(invalid("task"));
    }
    let mut bytes = input.profile.bytes();
    if input.profile.len() > limits.max_profile_bytes
        || !bytes.next().is_some_and(|b| b.is_ascii_alphanumeric())
        || !bytes.all(|b| b.is_ascii_alphanumeric() || b"._:/-".contains(&b))
    {
        return Err(invalid("profile"));
    }
    Ok((input.task, input.profile))
}

fn valid_task_id(id: &str) -> bool {
    id.len() == super::TASK_ID_BYTES
        && id.starts_with(super::TASK_ID_PREFIX)
        && id.as_bytes()[super::TASK_ID_SEPARATOR] == b'_'
        && id.as_bytes()[super::TASK_ID_PREFIX.len()..super::TASK_ID_SEPARATOR]
            .iter()
            .chain(&id.as_bytes()[super::TASK_ID_SEPARATOR + 1..])
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
}
pub(super) fn task_id(value: &Value) -> Result<&str, ExtensionError> {
    let object = value.as_object().ok_or_else(|| invalid("task_id"))?;
    let id = object
        .get("task_id")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("task_id"))?;
    if object.len() != 1 || !valid_task_id(id) {
        return Err(invalid("task_id"));
    }
    Ok(id)
}
pub(super) fn wait(
    value: Value,
    limits: &Limits,
) -> Result<(Vec<String>, std::time::Duration), ExtensionError> {
    // Preserve absent versus null: serde Option would silently accept null.
    let object = value.as_object().ok_or_else(|| invalid("shape"))?;
    if object
        .keys()
        .any(|k| k != "task_ids" && k != "timeout_seconds")
    {
        return Err(invalid("shape"));
    }
    let ids = object
        .get("task_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("task_ids"))?;
    if ids.is_empty() || ids.len() > limits.max_wait_ids {
        return Err(invalid("task_ids"));
    }
    let mut result = Vec::with_capacity(ids.len());
    for value in ids {
        let id = value
            .as_str()
            .filter(|id| valid_task_id(id))
            .ok_or_else(|| invalid("task_ids"))?;
        if result.iter().any(|s| s == id) {
            return Err(invalid("task_ids"));
        }
        result.push(id.to_owned());
    }
    let timeout = match object.get("timeout_seconds") {
        None => limits.max_wait,
        Some(value) => {
            // Require unsigned integer JSON representation; integral float
            // syntax (such as 1.0) is deliberately rejected by the executor.
            let seconds = value
                .as_u64()
                .filter(|s| *s > 0 && *s <= limits.max_wait.as_secs())
                .ok_or_else(|| invalid("timeout"))?;
            std::time::Duration::from_secs(seconds)
        }
    };
    Ok((result, timeout))
}
