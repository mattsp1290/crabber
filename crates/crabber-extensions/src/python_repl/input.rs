use super::{Limits, input_error};
use crabber::ExtensionError;
use serde_json::Value;
use std::time::Duration;
pub(super) struct Execute {
    pub(super) code: String,
    pub(super) timeout: Duration,
}
pub(super) fn execute(value: &Value, limits: &Limits) -> Result<Execute, ExtensionError> {
    let object = value.as_object().ok_or_else(|| input_error("shape"))?;
    if object
        .keys()
        .any(|key| key != "code" && key != "timeout_seconds")
    {
        return Err(input_error("shape"));
    }
    let code = object
        .get("code")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && !s.contains('\0') && s.len() <= limits.max_code_bytes)
        .ok_or_else(|| input_error("code"))?;
    let seconds = match object.get("timeout_seconds") {
        None => 0,
        Some(value) => value
            .as_u64()
            .filter(|s| *s <= limits.max_timeout.as_secs())
            .ok_or_else(|| input_error("timeout"))?,
    };
    Ok(Execute {
        code: code.into(),
        timeout: if seconds == 0 {
            limits.default_timeout
        } else {
            Duration::from_secs(seconds)
        },
    })
}
pub(super) fn clear(value: &Value) -> Result<(), ExtensionError> {
    if value.as_object().is_some_and(|o| o.is_empty()) {
        Ok(())
    } else {
        Err(input_error("shape"))
    }
}
