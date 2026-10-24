use super::{Limits, Outcome};
use serde_json::Value;

pub(super) fn extract(arguments: &Value, field: &str, limits: &Limits) -> Result<String, Outcome> {
    let object = arguments.as_object().ok_or(Outcome::InvalidCommand)?;
    let mut remaining = limits.max_json_nodes;
    let mut bytes = limits.max_analysis_bytes;
    visit(
        arguments,
        1,
        &mut remaining,
        &mut bytes,
        limits,
        Some(field),
    )?;
    let command = object
        .get(field)
        .and_then(Value::as_str)
        .ok_or(Outcome::InvalidCommand)?;
    Ok(command.to_owned())
}
fn charge(remaining: &mut usize) -> Result<(), Outcome> {
    *remaining = remaining.checked_sub(1).ok_or(Outcome::AnalysisLimit)?;
    Ok(())
}
fn visit(
    value: &Value,
    depth: usize,
    remaining: &mut usize,
    bytes: &mut usize,
    limits: &Limits,
    command_field: Option<&str>,
) -> Result<(), Outcome> {
    if (value.is_array() || value.is_object()) && depth > limits.max_json_depth {
        return Err(Outcome::AnalysisLimit);
    }
    charge(remaining)?;
    match value {
        Value::String(text) => {
            *bytes = bytes
                .checked_sub(text.len())
                .ok_or(Outcome::AnalysisLimit)?;
            if text.contains('\0') {
                return Err(Outcome::InvalidCommand);
            }
        }
        Value::Array(values) => {
            for value in values {
                visit(
                    value,
                    depth.saturating_add(1),
                    remaining,
                    bytes,
                    limits,
                    None,
                )?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                charge(remaining)?;
                *bytes = bytes.checked_sub(key.len()).ok_or(Outcome::AnalysisLimit)?;
                if key.contains('\0') {
                    return Err(Outcome::InvalidCommand);
                }
                visit(
                    value,
                    depth.saturating_add(1),
                    remaining,
                    bytes,
                    limits,
                    None,
                )?;
                if command_field == Some(key.as_str()) {
                    let text = value.as_str().ok_or(Outcome::InvalidCommand)?;
                    if text.len() > limits.max_command_bytes {
                        return Err(Outcome::AnalysisLimit);
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}
