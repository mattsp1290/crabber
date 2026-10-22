use super::{AskOption, CUSTOM_OPTION_LABEL, Limits, failure, input};
use crabber::ExtensionError;
use serde_json::{Value, json};

pub(super) fn response(options: &[AskOption], custom_cap: usize) -> Value {
    let mut choices = options.iter().enumerate().map(|(i, option)| {
        let mut branch = json!({"required":["selected"],"properties":{"selected":{"const":i+1}},"title":option.label});
        if let Some(description) = &option.description { branch["description"] = json!(description); }
        branch
    }).collect::<Vec<_>>();
    choices.push(json!({"required":["custom"],"title":CUSTOM_OPTION_LABEL}));
    json!({"type":"object","additionalProperties":false,"minProperties":1,"maxProperties":1,
        "properties":{"selected":{"type":"integer","minimum":1,"maximum":options.len()},
            "custom":{"type":"string","minLength":1,"maxLength":custom_cap}},"oneOf":choices})
}
pub(super) fn answer(
    value: Option<&Value>,
    options: &[AskOption],
    limits: &Limits,
) -> Result<Value, ExtensionError> {
    let object = value
        .and_then(Value::as_object)
        .ok_or_else(|| failure("answer"))?;
    if object.len() != 1 {
        return Err(failure("answer"));
    }
    // JSON Schema integers include values written as 2.0. Compare against the
    // tiny bounded choice set so Store-accepted integral numbers map identically.
    if let Some(number) = object.get("selected").and_then(Value::as_f64)
        && let Some(index) = (1..=options.len()).find(|i| number == *i as f64)
    {
        return Ok(
            json!({"status":"selected","answer":options[index - 1].label,"selected_option":index}),
        );
    }
    if let Some(answer) = object.get("custom").and_then(Value::as_str)
        && input::required_text(answer, limits.max_custom_answer_bytes)
    {
        return Ok(json!({"status":"custom","answer":answer}));
    }
    Err(failure("answer"))
}
