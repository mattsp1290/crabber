use super::{AskOption, Limits, MAX_OPTIONS, MIN_OPTIONS, invalid};
use crabber::ExtensionError;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    question: String,
    options: Vec<InputOption>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputOption {
    label: String,
    description: Option<String>,
}

pub(super) fn normalize(
    value: Value,
    limits: &Limits,
) -> Result<(String, Vec<AskOption>), ExtensionError> {
    let input: Input = serde_json::from_value(value).map_err(|_| invalid("shape"))?;
    if unsafe_controls(&input.question)
        || input.options.iter().any(|o| {
            unsafe_controls(&o.label) || o.description.as_deref().is_some_and(unsafe_controls)
        })
    {
        return Err(invalid("options"));
    }
    if !required_text(&input.question, limits.max_question_bytes) {
        return Err(invalid("question"));
    }
    if !(MIN_OPTIONS..=MAX_OPTIONS).contains(&input.options.len()) {
        return Err(invalid("option-count"));
    }
    let mut labels = HashSet::new();
    let mut options = Vec::with_capacity(input.options.len());
    for option in input.options {
        if !required_text(&option.label, limits.max_option_label_bytes) {
            return Err(invalid("option-label"));
        }
        let description = match option.description {
            Some(description) if description.is_empty() => None,
            Some(description) if text(&description, limits.max_option_description_bytes) => {
                Some(description)
            }
            Some(_) => return Err(invalid("option-description")),
            None => None,
        };
        if !labels.insert(option.label.clone()) {
            return Err(invalid("duplicate-option-label"));
        }
        options.push(AskOption {
            label: option.label,
            description,
        });
    }
    Ok((input.question, options))
}

pub(super) fn text(value: &str, max_bytes: usize) -> bool {
    value.len() <= max_bytes && !value.contains('\0')
}

pub(super) fn required_text(value: &str, max_bytes: usize) -> bool {
    text(value, max_bytes) && !value.trim().is_empty()
}

fn unsafe_controls(value: &str) -> bool {
    value
        .chars()
        .any(|c| c.is_control() && c != '\n' && c != '\t')
}
