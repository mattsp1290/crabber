//! Bounded JSON-value protection in Crabber's final-redaction phase.
mod patterns;

use async_trait::async_trait;
use crabber::core::{MessageContent, ToolOutput};
use crabber::extension::{
    Extension, ExtensionError, Registrar, ToolResultContext, TransformOutput,
};
use regex::Regex;
use serde::Serialize;
use serde_json::Value;
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::Semaphore;

pub const PLACEHOLDER: &str = "[REDACTED]";
pub const DEFAULT_ORDER: i32 = 1_000_000;

#[derive(Clone, Serialize)]
pub struct Pattern {
    pub id: String,
    pub expression: String,
}

/// Every bound is required. Nodes include object keys; total bytes count decoded
/// strings/keys and JSON primitive encodings, not wire formatting.
#[derive(Clone, Serialize)]
pub struct Limits {
    pub max_field_bytes: usize,
    pub max_total_bytes: usize,
    pub max_depth: usize,
    pub max_nodes: usize,
    pub max_matches_per_field: usize,
    pub max_patterns: usize,
    pub max_pattern_bytes: usize,
    pub max_in_flight: usize,
}

#[derive(Clone, Serialize)]
pub struct Options {
    pub order: i32,
    pub excluded_tools: Vec<String>,
    pub additional_patterns: Vec<Pattern>,
    pub limits: Limits,
}

/// Immutable policy. Pattern text is intentionally absent from Debug/errors.
pub struct ToolResultRedactor {
    options: Options,
    rules: Vec<patterns::Rule>,
    hash: String,
    capacity: Arc<Semaphore>,
}

impl ToolResultRedactor {
    pub fn new(mut options: Options) -> Result<Self, ExtensionError> {
        let l = &options.limits;
        if [
            l.max_field_bytes,
            l.max_total_bytes,
            l.max_depth,
            l.max_nodes,
            l.max_matches_per_field,
            l.max_patterns,
            l.max_pattern_bytes,
            l.max_in_flight,
        ]
        .contains(&0)
            || l.max_field_bytes < PLACEHOLDER.len()
            || l.max_total_bytes < PLACEHOLDER.len()
            || l.max_in_flight > 256
            || l.max_depth > 64
            || l.max_patterns > 64
            || l.max_pattern_bytes > 16 * 1024
            || l.max_field_bytes > 1024 * 1024
            || l.max_total_bytes > 8 * 1024 * 1024
            || l.max_nodes > 100_000
            || l.max_matches_per_field > 4096
        {
            return Err(crate::config_error("redactor-limits"));
        }
        if options.excluded_tools.len() > 256
            || options
                .excluded_tools
                .iter()
                .any(|s| !crate::valid_identity(s))
            || options.additional_patterns.len() > l.max_patterns
        {
            return Err(crate::config_error("redactor-policy"));
        }
        options.excluded_tools.sort();
        options.excluded_tools.dedup();
        options.additional_patterns.sort_by(|a, b| a.id.cmp(&b.id));
        let mut seen = BTreeSet::new();
        let mut rules = patterns::builtins();
        for p in &options.additional_patterns {
            if !crate::valid_identity(&p.id)
                || !seen.insert(&p.id)
                || p.expression.is_empty()
                || p.expression.len() > l.max_pattern_bytes
            {
                return Err(crate::config_error("redactor-pattern"));
            }
            let hir = regex_syntax::Parser::new()
                .parse(&p.expression)
                .map_err(|_| crate::config_error("redactor-pattern"))?;
            if hir.properties().minimum_len().unwrap_or(0) == 0 {
                return Err(crate::config_error("redactor-zero-width"));
            }
            let regex =
                Regex::new(&p.expression).map_err(|_| crate::config_error("redactor-pattern"))?;
            rules.push(patterns::Rule::plain(regex));
        }
        let hash = crate::config_hash(&("json-redactor-v1", PLACEHOLDER, &options));
        let capacity = Arc::new(Semaphore::new(l.max_in_flight));
        Ok(Self {
            options,
            rules,
            hash,
            capacity,
        })
    }

    /// Exclusions match the authoritative tool name exactly. They intentionally
    /// bypass all protection, and should only be configured by a trusted host.
    pub fn redact(&self, tool_name: &str, mut value: Value) -> Value {
        if self
            .options
            .excluded_tools
            .iter()
            .any(|name| name == tool_name)
        {
            return value;
        }
        self.scan(&mut value, None);
        value
    }

    fn scan(&self, value: &mut Value, context: Option<&ToolResultContext>) {
        let mut budget = Budget { nodes: 0, bytes: 0 };
        if !self.measure(value, 1, &mut budget, context) {
            *value = Value::String(PLACEHOLDER.into());
            return;
        }
        self.walk(value, context);
        let mut output_budget = Budget { nodes: 0, bytes: 0 };
        if !self.measure(value, 1, &mut output_budget, context) {
            *value = Value::String(PLACEHOLDER.into());
        }
        if context.is_some_and(|c| c.cancellation().is_cancelled()) {
            *value = Value::String(PLACEHOLDER.into());
        }
    }

    fn measure(
        &self,
        value: &Value,
        depth: usize,
        budget: &mut Budget,
        context: Option<&ToolResultContext>,
    ) -> bool {
        let l = &self.options.limits;
        if depth > l.max_depth || context.is_some_and(|c| c.cancellation().is_cancelled()) {
            return false;
        }
        budget.nodes += 1;
        if budget.nodes > l.max_nodes {
            return false;
        }
        match value {
            Value::Array(values) => {
                for item in values {
                    if !self.measure(item, depth + 1, budget, context) {
                        return false;
                    }
                }
            }
            Value::Object(values) => {
                for (key, item) in values {
                    budget.nodes += 1;
                    budget.bytes = budget.bytes.saturating_add(key.len());
                    if budget.nodes > l.max_nodes
                        || budget.bytes > l.max_total_bytes
                        || !self.measure(item, depth + 1, budget, context)
                    {
                        return false;
                    }
                }
            }
            Value::String(s) => budget.bytes = budget.bytes.saturating_add(s.len()),
            _ => budget.bytes = budget.bytes.saturating_add(value.to_string().len()),
        }
        budget.bytes <= l.max_total_bytes
    }

    fn walk(&self, value: &mut Value, context: Option<&ToolResultContext>) {
        match value {
            Value::String(s) => *s = self.scalar(s, context),
            Value::Array(items) => {
                for item in items {
                    self.walk(item, context);
                }
            }
            Value::Object(items) => {
                // Never rename keys: a secret/unsafe key placeholderizes the
                // containing object, avoiding collisions and leaving no key leak.
                if items.keys().any(|key| self.scalar(key, context) != *key) {
                    *value = Value::String(PLACEHOLDER.into());
                } else {
                    for item in items.values_mut() {
                        self.walk(item, context);
                    }
                }
            }
            _ => {}
        }
    }

    fn scalar(&self, text: &str, context: Option<&ToolResultContext>) -> String {
        if text.len() > self.options.limits.max_field_bytes || text.contains('\0') {
            return PLACEHOLDER.into();
        }
        let mut ranges = Vec::new();
        for rule in &self.rules {
            if context.is_some_and(|c| c.cancellation().is_cancelled()) {
                return PLACEHOLDER.into();
            }
            for m in rule.regex.find_iter(text) {
                if !rule.admits(text, m.start(), m.end()) {
                    continue;
                }
                if ranges.len() == self.options.limits.max_matches_per_field {
                    return PLACEHOLDER.into();
                }
                ranges.push((m.start(), m.end()));
            }
        }
        ranges.sort_unstable();
        let mut merged: Vec<(usize, usize)> = Vec::new();
        for (start, end) in ranges {
            if let Some(last) = merged.last_mut()
                && start <= last.1
            {
                last.1 = last.1.max(end);
            } else {
                merged.push((start, end));
            }
        }
        let mut output = String::new();
        let mut previous = 0;
        for (start, end) in merged {
            output.push_str(&text[previous..start]);
            output.push_str(PLACEHOLDER);
            previous = end;
        }
        output.push_str(&text[previous..]);
        // Replacement can expand short secrets. Bound the produced field too.
        if output.len() > self.options.limits.max_field_bytes {
            PLACEHOLDER.into()
        } else {
            output
        }
    }
}

struct Budget {
    nodes: usize,
    bytes: usize,
}

#[async_trait]
impl Extension for ToolResultRedactor {
    fn id(&self) -> &str {
        "crabber-extensions/tool-result-redactor"
    }
    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    fn config_hash(&self) -> String {
        self.hash.clone()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let policy = Arc::new(Self {
            options: self.options.clone(),
            rules: self.rules.clone(),
            hash: self.hash.clone(),
            capacity: self.capacity.clone(),
        });
        registrar.on_final_redaction(
            self.options.order,
            "tool-result-redactor",
            Arc::new(move |context, value| {
                let policy = policy.clone();
                Box::pin(async move {
                    if policy
                        .options
                        .excluded_tools
                        .iter()
                        .any(|name| name == context.tool_name())
                    {
                        return Ok(TransformOutput::new(value));
                    }
                    let Ok(permit) = policy.capacity.clone().try_acquire_owned() else {
                        return Ok(TransformOutput::new(placeholder(value.is_error())));
                    };
                    let is_error = value.is_error();
                    let (send, recv) = tokio::sync::oneshot::channel();
                    let cleanup = context.cleanup().clone();
                    cleanup.spawn(async move {
                        let result = tokio::task::spawn_blocking(move || {
                            let _permit = permit;
                            policy.scan_output(value, &context)
                        })
                        .await
                        .unwrap_or_else(|_| placeholder(is_error));
                        let _ = send.send(result);
                    });
                    let result = recv.await.unwrap_or_else(|_| placeholder(is_error));
                    Ok(TransformOutput::new(result))
                })
            }),
        );
        Ok(())
    }
}

fn placeholder(is_error: bool) -> ToolOutput {
    let output = ToolOutput::from_json(&Value::String(PLACEHOLDER.into()))
        .expect("fixed placeholder fits typed output");
    if is_error {
        output.with_error()
    } else {
        output
    }
}
impl ToolResultRedactor {
    fn scan_output(&self, output: ToolOutput, context: &ToolResultContext) -> ToolOutput {
        let is_error = output.is_error();
        let scanned = if let Ok(mut value) = output.json_value() {
            self.scan(&mut value, Some(context));
            ToolOutput::from_json(&value)
        } else {
            let mut blocks = Vec::with_capacity(output.content().len());
            for block in output.content() {
                let MessageContent::Text { text } = block else {
                    return placeholder(is_error);
                };
                let mut value = Value::String(text.clone());
                self.scan(&mut value, Some(context));
                let Value::String(text) = value else {
                    return placeholder(is_error);
                };
                blocks.push(MessageContent::Text { text });
            }
            ToolOutput::new(blocks, is_error)
        };
        match scanned {
            Ok(value) if is_error => value.with_error(),
            Ok(value) => value,
            Err(_) => placeholder(is_error),
        }
    }
}

#[cfg(test)]
mod typed_output_tests {
    use super::*;
    use crabber::{
        core::{MediaKind, MediaSource},
        extension::{ToolInput, ToolOutcomeClass},
    };
    fn policy() -> ToolResultRedactor {
        ToolResultRedactor::new(Options {
            order: DEFAULT_ORDER,
            excluded_tools: vec![],
            additional_patterns: vec![Pattern {
                id: "fixture".into(),
                expression: "SYNTHETIC_SECRET".into(),
            }],
            limits: Limits {
                max_field_bytes: 1024,
                max_total_bytes: 4096,
                max_depth: 8,
                max_nodes: 100,
                max_matches_per_field: 8,
                max_patterns: 8,
                max_pattern_bytes: 1024,
                max_in_flight: 1,
            },
        })
        .unwrap()
    }
    #[test]
    fn typed_redaction_preserves_bytes_and_errors_and_replaces_media() {
        let context = ToolResultContext::new(
            "fixture".into(),
            true,
            ToolInput::Normalized(serde_json::json!({})),
            "call".into(),
            "session".into(),
            "run".into(),
            ToolOutcomeClass::Succeeded,
        );
        let policy = policy();
        for is_error in [false, true] {
            let json = ToolOutput::from_json(&serde_json::json!({"safe":"unchanged"})).unwrap();
            let json = if is_error { json.with_error() } else { json };
            assert_eq!(policy.scan_output(json.clone(), &context), json);
            let plain = ToolOutput::new(
                vec![MessageContent::Text {
                    text: "plain safe text".into(),
                }],
                is_error,
            )
            .unwrap();
            assert_eq!(policy.scan_output(plain.clone(), &context), plain);
            let secret = ToolOutput::new(
                vec![MessageContent::Text {
                    text: "SYNTHETIC_SECRET".into(),
                }],
                is_error,
            )
            .unwrap();
            let scanned = policy.scan_output(secret, &context);
            assert!(
                matches!(&scanned.content()[0], MessageContent::Text { text } if text == "[REDACTED]")
            );
            assert_eq!(scanned.is_error(), is_error);
            let media = ToolOutput::new(
                vec![MessageContent::Media {
                    kind: MediaKind::Image,
                    mime_type: "image/png".into(),
                    source: MediaSource::File {
                        file_id: "synthetic".into(),
                    },
                }],
                is_error,
            )
            .unwrap();
            let replaced = policy.scan_output(media, &context);
            assert_eq!(replaced, placeholder(is_error));
            assert_eq!(
                replaced.content(),
                &[MessageContent::Text {
                    text: "\"[REDACTED]\"".into()
                }]
            );
        }
    }
}
