#![allow(clippy::semicolon_if_nothing_returned)]
use crate::{ModelRequest, ProviderError, ProviderErrorKind, StreamDelta, sse::Event};
use crabber_core::{ContentBlock, Role, ToolCallId, Usage};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

pub fn body(request: &ModelRequest) -> Value {
    let mut messages = Vec::new();
    for message in &request.messages {
        let mut content = Vec::new();
        for part in &message.parts {
            match &part.content {
                ContentBlock::Reasoning {
                    text,
                    provider_state: Some(state),
                    item_id: Some(_),
                    provenance: crabber_core::ReasoningProvenance::Private,
                } if message.role == Role::Assistant
                    && state["codec_id"] == "crabber.anthropic.thinking/v1"
                    && state["signature"].is_string() =>
                {
                    content.push(
                        json!({"type":"thinking","thinking":text,"signature":state["signature"]}),
                    );
                }
                // The API rejects empty text blocks.
                ContentBlock::Text { text }
                    if matches!(message.role, Role::User | Role::Assistant) && !text.is_empty() =>
                {
                    content.push(json!({"type":"text","text":text}))
                }
                block @ ContentBlock::MediaContent { .. } if message.role == Role::User => {
                    content.extend(crate::real::outbound::messages_image(block));
                }
                ContentBlock::ToolCall {
                    call_id,
                    name,
                    arguments,
                } => content
                    .push(json!({"type":"tool_use","id":call_id.0,"name":name,"input":arguments})),
                ContentBlock::ToolResult {
                    call_id,
                    content: blocks,
                    is_error,
                } => {
                    let result = tool_result_content(blocks);
                    content.push(json!({"type":"tool_result","tool_use_id":call_id.0,"content":result,"is_error":is_error}));
                }
                ContentBlock::ProviderState { codec_id, payload }
                    if message.role == Role::Assistant
                        && codec_id == "crabber.anthropic.redacted-thinking/v1"
                        && payload["type"] == "redacted_thinking" =>
                {
                    content.push(payload.clone());
                }
                _ => {}
            }
        }
        if !content.is_empty() {
            messages.push(json!({"role":if message.role == Role::Assistant {"assistant"} else {"user"},"content":content}));
        }
    }
    let mut body = json!({"model":request.selection.model_id,"messages":messages,"max_tokens":4096,"stream":true});
    // The API rejects replayed tool blocks without `tools`, so only a turn with
    // no tools and no tool history is sent tool free.
    let tool_history = request.messages.iter().any(|message| {
        message.parts.iter().any(|part| {
            matches!(
                part.content,
                ContentBlock::ToolCall { .. } | ContentBlock::ToolResult { .. }
            )
        })
    });
    if !request.tools.is_empty() || tool_history {
        let tools: Vec<_> = request
            .tools
            .iter()
            .map(|t| json!({"name":t.name,"description":t.description,"input_schema":t.parameters}))
            .collect();
        body["tools"] = json!(tools);
    }
    if let Some(temperature) = request.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(max_tokens) = request.max_tokens {
        body["max_tokens"] = json!(max_tokens);
    }
    if let Some(tool_choice) = request
        .tool_choice
        .as_ref()
        .filter(|_| !request.tools.is_empty())
    {
        body["tool_choice"] = match tool_choice.as_str() {
            "auto" | "any" | "none" => json!({"type":tool_choice}),
            "required" => json!({"type":"any"}),
            name => json!({"type":"tool","name":name}),
        };
    }
    if let Some(system) = &request.system {
        body["system"] = json!(system);
    }
    body
}
/// Text-only results keep the joined-string form; media needs a block array.
fn tool_result_content(blocks: &[ContentBlock]) -> Value {
    if !blocks
        .iter()
        .any(|b| matches!(b, ContentBlock::MediaContent { .. }))
    {
        let text = blocks
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        return json!(text);
    }
    // The API rejects empty text blocks inside content arrays.
    let parts: Vec<_> = blocks
        .iter()
        .filter_map(|b| match b {
            ContentBlock::Text { text } if !text.is_empty() => {
                Some(json!({"type":"text","text":text}))
            }
            block => crate::real::outbound::messages_image(block),
        })
        .collect();
    json!(parts)
}
#[derive(Default)]
pub struct Codec {
    calls: HashMap<u64, ToolCallId>,
    initial_input: HashMap<u64, Value>,
    streamed_input: HashSet<u64>,
    usage: Usage,
    thinking: HashSet<u64>,
    seen_thinking: HashSet<u64>,
    saw_tool_use: bool,
    saw_text: bool,
    /// Thinking blocks started without a signature yet; replaying one later
    /// fails outbound validation, so truncation inside one is an error.
    unsigned_thinking: HashSet<u64>,
    /// Set by text or tool output (which observers see as it streams) or by
    /// `message_stop`; a later stream error is never retryable because a retry
    /// would duplicate published output. Private thinking is never published.
    observable_emitted: bool,
}
impl Codec {
    pub fn event(&mut self, event: &Event) -> Result<Vec<StreamDelta>, ProviderError> {
        let value: Value =
            serde_json::from_str(&event.data).map_err(|_| invalid("invalid Messages event"))?;
        let mut out = Vec::new();
        match event.name.as_str() {
            "message_start" => {
                self.usage.input_tokens = value["message"]["usage"]["input_tokens"]
                    .as_u64()
                    .unwrap_or(0)
            }
            "content_block_start" => self.start_block(&value, &mut out)?,
            "content_block_delta" => self.delta_block(&value, &mut out)?,
            "content_block_stop" => self.stop_block(&value, &mut out),
            "message_delta" => {
                if let Some(output) = value["usage"]["output_tokens"].as_u64() {
                    self.usage.output_tokens = output;
                }
                if let Some(error) = self.stop_reason_error(value["delta"]["stop_reason"].as_str())
                {
                    out.push(StreamDelta::Error(error));
                }
            }
            "message_stop" => {
                self.observable_emitted = true;
                out.push(StreamDelta::Usage(self.usage.clone()));
                out.push(StreamDelta::Completed);
            }
            "error" => {
                let kind = match value["error"]["type"].as_str() {
                    Some("rate_limit_error") => ProviderErrorKind::RateLimited,
                    Some("authentication_error" | "permission_error") => ProviderErrorKind::Auth,
                    Some("billing_error" | "invalid_request_error" | "not_found_error") => {
                        ProviderErrorKind::Invalid
                    }
                    Some("request_too_large") => ProviderErrorKind::ContextOverflow,
                    _ => ProviderErrorKind::Server,
                };
                let retryable = matches!(
                    kind,
                    ProviderErrorKind::Server | ProviderErrorKind::RateLimited
                ) && !self.observable_emitted;
                out.push(StreamDelta::Error(ProviderError {
                    kind,
                    message: "Messages stream error".into(),
                    retryable,
                }));
            }
            _ => {}
        }
        Ok(out)
    }
    fn stop_reason_error(&self, stop_reason: Option<&str>) -> Option<ProviderError> {
        let (kind, message) = match stop_reason? {
            "refusal" => (ProviderErrorKind::Invalid, "Messages refusal"),
            "max_tokens"
                if self.saw_tool_use || !self.saw_text || !self.unsigned_thinking.is_empty() =>
            {
                (ProviderErrorKind::Invalid, "Messages output truncated")
            }
            "model_context_window_exceeded" => (
                ProviderErrorKind::ContextOverflow,
                "Messages context window exceeded",
            ),
            _ => return None,
        };
        Some(ProviderError {
            kind,
            message: message.into(),
            retryable: false,
        })
    }
    fn start_block(
        &mut self,
        value: &Value,
        out: &mut Vec<StreamDelta>,
    ) -> Result<(), ProviderError> {
        let block = &value["content_block"];
        if matches!(block["type"].as_str(), Some("text" | "tool_use")) {
            self.observable_emitted = true;
        }
        if block["type"] == "thinking" {
            let index = value["index"]
                .as_u64()
                .ok_or_else(|| invalid("missing thinking index"))?;
            if !self.seen_thinking.insert(index) {
                return Err(invalid("duplicate thinking block"));
            }
            self.thinking.insert(index);
            self.unsigned_thinking.insert(index);
            out.push(StreamDelta::Reasoning {
                item_id: thinking_id(index),
                provenance: crabber_core::ReasoningProvenance::Private,
                text: block["thinking"].as_str().unwrap_or_default().into(),
            });
        } else if block["type"] == "redacted_thinking" {
            out.push(StreamDelta::ProviderState {
                codec_id: "crabber.anthropic.redacted-thinking/v1".into(),
                payload: block.clone(),
            });
        }
        if block["type"] == "tool_use" {
            let id = block["id"]
                .as_str()
                .ok_or_else(|| invalid("missing tool id"))?;
            let index = value["index"].as_u64().unwrap_or(0);
            self.saw_tool_use = true;
            self.calls.insert(index, ToolCallId(id.into()));
            self.initial_input.insert(index, block["input"].clone());
            out.push(StreamDelta::ToolCallStart {
                call_id: ToolCallId(id.into()),
                name: block["name"].as_str().unwrap_or_default().into(),
            });
        }
        Ok(())
    }
    fn delta_block(
        &mut self,
        value: &Value,
        out: &mut Vec<StreamDelta>,
    ) -> Result<(), ProviderError> {
        let delta = &value["delta"];
        let kind = delta["type"].as_str().unwrap_or_default();
        if matches!(kind, "text_delta" | "input_json_delta") {
            self.observable_emitted = true;
        }
        match kind {
            "text_delta" => {
                let text = delta["text"].as_str().unwrap_or_default();
                self.saw_text |= !text.is_empty();
                out.push(StreamDelta::TextDelta(text.into()));
            }
            "thinking_delta" => {
                let index = self.thinking_index(value)?;
                out.push(StreamDelta::Reasoning {
                    item_id: thinking_id(index),
                    provenance: crabber_core::ReasoningProvenance::Private,
                    text: delta["thinking"].as_str().unwrap_or_default().into(),
                });
            }
            "input_json_delta" => {
                let index = value["index"].as_u64().unwrap_or(0);
                let id = self
                    .calls
                    .get(&index)
                    .ok_or_else(|| invalid("tool args without start"))?;
                self.streamed_input.insert(index);
                out.push(StreamDelta::ToolCallArgsDelta {
                    call_id: id.clone(),
                    text: delta["partial_json"].as_str().unwrap_or_default().into(),
                });
            }
            "signature_delta" => {
                let index = self.thinking_index(value)?;
                let signature = delta["signature"]
                    .as_str()
                    .ok_or_else(|| invalid("invalid thinking signature"))?;
                if !signature.is_empty() {
                    self.unsigned_thinking.remove(&index);
                }
                out.push(StreamDelta::ReasoningSignature {
                    item_id: thinking_id(index),
                    signature: signature.into(),
                });
            }
            _ => {}
        }
        Ok(())
    }
    fn stop_block(&mut self, value: &Value, out: &mut Vec<StreamDelta>) {
        let index = value["index"].as_u64().unwrap_or(0);
        self.thinking.remove(&index);
        if let Some(id) = self.calls.remove(&index) {
            let initial = self
                .initial_input
                .remove(&index)
                .unwrap_or_else(|| json!({}));
            if !self.streamed_input.remove(&index) {
                out.push(StreamDelta::ToolCallArgsDelta {
                    call_id: id.clone(),
                    text: if initial.is_null() {
                        "{}".into()
                    } else {
                        initial.to_string()
                    },
                });
            }
            out.push(StreamDelta::ToolCallDone { call_id: id });
        }
    }
    fn thinking_index(&self, value: &Value) -> Result<u64, ProviderError> {
        let index = value["index"]
            .as_u64()
            .ok_or_else(|| invalid("missing thinking index"))?;
        self.thinking
            .contains(&index)
            .then_some(index)
            .ok_or_else(|| invalid("thinking delta without start"))
    }
}
fn thinking_id(index: u64) -> String {
    format!("anthropic:{index}")
}
fn invalid(message: &str) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Invalid,
        message: message.into(),
        retryable: false,
    }
}
