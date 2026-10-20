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
                ContentBlock::Text { text }
                    if matches!(message.role, Role::User | Role::Assistant) =>
                {
                    content.push(json!({"type":"text","text":text}))
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
                    let text = blocks
                        .iter()
                        .filter_map(|b| match b {
                            ContentBlock::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    content.push(json!({"type":"tool_result","tool_use_id":call_id.0,"content":text,"is_error":is_error}));
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
    let tools: Vec<_> = request
        .tools
        .iter()
        .map(|t| json!({"name":t.name,"description":t.description,"input_schema":t.parameters}))
        .collect();
    let mut body = json!({"model":request.selection.model_id,"messages":messages,"tools":tools,"max_tokens":4096,"stream":true});
    if let Some(temperature) = request.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(max_tokens) = request.max_tokens {
        body["max_tokens"] = json!(max_tokens);
    }
    if let Some(tool_choice) = &request.tool_choice {
        body["tool_choice"] = if tool_choice == "auto" || tool_choice == "any" {
            json!({"type":tool_choice})
        } else {
            json!({"type":"tool","name":tool_choice})
        };
    }
    if let Some(system) = &request.system {
        body["system"] = json!(system);
    }
    body
}
#[derive(Default)]
pub struct Codec {
    calls: HashMap<u64, ToolCallId>,
    initial_input: HashMap<u64, Value>,
    streamed_input: HashSet<u64>,
    usage: Usage,
    thinking: HashSet<u64>,
    seen_thinking: HashSet<u64>,
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
                self.usage.output_tokens = value["usage"]["output_tokens"].as_u64().unwrap_or(0)
            }
            "message_stop" => {
                out.push(StreamDelta::Usage(self.usage.clone()));
                out.push(StreamDelta::Completed);
            }
            "error" => out.push(StreamDelta::Error(ProviderError {
                kind: ProviderErrorKind::Server,
                message: "Messages stream error".into(),
                retryable: false,
            })),
            _ => {}
        }
        Ok(out)
    }
    fn start_block(
        &mut self,
        value: &Value,
        out: &mut Vec<StreamDelta>,
    ) -> Result<(), ProviderError> {
        let block = &value["content_block"];
        if block["type"] == "thinking" {
            let index = value["index"]
                .as_u64()
                .ok_or_else(|| invalid("missing thinking index"))?;
            if !self.seen_thinking.insert(index) {
                return Err(invalid("duplicate thinking block"));
            }
            self.thinking.insert(index);
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
        match delta["type"].as_str().unwrap_or_default() {
            "text_delta" => out.push(StreamDelta::TextDelta(
                delta["text"].as_str().unwrap_or_default().into(),
            )),
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
            "signature_delta" => out.push(StreamDelta::ReasoningSignature {
                item_id: thinking_id(self.thinking_index(value)?),
                signature: delta["signature"]
                    .as_str()
                    .ok_or_else(|| invalid("invalid thinking signature"))?
                    .into(),
            }),
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
