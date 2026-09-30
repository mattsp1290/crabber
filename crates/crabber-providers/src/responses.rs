use crate::{ModelRequest, ProviderError, ProviderErrorKind, StreamDelta, sse::Event};
use crabber_core::{ContentBlock, Role, ToolCallId, Usage};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

pub const CODEX_STATE: &str = "crabber.codex.reasoning-items/v1";

pub fn body(request: &ModelRequest, codex: bool) -> Value {
    let mut input = Vec::new();
    for message in &request.messages {
        for part in &message.parts {
            match &part.content {
                ContentBlock::Text { text } if matches!(message.role, Role::User | Role::Assistant) =>
                    input.push(json!({"role": if message.role == Role::User {"user"} else {"assistant"}, "content": text})),
                ContentBlock::ProviderState { codec_id, payload } if codex && codec_id == CODEX_STATE =>
                    input.push(payload.clone()),
                ContentBlock::ToolCall { call_id, name, arguments } =>
                    input.push(json!({"type":"function_call","call_id":call_id.0,"name":name,"arguments":arguments.to_string()})),
                ContentBlock::ToolResult { call_id, content, .. } => {
                    let output = content.iter().filter_map(|v| match v { ContentBlock::Text { text } => Some(text.as_str()), _ => None }).collect::<Vec<_>>().join("\n");
                    input.push(json!({"type":"function_call_output","call_id":call_id.0,"output":output}));
                }
                _ => {}
            }
        }
    }
    let tools: Vec<Value> = request.tools.iter().map(|tool| json!({"type":"function","name":tool.name,"description":tool.description,"parameters":tool.parameters,"strict":false})).collect();
    let mut body = json!({"model":request.selection.model_id,"input":input,"tools":tools,"tool_choice":"auto","parallel_tool_calls":false,"store":false,"stream":true});
    if let Some(system) = &request.system {
        body["instructions"] = json!(system);
    }
    if codex {
        body["reasoning"] = Value::Null;
        // Encrypted reasoning items are needed to preserve continuity across tool turns.
        body["include"] = json!(["reasoning.encrypted_content"]);
    }
    body
}

#[derive(Default)]
pub struct Codec {
    calls: HashMap<String, ToolCallId>,
    arguments_seen: HashSet<String>,
    codex: bool,
}
impl Codec {
    pub fn new(codex: bool) -> Self {
        Self {
            calls: HashMap::new(),
            arguments_seen: HashSet::new(),
            codex,
        }
    }
    #[allow(clippy::too_many_lines)] // Event variants share the same in-flight call table.
    pub fn event(&mut self, event: &Event) -> Result<Vec<StreamDelta>, ProviderError> {
        if event.data == "[DONE]" {
            return Ok(vec![]);
        }
        let value: Value =
            serde_json::from_str(&event.data).map_err(|_| invalid("invalid Responses event"))?;
        let kind = value["type"].as_str().unwrap_or(&event.name);
        let mut out = Vec::new();
        match kind {
            "response.output_text.delta" => {
                if let Some(s) = value["delta"].as_str() {
                    out.push(StreamDelta::TextDelta(s.into()));
                }
            }
            "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => {
                if let Some(s) = value["delta"].as_str() {
                    out.push(StreamDelta::ReasoningDelta(s.into()));
                }
            }
            "response.output_item.added" => {
                let item = &value["item"];
                if item["type"] == "function_call" {
                    let id = item["call_id"]
                        .as_str()
                        .ok_or_else(|| invalid("missing function call id"))?;
                    let key = value["output_index"].to_string();
                    self.calls.insert(key, ToolCallId(id.into()));
                    out.push(StreamDelta::ToolCallStart {
                        call_id: ToolCallId(id.into()),
                        name: item["name"].as_str().unwrap_or_default().into(),
                    });
                }
            }
            "response.function_call_arguments.delta" => {
                let key = value["output_index"].to_string();
                let id = self
                    .calls
                    .get(&key)
                    .ok_or_else(|| invalid("arguments without function call"))?;
                self.arguments_seen.insert(key);
                out.push(StreamDelta::ToolCallArgsDelta {
                    call_id: id.clone(),
                    text: value["delta"].as_str().unwrap_or_default().into(),
                });
            }
            "response.function_call_arguments.done" => {
                let key = value["output_index"].to_string();
                if !self.arguments_seen.contains(&key) {
                    let id = self
                        .calls
                        .get(&key)
                        .ok_or_else(|| invalid("arguments without function call"))?;
                    out.push(StreamDelta::ToolCallArgsDelta {
                        call_id: id.clone(),
                        text: value["arguments"].as_str().unwrap_or("{}").into(),
                    });
                    self.arguments_seen.insert(key);
                }
            }
            "response.output_item.done" => {
                let item = &value["item"];
                if item["type"] == "function_call" {
                    let key = value["output_index"].to_string();
                    if let Some(id) = self.calls.remove(&key) {
                        if !self.arguments_seen.remove(&key) {
                            out.push(StreamDelta::ToolCallArgsDelta {
                                call_id: id.clone(),
                                text: item["arguments"].as_str().unwrap_or("{}").into(),
                            });
                        }
                        out.push(StreamDelta::ToolCallDone { call_id: id });
                    }
                } else if self.codex
                    && item["type"] == "reasoning"
                    && item.get("encrypted_content").is_some()
                {
                    out.push(StreamDelta::ProviderState {
                        codec_id: CODEX_STATE.into(),
                        payload: item.clone(),
                    });
                }
            }
            "response.completed" => {
                let usage = &value["response"]["usage"];
                if usage.is_object() {
                    out.push(StreamDelta::Usage(Usage {
                        input_tokens: usage["input_tokens"].as_u64().unwrap_or(0),
                        output_tokens: usage["output_tokens"].as_u64().unwrap_or(0),
                    }));
                }
                out.push(StreamDelta::Completed);
            }
            "response.failed" | "response.incomplete" => {
                let code = value["response"]["error"]["code"]
                    .as_str()
                    .unwrap_or_default();
                out.push(StreamDelta::Error(ProviderError {
                    kind: if code.contains("context") {
                        ProviderErrorKind::ContextOverflow
                    } else {
                        ProviderErrorKind::Server
                    },
                    message: format!("Responses {kind}: {code}"),
                    retryable: false,
                }));
            }
            _ => {}
        }
        Ok(out)
    }
}
fn invalid(message: &str) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Invalid,
        message: message.into(),
        retryable: false,
    }
}
