use crate::{ModelRequest, ProviderError, ProviderErrorKind, StreamDelta, sse::Event};
use crabber_core::{ContentBlock, Role, ToolCallId, Usage};
use serde_json::{Value, json};
use std::collections::HashMap;

pub fn body(request: &ModelRequest) -> Value {
    let mut messages = Vec::new();
    if let Some(system) = &request.system {
        messages.push(json!({"role":"system","content":system}));
    }
    for message in &request.messages {
        let mut text = String::new();
        let mut calls = Vec::new();
        for part in &message.parts {
            match &part.content {
                ContentBlock::Text { text: part } => text.push_str(part),
                ContentBlock::ToolCall { call_id, name, arguments } => calls.push(json!({"id":call_id.0,"type":"function","function":{"name":name,"arguments":arguments.to_string()}})),
                ContentBlock::ToolResult { call_id, content, .. } => {
                    let result = content.iter().filter_map(|v| match v { ContentBlock::Text { text } => Some(text.as_str()), _ => None }).collect::<Vec<_>>().join("\n");
                    messages.push(json!({"role":"tool","tool_call_id":call_id.0,"content":result}));
                }
                _ => {}
            }
        }
        if message.role != Role::Tool && (!text.is_empty() || !calls.is_empty()) {
            let mut entry = json!({"role":if message.role == Role::Assistant {"assistant"} else {"user"},"content":text});
            if !calls.is_empty() {
                entry["tool_calls"] = json!(calls);
            }
            messages.push(entry);
        }
    }
    let tools: Vec<_> = request.tools.iter().map(|t| json!({"type":"function","function":{"name":t.name,"description":t.description,"parameters":t.parameters}})).collect();
    let mut body = json!({"model":request.selection.model_id,"messages":messages,"tools":tools,"stream":true,"stream_options":{"include_usage":true}});
    if let Some(temperature) = request.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(max_tokens) = request.max_tokens {
        body["max_tokens"] = json!(max_tokens);
    }
    if let Some(tool_choice) = &request.tool_choice {
        body["tool_choice"] = json!(tool_choice);
    }
    body
}
#[derive(Default)]
pub struct Codec {
    ids: HashMap<u64, ToolCallId>,
}
impl Codec {
    pub fn event(&mut self, event: &Event) -> Result<Vec<StreamDelta>, ProviderError> {
        if event.data == "[DONE]" {
            return Ok(vec![StreamDelta::Completed]);
        }
        let value: Value = serde_json::from_str(&event.data).map_err(|_| ProviderError {
            kind: ProviderErrorKind::Invalid,
            message: "invalid Chat event".into(),
            retryable: false,
        })?;
        let mut out = Vec::new();
        if let Some(usage) = value.get("usage").filter(|v| v.is_object()) {
            out.push(StreamDelta::Usage(Usage {
                input_tokens: usage["prompt_tokens"].as_u64().unwrap_or(0),
                output_tokens: usage["completion_tokens"].as_u64().unwrap_or(0),
            }));
        }
        let delta = &value["choices"][0]["delta"];
        if let Some(text) = delta["content"].as_str() {
            out.push(StreamDelta::TextDelta(text.into()));
        }
        if let Some(calls) = delta["tool_calls"].as_array() {
            for call in calls {
                let index = call["index"].as_u64().unwrap_or(0);
                if let Some(id) = call["id"].as_str() {
                    let id = ToolCallId(id.into());
                    self.ids.insert(index, id.clone());
                    out.push(StreamDelta::ToolCallStart {
                        call_id: id,
                        name: call["function"]["name"].as_str().unwrap_or_default().into(),
                    });
                }
                if let Some(args) = call["function"]["arguments"].as_str() {
                    let id = self.ids.get(&index).ok_or_else(|| ProviderError {
                        kind: ProviderErrorKind::Invalid,
                        message: "tool args without start".into(),
                        retryable: false,
                    })?;
                    out.push(StreamDelta::ToolCallArgsDelta {
                        call_id: id.clone(),
                        text: args.into(),
                    });
                }
            }
        }
        if value["choices"][0]["finish_reason"] == "tool_calls" {
            for (_, id) in self.ids.drain() {
                out.push(StreamDelta::ToolCallDone { call_id: id });
            }
        }
        Ok(out)
    }
}
