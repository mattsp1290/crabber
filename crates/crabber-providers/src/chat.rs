use crate::{ModelRequest, ProviderError, ProviderErrorKind, StreamDelta, sse::Event};
use crabber_core::{ContentBlock, Role, ToolCallId, Usage};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenMode {
    MaxTokens,
    #[cfg(feature = "custom-http")]
    MaxCompletionTokens,
}

pub fn body(request: &ModelRequest, token_mode: TokenMode) -> Value {
    let mut messages = Vec::new();
    if let Some(system) = &request.system {
        messages.push(json!({"role":"system","content":system}));
    }
    for message in &request.messages {
        if message
            .parts
            .iter()
            .any(|part| matches!(part.content, ContentBlock::MediaContent { .. }))
        {
            let content: Vec<_> = message
                .parts
                .iter()
                .filter_map(|part| match &part.content {
                    ContentBlock::Text { text } => Some(json!({"type":"text","text":text})),
                    block => crate::real::outbound::chat_image(block),
                })
                .collect();
            messages.push(json!({"role":"user","content":content}));
            continue;
        }
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
            let role = match message.role {
                Role::System => "system",
                Role::Assistant => "assistant",
                Role::User | Role::Tool => "user",
            };
            let mut entry = json!({"role":role,"content":text});
            if !calls.is_empty() {
                entry["tool_calls"] = json!(calls);
            }
            messages.push(entry);
        }
    }
    let tools: Vec<_> = request.tools.iter().map(|t| json!({"type":"function","function":{"name":t.name,"description":t.description,"parameters":t.parameters}})).collect();
    let mut body = json!({"model":request.selection.model_id,"messages":messages,"stream":true,"stream_options":{"include_usage":true}});
    if !tools.is_empty() {
        body["tools"] = json!(tools);
        if let Some(tool_choice) = &request.tool_choice {
            body["tool_choice"] = json!(tool_choice);
        }
    }
    if let Some(temperature) = request.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(reasoning_effort) = request.reasoning_effort {
        body["reasoning_effort"] = json!(reasoning_effort.as_str());
    }
    if let Some(max_tokens) = request.max_tokens {
        match token_mode {
            TokenMode::MaxTokens => body["max_tokens"] = json!(max_tokens),
            #[cfg(feature = "custom-http")]
            TokenMode::MaxCompletionTokens => {
                body["max_completion_tokens"] = json!(max_tokens);
            }
        }
    }
    body
}
#[derive(Default)]
pub struct Codec {
    ids: HashMap<u64, ToolCallId>,
}

fn invalid_stream_error() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Invalid,
        message: "invalid Chat stream error".into(),
        retryable: false,
    }
}

#[derive(Deserialize)]
struct ChatStreamErrorEnvelope {
    error: ChatStreamErrorWire,
}

#[derive(Deserialize)]
struct ChatStreamErrorWire {
    code: serde_json::Number,
    #[serde(rename = "message")]
    _message: String,
    #[serde(default, deserialize_with = "deserialize_metadata")]
    metadata: Option<ChatStreamErrorMetadata>,
}

#[derive(Deserialize)]
struct ChatStreamErrorMetadata {
    error_type: Option<String>,
}

fn deserialize_metadata<'de, D>(
    deserializer: D,
) -> Result<Option<ChatStreamErrorMetadata>, D::Error>
where
    D: Deserializer<'de>,
{
    ChatStreamErrorMetadata::deserialize(deserializer).map(Some)
}

#[derive(Clone, Copy)]
enum ChatStreamErrorCode {
    Signed(i64),
    Unsigned(u64),
}

impl ChatStreamErrorCode {
    fn parse(number: &serde_json::Number) -> Option<Self> {
        number
            .as_u64()
            .map(Self::Unsigned)
            .or_else(|| number.as_i64().map(Self::Signed))
    }

    fn unsigned(self) -> Option<u64> {
        match self {
            Self::Unsigned(value) => Some(value),
            Self::Signed(value) => u64::try_from(value).ok(),
        }
    }
}

fn stream_error(value: Value) -> ProviderError {
    let Ok(envelope) = serde_json::from_value::<ChatStreamErrorEnvelope>(value) else {
        return invalid_stream_error();
    };
    let Some(code) = ChatStreamErrorCode::parse(&envelope.error.code) else {
        return invalid_stream_error();
    };
    let error_type = envelope
        .error
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.error_type.as_deref());
    let (kind, retryable) = classify_stream_error(error_type, code);
    ProviderError {
        kind,
        message: "Chat stream error".into(),
        retryable,
    }
}

fn classify_stream_error(
    error_type: Option<&str>,
    code: ChatStreamErrorCode,
) -> (ProviderErrorKind, bool) {
    match error_type {
        Some("rate_limit_exceeded") => (ProviderErrorKind::RateLimited, true),
        Some("authentication" | "permission_denied") => (ProviderErrorKind::Auth, false),
        Some("context_length_exceeded") => (ProviderErrorKind::ContextOverflow, false),
        Some("provider_overloaded" | "provider_unavailable") => (ProviderErrorKind::Server, true),
        Some("invalid_request") => (ProviderErrorKind::Invalid, false),
        _ => match code.unsigned() {
            Some(429) => (ProviderErrorKind::RateLimited, true),
            Some(401 | 403) => (ProviderErrorKind::Auth, false),
            Some(400 | 402 | 404 | 408 | 413 | 422) => (ProviderErrorKind::Invalid, false),
            _ => (ProviderErrorKind::Server, true),
        },
    }
}

impl Codec {
    pub fn event(&mut self, event: &Event) -> Result<Vec<StreamDelta>, ProviderError> {
        let named_error = event.name == "error";
        if event.data == "[DONE]" {
            return if named_error {
                Ok(vec![StreamDelta::Error(invalid_stream_error())])
            } else {
                Ok(vec![StreamDelta::Completed])
            };
        }
        let value: Value = match serde_json::from_str(&event.data) {
            Ok(value) => value,
            Err(_) if named_error => {
                return Ok(vec![StreamDelta::Error(invalid_stream_error())]);
            }
            Err(_) => {
                return Err(ProviderError {
                    kind: ProviderErrorKind::Invalid,
                    message: "invalid Chat event".into(),
                    retryable: false,
                });
            }
        };
        let object = value.as_object();
        if named_error || object.is_some_and(|object| object.contains_key("error")) {
            return Ok(vec![StreamDelta::Error(stream_error(value))]);
        }
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
        for key in ["reasoning_content", "reasoning"] {
            if let Some(text) = delta[key].as_str() {
                // Chat-compatible vendor fields carry no public-summary contract.
                out.push(StreamDelta::ReasoningDelta(text.into()));
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RequestIdentity, Selection};
    use crabber_core::{RunId, SessionId, ToolInfo, TurnId};

    fn request() -> ModelRequest {
        ModelRequest {
            context: Vec::new(),
            identity: RequestIdentity {
                session_id: SessionId::from("session"),
                run_id: RunId::from("run"),
                turn_id: TurnId::from("turn"),
            },
            selection: Selection {
                provider_id: "custom".into(),
                model_id: "model".into(),
            },
            system: None,
            messages: vec![],
            tools: vec![],
            temperature: None,
            max_tokens: None,
            reasoning_effort: None,
            tool_choice: Some("required".into()),
        }
    }

    #[test]
    fn empty_tools_omit_tools_and_tool_choice() {
        let value = body(&request(), TokenMode::MaxTokens);
        assert!(value.get("tools").is_none());
        assert!(value.get("tool_choice").is_none());
    }

    #[test]
    fn nonempty_tools_preserve_schema_and_optional_choice() {
        let mut request = request();
        request.tools.push(ToolInfo {
            name: "echo".into(),
            description: "Echo input".into(),
            parameters: json!({"type":"object"}),
            retry_safe: true,
            required_permissions: vec![],
        });
        let value = body(&request, TokenMode::MaxTokens);
        assert_eq!(value["tools"][0]["type"], "function");
        assert_eq!(value["tools"][0]["function"]["name"], "echo");
        assert_eq!(value["tool_choice"], "required");

        request.tool_choice = None;
        assert!(
            body(&request, TokenMode::MaxTokens)
                .get("tool_choice")
                .is_none()
        );
    }

    #[test]
    fn max_tokens_mode_emits_only_max_tokens() {
        let mut request = request();
        request.max_tokens = Some(42);
        let value = body(&request, TokenMode::MaxTokens);
        assert_eq!(value["max_tokens"], 42);
        assert!(value.get("max_completion_tokens").is_none());
    }

    #[test]
    #[cfg(feature = "custom-http")]
    fn max_completion_tokens_mode_and_none_are_exact() {
        let mut request = request();
        request.max_tokens = Some(42);
        let value = body(&request, TokenMode::MaxCompletionTokens);
        assert_eq!(value["max_completion_tokens"], 42);
        assert!(value.get("max_tokens").is_none());

        request.max_tokens = None;
        let value = body(&request, TokenMode::MaxCompletionTokens);
        assert!(value.get("max_completion_tokens").is_none());
        assert!(value.get("max_tokens").is_none());
    }
}
