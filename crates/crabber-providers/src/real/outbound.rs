//! Reject unsupported content before credentials, hooks or HTTP can observe it.
use crate::{ModelRequest, ProviderError, ProviderErrorKind};
use crabber_core::ContentBlock;

pub(super) fn validate(request: &ModelRequest) -> Result<(), ProviderError> {
    let mut pending: Vec<_> = request
        .messages
        .iter()
        .flat_map(|message| &message.parts)
        .map(|part| &part.content)
        .collect();
    while let Some(block) = pending.pop() {
        match block {
            ContentBlock::Media { .. } | ContentBlock::MediaContent { .. } => {
                return Err(ProviderError {
                    kind: ProviderErrorKind::Invalid,
                    message: "unsupported outbound media representation".into(),
                    retryable: false,
                });
            }
            ContentBlock::ToolResult { content, .. } => pending.extend(content),
            _ => {}
        }
    }
    Ok(())
}

/// Prepare every fallible representation before credential/header callbacks.
pub(super) fn prepare(
    adapter: &super::HttpAdapter,
    request: &ModelRequest,
) -> Result<serde_json::Value, ProviderError> {
    validate(request)?;
    let invalid = || ProviderError {
        kind: ProviderErrorKind::Invalid,
        message: "unsupported outbound input representation".into(),
        retryable: false,
    };
    if request
        .messages
        .iter()
        .any(|message| !adapter.input_support().roles.contains(&message.role))
    {
        return Err(invalid());
    }
    let mut contexts = Vec::new();
    let mut bytes: usize = 0;
    for context in &request.context {
        let text = context.canonical_text().map_err(|_| invalid())?;
        bytes = bytes.checked_add(text.len()).ok_or_else(invalid)?;
        if bytes > crabber_core::input::MAX_INPUT_BYTES {
            return Err(invalid());
        }
        contexts.push(text);
    }
    let mut body = match adapter.protocol {
        super::Protocol::Responses => {
            crate::responses::body(request, adapter.uses_codex_responses_mode())
        }
        super::Protocol::Messages => crate::messages::body(request),
        super::Protocol::ChatCompletions => crate::chat::body(request, adapter.chat_token_mode),
    };
    let field = if adapter.protocol == super::Protocol::Responses {
        "input"
    } else {
        "messages"
    };
    let messages = body
        .get_mut(field)
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(invalid)?;
    let offset = usize::from(
        adapter.protocol == super::Protocol::ChatCompletions && request.system.is_some(),
    );
    let context_messages = contexts.into_iter().map(|text| {
        if adapter.protocol == super::Protocol::Responses {
            serde_json::json!({"role":"user", "content":text})
        } else if adapter.protocol == super::Protocol::Messages {
            serde_json::json!({"role":"user", "content":[{"type":"text", "text":text}]})
        } else {
            serde_json::json!({"role":"user", "content":text})
        }
    });
    messages.splice(offset..offset, context_messages);
    Ok(body)
}

#[cfg(all(test, feature = "custom-http"))]
mod tests {
    use super::*;
    use crate::{CredentialSource, HttpAdapter, Protocol, RequestIdentity, Selection, Streamer};
    use crabber_core::{
        MediaKind, MediaSource, Message, MessageId, Part, PartId, PartKind, Role, RunId, SessionId,
        ToolCallId, TurnId,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Credentials(AtomicUsize);
    #[async_trait::async_trait]
    impl CredentialSource for Credentials {
        async fn credential(&self, _: &ModelRequest) -> Result<String, ProviderError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("unsupported media must be rejected before credentials")
        }
        async fn invalidate(&self, _: &str) {
            panic!("no credential was acquired")
        }
    }
    fn request(content: ContentBlock, nested: bool) -> ModelRequest {
        let id = MessageId::new();
        let content = if nested {
            ContentBlock::ToolResult {
                call_id: ToolCallId::new(),
                content: vec![
                    ContentBlock::Text {
                        text: "partial".into(),
                    },
                    content,
                ],
                is_error: true,
            }
        } else {
            content
        };
        ModelRequest {
            context: Vec::new(),
            identity: RequestIdentity {
                session_id: SessionId::new(),
                run_id: RunId::new(),
                turn_id: TurnId::new(),
            },
            selection: Selection {
                provider_id: "fixture".into(),
                model_id: "model".into(),
            },
            system: None,
            messages: vec![Message {
                id: id.clone(),
                session_id: SessionId::new(),
                run_id: None,
                role: if nested { Role::Tool } else { Role::User },
                parent_id: None,
                parts: vec![Part {
                    id: PartId::new(),
                    message_id: id,
                    ordinal: 0,
                    kind: PartKind::Custom {
                        custom_type: "fixture-media".into(),
                    },
                    content,
                }],
                created_at: std::time::UNIX_EPOCH.into(),
            }],
            tools: vec![],
            temperature: None,
            max_tokens: None,
            tool_choice: None,
        }
    }
    #[tokio::test]
    async fn every_protocol_rejects_media_and_tool_result_media_before_credentials_or_network() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let credentials = Arc::new(Credentials(AtomicUsize::new(0)));
        for protocol in [
            Protocol::Responses,
            Protocol::Messages,
            Protocol::ChatCompletions,
        ] {
            let adapter = HttpAdapter::custom("fixture", &endpoint, protocol)
                .with_credential_source(credentials.clone());
            let mut blocks = vec![ContentBlock::Media {
                uri: "PRIVATE_SENTINEL".into(),
                mime_type: "image/png".into(),
            }];
            for (kind, mime) in [
                (MediaKind::Image, "image/png"),
                (MediaKind::Audio, "audio/wav"),
                (MediaKind::Video, "video/mp4"),
                (MediaKind::Document, "application/pdf"),
            ] {
                for source in [
                    MediaSource::Data {
                        base64: "AQ==".into(),
                    },
                    MediaSource::Url {
                        url: "https://example.invalid/PRIVATE_SENTINEL".into(),
                    },
                    MediaSource::File {
                        file_id: "PRIVATE_SENTINEL".into(),
                    },
                ] {
                    blocks.push(ContentBlock::MediaContent {
                        kind,
                        mime_type: mime.into(),
                        source,
                    });
                }
            }
            for block in blocks {
                for nested in [false, true] {
                    let Err(error) = adapter.stream(request(block.clone(), nested)).await else {
                        panic!("unsupported media must not be silently discarded")
                    };
                    assert_eq!(error.kind, ProviderErrorKind::Invalid);
                    assert!(!error.retryable);
                    assert!(!error.to_string().contains("PRIVATE_SENTINEL"));
                    assert_eq!(credentials.0.load(Ordering::SeqCst), 0);
                    assert_eq!(
                        listener.accept().unwrap_err().kind(),
                        std::io::ErrorKind::WouldBlock
                    );
                }
            }
        }
    }
}
