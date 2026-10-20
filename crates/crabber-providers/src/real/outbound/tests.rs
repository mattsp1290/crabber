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

pub(super) struct Credentials(pub(super) AtomicUsize);
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
pub(super) fn request(content: ContentBlock, nested: bool) -> ModelRequest {
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
async fn incomplete_signed_thinking_refuses_before_credentials_or_network() {
    let credentials = Arc::new(Credentials(AtomicUsize::new(0)));
    let adapter = HttpAdapter::custom("fixture", "http://127.0.0.1:1", Protocol::Messages)
        .with_credential_source(credentials.clone());
    let mut request = request(
        ContentBlock::Reasoning {
            text: "PRIVATE_THINKING".into(),
            item_id: Some("anthropic:0".into()),
            provenance: crabber_core::ReasoningProvenance::Private,
            provider_state: None,
        },
        false,
    );
    request.messages[0].role = Role::Assistant;
    let error = adapter.stream(request).await.err().unwrap();
    assert_eq!(error.kind, ProviderErrorKind::Invalid);
    assert!(!format!("{error:?}").contains("PRIVATE"));
    assert_eq!(credentials.0.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn unsupported_media_is_rejected_before_credentials_or_network() {
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
                let request = request(block.clone(), nested);
                let supported = matches!(
                    &block,
                    ContentBlock::MediaContent {
                        kind: MediaKind::Image,
                        ..
                    }
                ) && match protocol {
                    Protocol::Responses => true,
                    Protocol::ChatCompletions => {
                        !nested
                            && !matches!(
                                &block,
                                ContentBlock::MediaContent {
                                    source: MediaSource::File { .. },
                                    ..
                                }
                            )
                    }
                    Protocol::Messages => false,
                };
                if supported {
                    continue;
                }
                let Err(error) = adapter.stream(request).await else {
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
