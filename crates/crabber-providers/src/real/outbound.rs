//! Reject unsupported content before credentials, hooks or HTTP can observe it.
use crate::{ModelRequest, ProviderError, ProviderErrorKind};
use crabber_core::ContentBlock;

const IMAGE_MIMES: &[&str] = &["image/png", "image/jpeg", "image/webp", "image/gif"];
const fn image(source: super::MediaInputSource, tool_results: bool) -> super::HttpMediaSupport {
    super::HttpMediaSupport {
        kind: crabber_core::MediaKind::Image,
        source,
        mime_types: IMAGE_MIMES,
        message_roles: &[crabber_core::Role::User],
        tool_results,
    }
}
const RESPONSE_MEDIA: &[super::HttpMediaSupport] = &[
    image(super::MediaInputSource::Data, true),
    image(super::MediaInputSource::Url, true),
    image(super::MediaInputSource::OpaqueFile, true),
];
const MESSAGES_MEDIA: &[super::HttpMediaSupport] = &[
    image(super::MediaInputSource::Data, true),
    image(super::MediaInputSource::Url, true),
    image(super::MediaInputSource::OpaqueFile, true),
];
const CHAT_MEDIA: &[super::HttpMediaSupport] = &[
    image(super::MediaInputSource::Data, false),
    image(super::MediaInputSource::Url, false),
];
pub(super) fn media_support(protocol: super::Protocol) -> &'static [super::HttpMediaSupport] {
    use super::Protocol;
    match protocol {
        Protocol::Responses => RESPONSE_MEDIA,
        Protocol::ChatCompletions => CHAT_MEDIA,
        Protocol::Messages => MESSAGES_MEDIA,
    }
}
fn invalid_media() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Invalid,
        message: "unsupported outbound media representation".into(),
        retryable: false,
    }
}
fn validate_reasoning(
    protocol: super::Protocol,
    block: &ContentBlock,
) -> Result<(), ProviderError> {
    if let ContentBlock::Reasoning {
        item_id: Some(item),
        provenance: crabber_core::ReasoningProvenance::Private,
        provider_state,
        ..
    } = block
        && protocol == super::Protocol::Messages
        && item.starts_with("anthropic:")
        && !provider_state.as_ref().is_some_and(|state| {
            state["codec_id"] == "crabber.anthropic.thinking/v1"
                && state["signature"]
                    .as_str()
                    .is_some_and(|signature| !signature.is_empty())
        })
    {
        return Err(ProviderError {
            kind: ProviderErrorKind::Invalid,
            message: "incomplete signed thinking continuity".into(),
            retryable: false,
        });
    }
    Ok(())
}
pub(super) fn validate(
    protocol: super::Protocol,
    request: &ModelRequest,
) -> Result<(), ProviderError> {
    use crabber_core::{MediaSource, MessageContent, ToolOutput};
    for message in &request.messages {
        if message
            .parts
            .iter()
            .any(|part| matches!(part.content, ContentBlock::MediaContent { .. }))
            && message.parts.iter().any(|part| {
                !matches!(
                    part.content,
                    ContentBlock::Text { .. } | ContentBlock::MediaContent { .. }
                )
            })
        {
            return Err(invalid_media());
        }
        let mut pending: Vec<_> = message
            .parts
            .iter()
            .map(|part| (&part.content, false))
            .collect();
        while let Some((block, in_result)) = pending.pop() {
            validate_reasoning(protocol, block)?;
            match block {
                ContentBlock::MediaContent {
                    kind,
                    mime_type,
                    source,
                } => {
                    let source_kind = match source {
                        MediaSource::Data { base64 }
                            if base64.len() <= crabber_core::input::MAX_INLINE_MEDIA_BYTES * 2 =>
                        {
                            super::MediaInputSource::Data
                        }
                        MediaSource::Url { url } if url.len() <= 4096 => {
                            super::MediaInputSource::Url
                        }
                        MediaSource::File { file_id } if file_id.len() <= 512 => {
                            super::MediaInputSource::OpaqueFile
                        }
                        _ => return Err(invalid_media()),
                    };
                    if !media_support(protocol).iter().any(|support| {
                        support.kind == *kind
                            && support.source == source_kind
                            && support.mime_types.contains(&mime_type.as_str())
                            && if in_result {
                                support.tool_results
                            } else {
                                support.message_roles.contains(&message.role)
                            }
                    }) {
                        return Err(invalid_media());
                    }
                    // Validate explicit source syntax and bounds before any I/O.
                    ToolOutput::new(
                        vec![MessageContent::Media {
                            kind: *kind,
                            mime_type: mime_type.clone(),
                            source: source.clone(),
                        }],
                        false,
                    )
                    .map_err(|_| invalid_media())?;
                }
                ContentBlock::ToolResult { content, .. } if !in_result => {
                    pending.extend(content.iter().map(|block| (block, true)));
                }
                ContentBlock::Media { .. } | ContentBlock::ToolResult { .. } => {
                    return Err(invalid_media());
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Called only after strict outbound preflight has validated the representation.
pub(crate) fn response_image(block: &ContentBlock) -> Option<serde_json::Value> {
    let ContentBlock::MediaContent {
        kind: crabber_core::MediaKind::Image,
        mime_type,
        source,
    } = block
    else {
        return None;
    };
    use crabber_core::MediaSource;
    Some(match source {
        MediaSource::Data { base64 } => {
            serde_json::json!({"type":"input_image","detail":"auto","image_url":format!("data:{mime_type};base64,{base64}")})
        }
        MediaSource::Url { url } => {
            serde_json::json!({"type":"input_image","detail":"auto","image_url":url})
        }
        MediaSource::File { file_id } => {
            serde_json::json!({"type":"input_image","detail":"auto","file_id":file_id})
        }
    })
}
pub(crate) fn chat_image(block: &ContentBlock) -> Option<serde_json::Value> {
    let mut image = response_image(block)?;
    let url = image.as_object_mut()?.remove("image_url")?;
    Some(serde_json::json!({"type":"image_url","image_url":{"url":url}}))
}

pub(crate) fn messages_image(block: &ContentBlock) -> Option<serde_json::Value> {
    let ContentBlock::MediaContent {
        kind: crabber_core::MediaKind::Image,
        mime_type,
        source,
    } = block
    else {
        return None;
    };
    use crabber_core::MediaSource;
    let source = match source {
        MediaSource::Data { base64 } => {
            serde_json::json!({"type":"base64","media_type":mime_type,"data":base64})
        }
        MediaSource::Url { url } => serde_json::json!({"type":"url","url":url}),
        MediaSource::File { file_id } => serde_json::json!({"type":"file","file_id":file_id}),
    };
    Some(serde_json::json!({"type":"image","source":source}))
}

/// Prepare every fallible representation before credential/header callbacks.
pub(super) fn prepare(
    adapter: &super::HttpAdapter,
    request: &ModelRequest,
) -> Result<serde_json::Value, ProviderError> {
    validate(adapter.protocol, request)?;
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
mod tests;

#[cfg(all(test, feature = "custom-http"))]
mod image_rejections;
