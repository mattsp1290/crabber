use super::WireInputError;
use ag_ui_core::types::{ContentPart, MessageContent as WireContent, PartSource};
use crabber_core::{InputContent, InputPart, MediaKind, MediaSource, MessageContent};

pub(super) fn parts(content: &WireContent) -> Result<Vec<InputPart>, WireInputError> {
    match content {
        WireContent::Text(text) => Ok(vec![InputPart {
            id: None,
            content: InputContent::Content {
                content: MessageContent::Text { text: text.clone() },
            },
        }]),
        WireContent::Parts(parts) => {
            if parts.len() > crabber_core::input::MAX_INPUT_PARTS {
                return Err(WireInputError::Limit);
            }
            parts.iter().map(part).collect()
        }
    }
}

fn part(part: &ContentPart) -> Result<InputPart, WireInputError> {
    let (id, content) = match part {
        ContentPart::Text { text, id, .. } => (id, MessageContent::Text { text: text.clone() }),
        ContentPart::Image { source, id, .. } => (id, media(MediaKind::Image, source)?),
        ContentPart::Audio { source, id, .. } => (id, media(MediaKind::Audio, source)?),
        ContentPart::Video { source, id, .. } => (id, media(MediaKind::Video, source)?),
        ContentPart::Document { source, id, .. } => (id, media(MediaKind::Document, source)?),
    };
    Ok(InputPart {
        id: id.clone(),
        content: InputContent::Content { content },
    })
}

fn media(kind: MediaKind, source: &PartSource) -> Result<MessageContent, WireInputError> {
    let (mime_type, source) = match source {
        PartSource::Data { value, mime_type } => (
            mime_type.clone(),
            MediaSource::Data {
                base64: value.clone(),
            },
        ),
        PartSource::Url { value, mime_type } => (
            mime_type.clone().ok_or(WireInputError::Unsupported)?,
            MediaSource::Url { url: value.clone() },
        ),
        PartSource::File {
            value,
            provider,
            mime_type,
        } => {
            // Core has no provider-qualified file reference. Dropping that hint
            // could send an opaque file ID to an unintended provider.
            if provider.is_some() {
                return Err(WireInputError::Unsupported);
            }
            (
                mime_type.clone().ok_or(WireInputError::Unsupported)?,
                MediaSource::File {
                    file_id: value.clone(),
                },
            )
        }
    };
    Ok(MessageContent::Media {
        kind,
        mime_type,
        source,
    })
}

pub(super) fn result_content(content: &WireContent) -> Result<Vec<MessageContent>, WireInputError> {
    parts(content)?
        .into_iter()
        .map(|part| {
            if part.id.is_some() {
                return Err(WireInputError::Unsupported);
            }
            match part.content {
                InputContent::Content { content } => Ok(content),
                _ => Err(WireInputError::Malformed),
            }
        })
        .collect()
}
