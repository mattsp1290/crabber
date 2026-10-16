//! Shared public conversation redaction, including imported history.
use crabber_core::{ContentBlock, Message, PartKind};

fn visible(block: &ContentBlock) -> bool {
    !matches!(
        block,
        ContentBlock::ProviderState { .. } | ContentBlock::Reasoning { .. }
    )
}
fn redact(block: &mut ContentBlock) {
    if let ContentBlock::ToolResult { content, .. } = block {
        content.retain(visible);
        for child in content {
            redact(child);
        }
    }
}
pub(crate) fn redact_message(message: &mut Message) {
    message.parts.retain(|part| {
        !matches!(part.kind, PartKind::ProviderState | PartKind::Reasoning)
            && visible(&part.content)
    });
    for part in &mut message.parts {
        redact(&mut part.content);
    }
}
pub(crate) fn redact_result(result: &mut crabber_core::ToolResult) {
    result.content.retain(visible);
    for block in &mut result.content {
        redact(block);
    }
}
