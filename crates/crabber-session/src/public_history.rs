//! Public conversation projection with conservative raw-read accounting.
use crate::{SnapshotContinuation, SnapshotLimit, SnapshotPage, SnapshotUsage};
use crabber_core::{ContentBlock, EventCursor, Message, PartKind, SessionRevision, ToolCallRecord};
use serde::{Deserialize, Serialize};

/// A bounded history page safe for ordinary public conversation display.
/// Provider state and reasoning are omitted at every content depth. IDs and
/// relationships survive projection, including messages whose parts become empty.
/// This history-only projection is not an aggregate session/control snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicHistoryPage {
    pub captured_revision: SessionRevision,
    pub high_water: EventCursor,
    pub messages: Vec<Message>,
    pub tool_calls: Vec<ToolCallRecord>,
    /// Cost of the bounded raw read, before redaction. Private records still
    /// consume this budget; projection never bypasses backend allocation caps.
    pub read_usage: SnapshotUsage,
    pub continuation: Option<SnapshotContinuation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PublicHistoryOutcome {
    Page(PublicHistoryPage),
    Limited {
        high_water: EventCursor,
        limit: SnapshotLimit,
        continuation: SnapshotContinuation,
    },
    Invalidated {
        high_water: EventCursor,
    },
}

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
impl From<SnapshotPage> for PublicHistoryPage {
    fn from(mut page: SnapshotPage) -> Self {
        for message in &mut page.messages {
            message.parts.retain(|part| {
                !matches!(part.kind, PartKind::ProviderState | PartKind::Reasoning)
                    && visible(&part.content)
            });
            for part in &mut message.parts {
                redact(&mut part.content);
            }
        }
        for call in &mut page.tool_calls {
            if let Some(result) = &mut call.result {
                result.content.retain(visible);
                for block in &mut result.content {
                    redact(block);
                }
            }
        }
        Self {
            captured_revision: page.captured_revision,
            high_water: page.high_water,
            messages: page.messages,
            tool_calls: page.tool_calls,
            read_usage: page.usage,
            continuation: page.continuation,
        }
    }
}
impl From<crate::SnapshotOutcome> for PublicHistoryOutcome {
    fn from(outcome: crate::SnapshotOutcome) -> Self {
        match outcome {
            crate::SnapshotOutcome::Page(page) => Self::Page(page.into()),
            crate::SnapshotOutcome::Limited {
                high_water,
                limit,
                continuation,
            } => Self::Limited {
                high_water,
                limit,
                continuation,
            },
            crate::SnapshotOutcome::Invalidated { high_water } => Self::Invalidated { high_water },
        }
    }
}

#[cfg(test)]
#[path = "public_history/tests.rs"]
pub(crate) mod tests;
