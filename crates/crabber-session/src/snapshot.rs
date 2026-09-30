//! Bounded, all-history reads independent of runtime context projection.
use crabber_core::{ContentBlock, EventCursor, Message, SessionId, ToolCallRecord};
use serde::{Deserialize, Serialize};

/// Per-page caps. Zero is permitted and produces a limit if a record needs it.
/// Bytes count UTF-8 text/reasoning in content blocks, and compact serde JSON for
/// each complete message/tool record (including IDs, metadata and nested values).
/// Envelope/token bytes are excluded; token sizes are backend-bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotLimits {
    pub messages: usize,
    pub tool_calls: usize,
    pub parts: usize,
    pub text_bytes: usize,
    pub encoded_bytes: usize,
}

/// Backend-specific opaque continuation, portable across processes only for
/// durable backends. Treat as untrusted input. Backends must authenticate or
/// validate its boundary and cap its size before decoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotContinuation(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRequest {
    pub session_id: SessionId,
    pub limits: SnapshotLimits,
    pub continuation: Option<SnapshotContinuation>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotUsage {
    pub messages: usize,
    pub tool_calls: usize,
    pub parts: usize,
    pub text_bytes: usize,
    pub encoded_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotLimit {
    Messages,
    ToolCalls,
    Parts,
    TextBytes,
    EncodedBytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPage {
    /// Immutable inclusive boundary. Read events strictly after this cursor.
    /// Zero represents a snapshot preceding every durable event.
    pub high_water: EventCursor,
    pub messages: Vec<Message>,
    pub tool_calls: Vec<ToolCallRecord>,
    pub usage: SnapshotUsage,
    /// None means complete. Keep the original high-water until every page is read.
    pub continuation: Option<SnapshotContinuation>,
}

/// A limit is a normal result, distinct from a failed Store operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotOutcome {
    Page(SnapshotPage),
    /// The next indivisible record cannot fit an empty page. Retry with larger
    /// caps using this continuation; no record was skipped or cloned.
    Limited {
        high_water: EventCursor,
        limit: SnapshotLimit,
        continuation: SnapshotContinuation,
    },
    /// Captured data changed, or the backend conservatively invalidated after
    /// a session mutation. Discard all accumulated pages
    /// and restart without a continuation. Never combine old pages with a new
    /// boundary or begin event consumption from an incomplete snapshot.
    Invalidated {
        high_water: EventCursor,
    },
}

impl SnapshotUsage {
    pub(crate) fn exceeded(self, limits: SnapshotLimits) -> Option<SnapshotLimit> {
        if self.messages > limits.messages {
            Some(SnapshotLimit::Messages)
        } else if self.tool_calls > limits.tool_calls {
            Some(SnapshotLimit::ToolCalls)
        } else if self.parts > limits.parts {
            Some(SnapshotLimit::Parts)
        } else if self.text_bytes > limits.text_bytes {
            Some(SnapshotLimit::TextBytes)
        } else if self.encoded_bytes > limits.encoded_bytes {
            Some(SnapshotLimit::EncodedBytes)
        } else {
            None
        }
    }
}

pub(crate) fn text_bytes(block: &ContentBlock) -> usize {
    match block {
        ContentBlock::Text { text } | ContentBlock::Reasoning { text, .. } => text.len(),
        ContentBlock::ToolResult { content, .. } => content
            .iter()
            .fold(0usize, |n, b| n.saturating_add(text_bytes(b))),
        _ => 0,
    }
}

/// Serialization writes into a counter, never a history-sized byte vector.
/// Stop as soon as the available encoded-byte budget is exceeded.
pub(crate) fn encoded_bytes<T: Serialize>(value: &T, cap: usize) -> Option<usize> {
    struct Counter {
        bytes: usize,
        cap: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self.bytes.saturating_add(bytes.len());
            if self.bytes > self.cap {
                return Err(std::io::Error::other("snapshot byte cap"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, cap };
    serde_json::to_writer(&mut counter, value).ok()?;
    Some(counter.bytes)
}
