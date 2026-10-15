use ag_ui_core::event::Event;
use ag_ui_encoder::{EncodeError, Encoder, Limits};
use serde::Serialize;
use std::io::{self, Write};

use crate::ProjectionError;

struct Counter {
    bytes: usize,
    limit: usize,
    exceeded: bool,
}
impl Write for Counter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > self.limit.saturating_sub(self.bytes) {
            self.exceeded = true;
            return Err(io::Error::other("projection limit"));
        }
        self.bytes += buf.len();
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn json_bytes(value: &impl Serialize, limit: usize) -> Result<usize, ProjectionError> {
    let mut counter = Counter {
        bytes: 0,
        limit,
        exceeded: false,
    };
    serde_json::to_writer(&mut counter, value).map_err(|_| {
        if counter.exceeded {
            ProjectionError::Limit
        } else {
            ProjectionError::Malformed
        }
    })?;
    Ok(counter.bytes)
}

/// Preflight a typed event without allocating a normalized event or output frame.
///
/// The returned conservative byte budget includes SSE framing. Normalization may
/// omit optional nulls, so use the encoded frame's length for queue accounting.
/// Live `RUN_STARTED` version insertion is covered even when the input omits it.
///
/// # Errors
/// Returns `Limit` for an oversized event, or `Malformed` for invalid serialization.
pub fn frame_budget(event: &Event, max_event_bytes: usize) -> Result<usize, ProjectionError> {
    let version_bytes = match event {
        Event::RunStarted(start) if start.protocol_version.is_none() => {
            ",\"protocolVersion\":\"1.0\"".len()
        }
        _ => 0,
    };
    let overhead = 8 + version_bytes;
    let json_limit = max_event_bytes
        .checked_sub(overhead)
        .ok_or(ProjectionError::Limit)?;
    json_bytes(event, json_limit)?
        .checked_add(overhead)
        .ok_or(ProjectionError::Limit)
}

fn map_error(error: &EncodeError) -> ProjectionError {
    match error {
        EncodeError::SizeLimit { .. } => ProjectionError::Limit,
        EncodeError::InvalidEvent(_) | EncodeError::Serialization => ProjectionError::Malformed,
    }
}

fn checked_encode(
    event: &Event,
    limit: usize,
    encode: impl FnOnce(&Encoder, &Event) -> Result<Vec<u8>, EncodeError>,
) -> Result<Vec<u8>, ProjectionError> {
    frame_budget(event, limit)?;
    let encoder = Encoder::new(Limits {
        max_event_bytes: limit,
        ..Limits::default()
    });
    encode(&encoder, event).map_err(|error| map_error(&error))
}

/// Encode a live UTF-8 SSE frame using the pinned SDK's producer normalization.
/// `RUN_STARTED` declares the SDK protocol version. No replay ID or sentinel is added.
///
/// # Errors
/// Returns `Limit` before normalization for an oversized typed event, or
/// `Malformed` for a permanent SDK validation/serialization fault.
pub fn encode_sse(event: &Event, max_event_bytes: usize) -> Result<Vec<u8>, ProjectionError> {
    checked_encode(event, max_event_bytes, Encoder::encode_live)
}

/// Encode a recorded event, preserving its explicit protocol version or omission.
///
/// # Errors
/// Uses the same preflight limits and permanent faults as `encode_sse`.
pub fn encode_sse_recorded(
    event: &Event,
    max_event_bytes: usize,
) -> Result<Vec<u8>, ProjectionError> {
    checked_encode(event, max_event_bytes, Encoder::encode)
}

/// Return the live frame's exact size. This wrapper allocates and normalizes.
/// Prefer encoding once and accounting with `frame.len()` on delivery paths.
///
/// # Errors
/// Returns the same errors as `encode_sse`.
pub fn sse_frame_len(event: &Event, max_event_bytes: usize) -> Result<usize, ProjectionError> {
    encode_sse(event, max_event_bytes).map(|frame| frame.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cell::Cell;

    #[test]
    fn oversized_snapshots_never_enter_sdk_normalization() {
        let text = "x".repeat(8192);
        for value in [
            json!({"type":"STATE_SNAPSHOT", "snapshot":{"text":text}}),
            json!({"type":"MESSAGES_SNAPSHOT", "messages":[{"id":"m", "role":"user", "content":text}]}),
            json!({"type":"ACTIVITY_SNAPSHOT", "messageId":"a", "activityType":"work", "content":{"text":text}}),
        ] {
            let event: Event = serde_json::from_value(value).unwrap();
            let entered = Cell::new(0);
            let result = checked_encode(&event, 1024, |encoder, event| {
                entered.set(entered.get() + 1);
                encoder.encode_live(event)
            });
            assert_eq!(result, Err(ProjectionError::Limit));
            assert_eq!(entered.get(), 0, "oversize must fail before SDK allocation");
            checked_encode(&event, 16384, |encoder, event| {
                entered.set(entered.get() + 1);
                encoder.encode_live(event)
            })
            .unwrap();
            assert_eq!(entered.get(), 1, "fitting event must enter exactly once");
        }
    }
}
