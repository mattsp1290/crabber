use ag_ui_core::event::Event;
use serde::Serialize;
use std::io::{self, Write};

use crate::ProjectionError;

struct Counter {
    bytes: usize,
    limit: usize,
}
impl Write for Counter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buf.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| io::Error::other("projection limit"))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn json_bytes(value: &impl Serialize, limit: usize) -> Result<usize, ProjectionError> {
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value).map_err(|_| ProjectionError::Limit)?;
    Ok(counter.bytes)
}

/// Encode one compact UTF-8 SSE frame without a replay ID or sentinel.
///
/// # Errors
/// Returns `Limit` before allocating a frame exceeding `max_json_bytes`.
pub fn encode_sse(event: &Event, max_json_bytes: usize) -> Result<Vec<u8>, ProjectionError> {
    let bytes = sse_frame_len(event, max_json_bytes)?;
    let mut frame = Vec::with_capacity(bytes);
    frame.extend_from_slice(b"data: ");
    serde_json::to_writer(&mut frame, event).map_err(|_| ProjectionError::Malformed)?;
    frame.extend_from_slice(b"\n\n");
    Ok(frame)
}

/// Count an encoded frame before allocation, including its SSE delimiters.
///
/// # Errors
/// Returns `Limit` if JSON exceeds the limit or the frame length overflows.
pub fn sse_frame_len(event: &Event, max_json_bytes: usize) -> Result<usize, ProjectionError> {
    json_bytes(event, max_json_bytes)?
        .checked_add(8)
        .ok_or(ProjectionError::Limit)
}
