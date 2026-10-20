//! Count borrowed opaque continuity before retaining it or cloning on interrupt.
use super::{RuntimeError, invalid_provider};
use serde_json::Value;
use std::io;

struct Bytes {
    written: usize,
    limit: usize,
}
impl io::Write for Bytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.written = self
            .written
            .checked_add(bytes.len())
            .filter(|written| *written <= self.limit)
            .ok_or_else(|| io::Error::other("reasoning limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn opaque_bytes(
    codec: &str,
    payload: &Value,
    limit: usize,
) -> Result<usize, RuntimeError> {
    validate_depth(payload, 0)?;
    let mut bytes = Bytes { written: 0, limit };
    serde_json::to_writer(&mut bytes, &(codec, payload))
        .map_err(|_| invalid_provider("reasoning limit"))?;
    Ok(bytes.written)
}

fn validate_depth(value: &Value, depth: usize) -> Result<(), RuntimeError> {
    if depth > 64 {
        return Err(invalid_provider("opaque reasoning depth limit").into());
    }
    match value {
        Value::Array(values) => {
            for value in values {
                validate_depth(value, depth + 1)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_depth(value, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}
