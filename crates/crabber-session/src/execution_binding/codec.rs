use super::MAX_BINDING_BYTES;
use crate::StoreError;
use serde::Serialize;
use serde_json::Value;

pub(super) fn validate_depth(value: &Value, depth: usize) -> Result<(), StoreError> {
    if depth > 64 {
        return Err(StoreError::Limit(
            "execution binding configuration depth".into(),
        ));
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
pub(crate) fn encode_bounded(value: &impl Serialize) -> Result<String, StoreError> {
    struct Writer {
        bytes: Vec<u8>,
        exceeded: bool,
    }
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_BINDING_BYTES.saturating_sub(self.bytes.len()) {
                self.exceeded = true;
                return Err(std::io::Error::other("execution binding size"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        exceeded: false,
    };
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(if writer.exceeded {
            StoreError::Limit("execution binding capsule".into())
        } else {
            StoreError::AdmissionConflict
        });
    }
    String::from_utf8(writer.bytes).map_err(|_| StoreError::AdmissionConflict)
}
