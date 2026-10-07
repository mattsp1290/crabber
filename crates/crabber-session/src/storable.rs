//! Validation shared by bundled stores before they persist a record.

use crate::StoreError;
use serde::Serialize;
use serde_json::Value;

const ENCODING_ERROR: &str = "record encoding failed";
const DEPTH_ERROR: &str = "record nesting is too deep";
const NUL_ERROR: &str = "record contains NUL";

/// Verifies that a record can be decoded by the JSON readers used by bundled
/// stores and contains no NUL character.
///
/// # Errors
///
/// Returns [`StoreError::Validation`] when encoding, reader depth, or NUL
/// validation fails.
pub fn ensure_storable<T: Serialize + ?Sized>(record: &T) -> Result<(), StoreError> {
    storable_value(record).map(|_| ())
}

/// Produces the checked JSON value used for a JSONB bind.
pub(crate) fn storable_value<T: Serialize + ?Sized>(record: &T) -> Result<Value, StoreError> {
    checked_record(record).map(|(_, value)| value)
}

/// Produces canonical JSON text with the same validation as JSONB binds.
#[cfg(feature = "sqlite")]
#[allow(dead_code)] // Removed in W3 when SqliteStore calls these.
pub(crate) fn storable_text<T: Serialize + ?Sized>(record: &T) -> Result<String, StoreError> {
    checked_record(record).map(|(text, _)| text)
}

fn checked_record<T: Serialize + ?Sized>(record: &T) -> Result<(String, Value), StoreError> {
    let text =
        serde_json::to_string(record).map_err(|_| StoreError::Validation(ENCODING_ERROR.into()))?;
    let value = serde_json::from_str::<Value>(&text).map_err(|error| {
        let message = error.to_string();
        StoreError::Validation(
            if message.starts_with("recursion limit exceeded") {
                DEPTH_ERROR
            } else {
                ENCODING_ERROR
            }
            .into(),
        )
    })?;
    ensure_value_has_no_nul(&value)?;
    Ok((text, value))
}

/// Verifies text destined for a relational text column.
///
/// # Errors
///
/// Returns [`StoreError::Validation`] when the text contains a NUL character.
pub fn ensure_storable_text(text: &str) -> Result<(), StoreError> {
    if text.contains('\0') {
        Err(StoreError::Validation(NUL_ERROR.into()))
    } else {
        Ok(())
    }
}

fn ensure_value_has_no_nul(value: &Value) -> Result<(), StoreError> {
    let mut values = vec![value];
    while let Some(value) = values.pop() {
        match value {
            Value::String(text) => ensure_storable_text(text)?,
            Value::Array(values_in_array) => values.extend(values_in_array),
            Value::Object(object) => {
                for (key, value) in object {
                    ensure_storable_text(key)?;
                    values.push(value);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matches_value_reader_depth_boundary() {
        let mut accepted = false;
        let mut rejected = false;
        for depth in 100..=140 {
            let mut value = json!(null);
            for _ in 0..depth {
                value = json!([value]);
            }
            let text = serde_json::to_string(&value).unwrap();
            let reader_accepts = serde_json::from_str::<Value>(&text).is_ok();
            assert_eq!(ensure_storable(&value).is_ok(), reader_accepts);
            #[cfg(feature = "sqlite")]
            assert_text_matches_validation(&value);
            accepted |= reader_accepts;
            rejected |= !reader_accepts;
        }
        assert!(accepted && rejected);
    }

    #[test]
    fn rejects_nul_without_rejecting_literal_escape() {
        for value in [json!("a\0b"), json!({"a\0b": 1}), json!(["\0"])] {
            #[cfg(feature = "sqlite")]
            assert_text_matches_validation(&value);
            assert!(matches!(
                ensure_storable(&value),
                Err(StoreError::Validation(_))
            ));
        }
        assert!(ensure_storable(&json!(r"\u0000")).is_ok());
        #[cfg(feature = "sqlite")]
        assert_text_matches_validation(&json!(r"\u0000"));
    }

    #[cfg(feature = "sqlite")]
    fn assert_text_matches_validation(value: &Value) {
        let text = storable_text(value);
        match ensure_storable(value) {
            Ok(()) => assert_eq!(text.unwrap(), serde_json::to_string(value).unwrap()),
            Err(error) => assert_eq!(text.unwrap_err().to_string(), error.to_string()),
        }
    }

    #[test]
    fn text_guard_rejects_nul() {
        assert!(ensure_storable_text("a\0b").is_err());
        assert!(ensure_storable_text("").is_ok());
    }
}
