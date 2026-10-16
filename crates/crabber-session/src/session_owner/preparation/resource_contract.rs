//! The reserved marker is configuration, never physical ownership evidence.
use crate::session_owner::SessionOwnerError as Error;
use crabber_core::InputFingerprint;
use serde_json::Value;

const NAMESPACE: &str = "crabber.runtime.preparation.resources.";
const MARKER: &str = "crabber.runtime.preparation.resources.v1";

/// Read the exact optional resource declaration from frozen runtime semantics.
/// Old semantics without a marker remain legacy ownership-unknown.
/// # Errors
/// Rejects duplicate, nested, malformed or unknown-version reserved markers.
pub fn resource_contract(semantics: &Value) -> Result<Option<InputFingerprint>, Error> {
    let mut marker = None;
    if let Some(items) = semantics.as_array() {
        for (index, item) in items.iter().enumerate() {
            if let Some(object) = item.as_object()
                && let Some(value) = object.get(MARKER)
            {
                if marker.is_some() || object.len() != 1 || index + 1 != items.len() {
                    return Err(Error::InvalidRequest);
                }
                let fingerprint = value.as_str().ok_or(Error::InvalidRequest)?;
                marker = Some(
                    InputFingerprint::new(fingerprint.to_owned())
                        .map_err(|_| Error::InvalidRequest)?,
                );
                continue;
            }
            reject_reserved(item)?;
        }
    } else {
        reject_reserved(semantics)?;
    }
    Ok(marker)
}

fn reject_reserved(value: &Value) -> Result<(), Error> {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                if key.starts_with(NAMESPACE) {
                    return Err(Error::InvalidRequest);
                }
                reject_reserved(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                reject_reserved(value)?;
            }
        }
        _ => (),
    }
    Ok(())
}

/// Append the original pure declaration while constructing new semantics.
/// Absence preserves the original JSON exactly. This grants no ownership credit.
/// # Errors
/// Refuses replacing an existing declaration or a malformed semantics carrier.
pub fn bind_resource_contract(
    semantics: &mut Value,
    declared: Option<&InputFingerprint>,
) -> Result<(), Error> {
    if resource_contract(semantics)?.is_some() {
        return Err(Error::InvalidRequest);
    }
    if let Some(declared) = declared {
        semantics
            .as_array_mut()
            .ok_or(Error::InvalidRequest)?
            .push(serde_json::json!({MARKER: declared.as_str()}));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn exact_optional_marker_preserves_legacy_and_rejects_ambiguous_policy() {
        let mut legacy = json!(["runtime.v1", {"plan": "old"}]);
        let original = legacy.clone();
        bind_resource_contract(&mut legacy, None).unwrap();
        assert_eq!(legacy, original);
        assert_eq!(resource_contract(&legacy).unwrap(), None);
        let fingerprint = InputFingerprint::new("a".repeat(64)).unwrap();
        bind_resource_contract(&mut legacy, Some(&fingerprint)).unwrap();
        assert_eq!(resource_contract(&legacy).unwrap(), Some(fingerprint));
        assert!(bind_resource_contract(&mut legacy, None).is_err());
        for invalid in [
            json!([{MARKER: "a".repeat(64)}, {MARKER: "a".repeat(64)}]),
            json!([{"nested": {MARKER: "a".repeat(64)}}]),
            json!([{MARKER: "a".repeat(64), "extra": true}]),
            json!([{MARKER: {"claim": "a".repeat(64)}}]),
            json!([{"crabber.runtime.preparation.resources.v2": "a".repeat(64)}]),
        ] {
            assert!(resource_contract(&invalid).is_err());
        }
    }
}
