//! Preserve complete RFC6902 operation objects, including legal extension fields.
use crate::{CoreError, MAX_PUBLIC_STATE_BYTES, input};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PublicPatchOperation(Map<String, Value>);
impl PublicPatchOperation {
    /// Retain an exact operation object after validating its required members.
    /// # Errors
    /// Rejects malformed operations, invalid pointers, or unbounded payloads.
    pub fn new(value: Value) -> Result<Self, CoreError> {
        input::validate_json(&value, 0).map_err(|_| invalid())?;
        input::validate_serialized_limit(&value, MAX_PUBLIC_STATE_BYTES).map_err(|_| invalid())?;
        let Value::Object(fields) = value else {
            return Err(invalid());
        };
        let operation = Self(fields);
        operation.native()?;
        Ok(operation)
    }
    #[must_use]
    pub fn members(&self) -> &Map<String, Value> {
        &self.0
    }
    fn native(&self) -> Result<json_patch::PatchOperation, CoreError> {
        serde_json::from_value(Value::Object(self.0.clone())).map_err(|_| invalid())
    }
}
impl<'de> Deserialize<'de> for PublicPatchOperation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(Value::deserialize(deserializer)?)
            .map_err(|_| serde::de::Error::custom("invalid public patch operation"))
    }
}
/// Apply only to a private clone: a failing operation never modifies the caller's
/// original value. Store transactions publish this result with event and revision.
/// # Errors
/// Rejects invalid RFC6902 operations, failed tests and oversized/deep results.
pub fn apply_public_patch(
    current: &Value,
    operations: &[PublicPatchOperation],
) -> Result<Value, CoreError> {
    if operations.len() > 256 {
        return Err(invalid());
    }
    input::validate_serialized_limit(&operations, MAX_PUBLIC_STATE_BYTES).map_err(|_| invalid())?;
    input::validate_json(current, 0).map_err(|_| invalid())?;
    input::validate_serialized_limit(current, MAX_PUBLIC_STATE_BYTES).map_err(|_| invalid())?;
    let native = operations
        .iter()
        .map(PublicPatchOperation::native)
        .collect::<Result<Vec<_>, _>>()?;
    let mut result = current.clone();
    for operation in &native {
        // A single copy can at most combine the bounded current value with one
        // bounded operation. Refuse before another operation can amplify it.
        json_patch::patch(&mut result, std::slice::from_ref(operation)).map_err(|_| invalid())?;
        input::validate_json(&result, 0).map_err(|_| invalid())?;
        input::validate_serialized_limit(&result, MAX_PUBLIC_STATE_BYTES).map_err(|_| invalid())?;
    }
    Ok(result)
}
fn invalid() -> CoreError {
    CoreError::Validation("invalid public patch operation".into())
}
