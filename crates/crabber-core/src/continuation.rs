//! Explicit typed suspension; checkpoints are private, never tool-result JSON.
//! Unstable until the durable-host contract is qualified.
use crate::{CoreError, InteractionDescription, ToolOutput, input, interaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

pub const MAX_TOOL_CHECKPOINT_BYTES: usize = 64 * 1024;
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireCheckpoint {
    version: u32,
    schema_id: String,
    schema_version: String,
    value: Value,
}

/// Owner-authored private state. Runtime restores only the exact declared schema
/// version; no automatic migration or repetition of earlier effects is implied.
#[derive(Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ToolCheckpoint(WireCheckpoint);
impl ToolCheckpoint {
    /// # Errors
    /// Rejects invalid schema identity, deep JSON and oversized private state.
    pub fn new(
        schema_id: impl Into<String>,
        schema_version: impl Into<String>,
        value: Value,
    ) -> Result<Self, CoreError> {
        Self::from_wire(WireCheckpoint {
            version: 1,
            schema_id: schema_id.into(),
            schema_version: schema_version.into(),
            value,
        })
    }
    fn from_wire(value: WireCheckpoint) -> Result<Self, CoreError> {
        if value.version != 1
            || !interaction::identity(&value.schema_id)
            || !interaction::identity(&value.schema_version)
        {
            return Err(invalid());
        }
        input::validate_json(&value.value, 0).map_err(|_| invalid())?;
        input::validate_serialized_limit(&value, MAX_TOOL_CHECKPOINT_BYTES)
            .map_err(|_| invalid())?;
        Ok(Self(value))
    }
    #[must_use]
    pub fn schema_id(&self) -> &str {
        &self.0.schema_id
    }
    #[must_use]
    pub fn schema_version(&self) -> &str {
        &self.0.schema_version
    }
    #[must_use]
    pub fn value(&self) -> &Value {
        &self.0.value
    }
}
impl<'de> Deserialize<'de> for ToolCheckpoint {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let invalid = || serde::de::Error::custom("invalid tool checkpoint");
        let wire = WireCheckpoint::deserialize(deserializer).map_err(|_| invalid())?;
        Self::from_wire(wire).map_err(|_| invalid())
    }
}
impl std::fmt::Debug for ToolCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ToolCheckpoint([redacted])")
    }
}

/// Tool-authored suspension proposal. Runtime supplies identities and persists
/// checkpoint, pending set and barrier atomically, then proves phase quiescence.
#[derive(Clone, PartialEq)]
pub struct ToolSuspension {
    checkpoint: ToolCheckpoint,
    interactions: Vec<InteractionDescription>,
}
impl ToolSuspension {
    /// # Errors
    /// Rejects an empty/oversized pending set or duplicate local interaction names.
    pub fn new(
        checkpoint: ToolCheckpoint,
        interactions: Vec<InteractionDescription>,
    ) -> Result<Self, CoreError> {
        if interactions.is_empty() || interactions.len() > 64 {
            return Err(invalid());
        }
        let mut names = BTreeSet::new();
        if interactions
            .iter()
            .any(|description| !names.insert(description.name()))
        {
            return Err(invalid());
        }
        input::validate_serialized_bound(&(&checkpoint, &interactions)).map_err(|_| invalid())?;
        Ok(Self {
            checkpoint,
            interactions,
        })
    }
    #[must_use]
    pub fn checkpoint(&self) -> &ToolCheckpoint {
        &self.checkpoint
    }
    #[must_use]
    pub fn interactions(&self) -> &[InteractionDescription] {
        &self.interactions
    }
}
impl std::fmt::Debug for ToolSuspension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ToolSuspension([redacted])")
    }
}

/// Control outcome kept separate from completion content. Never interpret an
/// ordinary JSON value as this enum or feed suspension into result transforms.
#[derive(Debug, Clone, PartialEq)]
pub enum ToolPhaseOutcome {
    Completed(ToolOutput),
    Suspended(ToolSuspension),
}
fn invalid() -> CoreError {
    CoreError::Validation("invalid tool checkpoint or suspension".into())
}
