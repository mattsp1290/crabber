//! Bounded public state/activity values. Unstable until durable-host qualification.
use crate::{CoreError, input};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

pub const MAX_PUBLIC_STATE_BYTES: usize = 256 * 1024;
pub const MAX_ACTIVITY_BYTES: usize = 64 * 1024;
pub const MAX_PUBLIC_CONTENTS_BYTES: usize = 1024 * 1024;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireActivity {
    id: String,
    activity_type: String,
    content: Value,
}
/// Activity is display data, never implicitly provider conversation input.
#[derive(Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PublicActivity(WireActivity);
impl PublicActivity {
    /// # Errors
    /// Rejects invalid identity, deep JSON and oversized content.
    pub fn new(
        id: impl Into<String>,
        activity_type: impl Into<String>,
        content: Value,
    ) -> Result<Self, CoreError> {
        let wire = WireActivity {
            id: id.into(),
            activity_type: activity_type.into(),
            content,
        };
        if !identity(&wire.id) || !identity(&wire.activity_type) {
            return Err(invalid());
        }
        input::validate_json(&wire.content, 0).map_err(|_| invalid())?;
        input::validate_serialized_limit(&wire, MAX_ACTIVITY_BYTES).map_err(|_| invalid())?;
        Ok(Self(wire))
    }
    #[must_use]
    pub fn id(&self) -> &str {
        &self.0.id
    }
    #[must_use]
    pub fn activity_type(&self) -> &str {
        &self.0.activity_type
    }
    #[must_use]
    pub fn content(&self) -> &Value {
        &self.0.content
    }
}
impl<'de> Deserialize<'de> for PublicActivity {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let invalid = || serde::de::Error::custom("invalid public activity");
        let wire = WireActivity::deserialize(d).map_err(|_| invalid())?;
        Self::new(wire.id, wire.activity_type, wire.content).map_err(|_| invalid())
    }
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireContents {
    state: Value,
    activities: Vec<PublicActivity>,
}
/// One complete state/activity replacement. An expected revision protects read /
/// modify / replace. Explicit null, arrays and scalar JSON retain their meaning.
#[derive(Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PublicStateContents(WireContents);
impl PublicStateContents {
    /// # Errors
    /// Rejects deep/oversized JSON, duplicate activity IDs and oversized sets.
    pub fn new(state: Value, activities: Vec<PublicActivity>) -> Result<Self, CoreError> {
        if activities.len() > 128 {
            return Err(invalid());
        }
        let mut ids = BTreeSet::new();
        if activities.iter().any(|activity| !ids.insert(activity.id())) {
            return Err(invalid());
        }
        input::validate_json(&state, 0).map_err(|_| invalid())?;
        input::validate_serialized_limit(&state, MAX_PUBLIC_STATE_BYTES).map_err(|_| invalid())?;
        let wire = WireContents { state, activities };
        input::validate_serialized_limit(&wire, MAX_PUBLIC_CONTENTS_BYTES)
            .map_err(|_| invalid())?;
        Ok(Self(wire))
    }
    #[must_use]
    pub fn state(&self) -> &Value {
        &self.0.state
    }
    #[must_use]
    pub fn activities(&self) -> &[PublicActivity] {
        &self.0.activities
    }
}
impl Default for PublicStateContents {
    fn default() -> Self {
        Self(WireContents {
            state: Value::Null,
            activities: vec![],
        })
    }
}
impl<'de> Deserialize<'de> for PublicStateContents {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let invalid = || serde::de::Error::custom("invalid public state");
        let wire = WireContents::deserialize(d).map_err(|_| invalid())?;
        Self::new(wire.state, wire.activities).map_err(|_| invalid())
    }
}
impl std::fmt::Debug for PublicStateContents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PublicStateContents([redacted])")
    }
}
impl std::fmt::Debug for PublicActivity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PublicActivity([redacted])")
    }
}
fn identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}
fn invalid() -> CoreError {
    CoreError::Validation("invalid public state".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_values_preserve_arbitrary_json_and_bound_display_data() {
        let contents = PublicStateContents::new(
            serde_json::json!({"nul":"\u{0}","values":[null,0,true]}),
            vec![PublicActivity::new("id", "display", Value::String("\u{0}".into())).unwrap()],
        )
        .unwrap();
        let restored: PublicStateContents =
            serde_json::from_str(&serde_json::to_string(&contents).unwrap()).unwrap();
        assert_eq!(restored, contents);
        assert!(
            PublicStateContents::new(Value::String("x".repeat(MAX_PUBLIC_STATE_BYTES)), vec![])
                .is_err()
        );
        assert!(
            PublicActivity::new(
                "id",
                "display",
                Value::String("x".repeat(MAX_ACTIVITY_BYTES))
            )
            .is_err()
        );
        let activity = PublicActivity::new("id", "display", Value::Null).unwrap();
        assert!(PublicStateContents::new(Value::Null, vec![activity.clone(), activity]).is_err());
        assert!(PublicActivity::new("invalid\nidentity", "display", Value::Null).is_err());
        let invalid = serde_json::json!({"state":{},"activities":[{"id":"id","activity_type":"display","content":null,"PRIVATE_SENTINEL":true}]});
        let error = serde_json::from_value::<PublicStateContents>(invalid).unwrap_err();
        assert!(!error.to_string().contains("PRIVATE_SENTINEL"));
    }
}
