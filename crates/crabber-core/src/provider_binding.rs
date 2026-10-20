//! Frozen, nonsecret backend identity retained with execution evidence.
use crate::{CoreError, InputFingerprint};
use serde::{Deserialize, Serialize};

/// Inspectable token bounds. Unknown limits remain absent, never guessed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ProviderBindingLimits {
    pub context_tokens: Option<u64>,
    pub output_tokens: Option<u32>,
}
impl ProviderBindingLimits {
    fn valid(self) -> bool {
        self.context_tokens != Some(0)
            && self.output_tokens != Some(0)
            && !matches!((self.context_tokens, self.output_tokens), (Some(context), Some(output)) if u64::from(output) > context)
    }
}

/// Versioned backend/model/configuration identity, never credentials or headers.
/// Native hosts must change behavior/config identity when routing or callbacks
/// change. This cannot guarantee that a remote model alias remains immutable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WireDescriptor", into = "WireDescriptor")]
pub struct ProviderBindingDescriptor(WireDescriptor);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireDescriptor {
    version: u32,
    provider_id: String,
    model_id: String,
    backend_id: String,
    behavior_version: String,
    config_fingerprint: InputFingerprint,
    limits: ProviderBindingLimits,
}
impl ProviderBindingDescriptor {
    /// Freeze host-declared behavior and configuration, excluding credentials.
    /// # Errors
    /// Rejects unbounded/empty/control-bearing identities and impossible limits.
    pub fn new(
        provider_id: impl Into<String>,
        model_id: impl Into<String>,
        backend_id: impl Into<String>,
        behavior_version: impl Into<String>,
        config_fingerprint: InputFingerprint,
        limits: ProviderBindingLimits,
    ) -> Result<Self, CoreError> {
        WireDescriptor {
            version: 1,
            provider_id: provider_id.into(),
            model_id: model_id.into(),
            backend_id: backend_id.into(),
            behavior_version: behavior_version.into(),
            config_fingerprint,
            limits,
        }
        .try_into()
    }
    #[must_use]
    pub fn provider_id(&self) -> &str {
        &self.0.provider_id
    }
    #[must_use]
    pub fn model_id(&self) -> &str {
        &self.0.model_id
    }
    #[must_use]
    pub fn backend_id(&self) -> &str {
        &self.0.backend_id
    }
    #[must_use]
    pub fn behavior_version(&self) -> &str {
        &self.0.behavior_version
    }
    #[must_use]
    pub fn config_fingerprint(&self) -> &InputFingerprint {
        &self.0.config_fingerprint
    }
    #[must_use]
    pub fn limits(&self) -> ProviderBindingLimits {
        self.0.limits
    }
}
impl TryFrom<WireDescriptor> for ProviderBindingDescriptor {
    type Error = CoreError;
    fn try_from(value: WireDescriptor) -> Result<Self, Self::Error> {
        let valid_identity =
            |s: &str| !s.is_empty() && s.len() <= 512 && !s.chars().any(char::is_control);
        if value.version != 1
            || !value.limits.valid()
            || ![
                &value.provider_id,
                &value.model_id,
                &value.backend_id,
                &value.behavior_version,
            ]
            .into_iter()
            .all(|s| valid_identity(s))
        {
            return Err(CoreError::Validation(
                "invalid provider binding descriptor".into(),
            ));
        }
        Ok(Self(value))
    }
}
impl From<ProviderBindingDescriptor> for WireDescriptor {
    fn from(value: ProviderBindingDescriptor) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn persisted_binding_validates_versions_and_bounds() {
        let descriptor = ProviderBindingDescriptor::new(
            "provider",
            "model",
            "backend",
            "v1",
            InputFingerprint::new("a".repeat(64)).unwrap(),
            ProviderBindingLimits {
                context_tokens: Some(100),
                output_tokens: Some(50),
            },
        )
        .unwrap();
        let wire = serde_json::to_value(&descriptor).unwrap();
        assert_eq!(
            serde_json::from_value::<ProviderBindingDescriptor>(wire.clone()).unwrap(),
            descriptor
        );
        for invalid in [serde_json::json!(2), serde_json::json!(null)] {
            let mut changed = wire.clone();
            changed["version"] = invalid;
            assert!(serde_json::from_value::<ProviderBindingDescriptor>(changed).is_err());
        }
        for key in ["provider_id", "model_id", "backend_id", "behavior_version"] {
            for invalid in [String::new(), "x".repeat(513), "invalid\nidentity".into()] {
                let mut changed = wire.clone();
                changed[key] = serde_json::json!(invalid);
                assert!(serde_json::from_value::<ProviderBindingDescriptor>(changed).is_err());
            }
        }
        for limits in [
            serde_json::json!({"context_tokens":0,"output_tokens":null}),
            serde_json::json!({"context_tokens":100,"output_tokens":101}),
        ] {
            let mut changed = wire.clone();
            changed["limits"] = limits;
            assert!(serde_json::from_value::<ProviderBindingDescriptor>(changed).is_err());
        }
    }
}
