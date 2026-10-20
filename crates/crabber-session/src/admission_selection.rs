//! Frozen policy provenance for a resolved provider/model choice.
use crate::StoreError;
use serde::{Deserialize, Serialize};
/// Reserved configuration component binds default-selection intent to admission.
pub const SELECTION_POLICY_COMPONENT: &str = "contract:crabber/session-default-selection.v1";
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AdmissionSelectionPolicy {
    #[default]
    Explicit,
    /// Each absent stored default independently uses this original host fallback.
    SessionDefaults { fallback: Box<SelectionFallback> },
}
/// Original host fallback identifiers; credentials never belong in this record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionFallback {
    pub provider_id: String,
    pub model_id: String,
}
impl AdmissionSelectionPolicy {
    #[must_use]
    pub fn session_defaults(provider_id: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self::SessionDefaults {
            fallback: Box::new(SelectionFallback {
                provider_id: provider_id.into(),
                model_id: model_id.into(),
            }),
        }
    }

    #[must_use]
    pub fn is_explicit(&self) -> bool {
        matches!(self, Self::Explicit)
    }
    /// Validate bounded provenance before admission or replay.
    /// # Errors
    /// Rejects invalid host fallback identifiers.
    pub fn validate(&self) -> Result<(), StoreError> {
        if let Self::SessionDefaults { fallback } = self {
            let provider_id = &fallback.provider_id;
            let model_id = &fallback.model_id;
            for value in [provider_id, model_id] {
                if value.trim().is_empty()
                    || value.len() > 512
                    || value.chars().any(char::is_control)
                {
                    return Err(StoreError::AdmissionConflict);
                }
            }
        }
        Ok(())
    }
    pub(crate) fn verify_components(
        &self,
        semantics: &serde_json::Value,
    ) -> Result<(), StoreError> {
        self.validate()?;
        let components = semantics
            .get(12)
            .and_then(serde_json::Value::as_array)
            .ok_or(StoreError::AdmissionConflict)?;
        let mut candidates = components.iter().filter(|value| {
            value.get("id").and_then(serde_json::Value::as_str) == Some(SELECTION_POLICY_COMPONENT)
        });
        let original = candidates.next();
        if candidates.next().is_some() {
            return Err(StoreError::AdmissionConflict);
        }
        match (self, original) {
            (Self::Explicit, None) => Ok(()),
            (Self::SessionDefaults { .. }, Some(value))
                if value
                    .get("version")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|encoded| serde_json::from_str::<Self>(encoded).ok())
                    .as_ref()
                    == Some(self) =>
            {
                Ok(())
            }
            _ => Err(StoreError::AdmissionConflict),
        }
    }
}
