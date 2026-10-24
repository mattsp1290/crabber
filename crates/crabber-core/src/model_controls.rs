//! Model controls captured beside each selected provider and model.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningEffort {
    Low,
    Medium,
    High,
}
impl ReasoningEffort {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}
impl std::str::FromStr for ReasoningEffort {
    type Err = crate::CoreError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            _ => Err(crate::CoreError::Validation(
                "invalid reasoning effort".into(),
            )),
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelControls {
    #[serde(default)]
    pub reasoning_effort: Option<ReasoningEffort>,
}
impl ModelControls {
    #[must_use]
    pub fn reasoning_effort(mut self, effort: ReasoningEffort) -> Self {
        self.reasoning_effort = Some(effort);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn efforts_are_exact_and_controls_reject_unknown_fields() {
        for effort in [
            ReasoningEffort::Low,
            ReasoningEffort::Medium,
            ReasoningEffort::High,
        ] {
            assert_eq!(effort.as_str().parse(), Ok(effort));
            assert_eq!(serde_json::to_value(effort).unwrap(), effort.as_str());
        }
        for invalid in ["", "LOW", " medium", "maximum"] {
            assert!(invalid.parse::<ReasoningEffort>().is_err());
        }
        assert_eq!(
            serde_json::from_str::<ModelControls>("{}").unwrap(),
            ModelControls::default()
        );
        assert!(serde_json::from_str::<ModelControls>(r#"{"effort":"high"}"#).is_err());
    }
}
