//! Provider classification is separate from a host's summary display policy.
use serde::{Deserialize, Serialize};

/// Missing provenance, including legacy/custom-provider reasoning, is private.
/// Opaque continuation material remains private `ContentBlock::ProviderState`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningProvenance {
    /// A provider's explicitly supported summary channel, never raw thinking.
    PublicSummary,
    Private,
    #[default]
    Unspecified,
}
impl ReasoningProvenance {
    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "Serde skip_serializing_if predicates receive a borrowed field"
    )]
    pub(crate) const fn is_unspecified(&self) -> bool {
        matches!(self, Self::Unspecified)
    }
}

#[cfg(test)]
mod tests {
    use super::ReasoningProvenance;
    use crate::ContentBlock;
    #[test]
    fn legacy_reasoning_is_unspecified_and_round_trips_without_new_public_claims() {
        let original = serde_json::json!({"type":"reasoning","text":"PRIVATE_LEGACY","provider_state":{"signature":"OPAQUE"}});
        let restored: ContentBlock = serde_json::from_value(original.clone()).unwrap();
        assert!(matches!(
            &restored,
            ContentBlock::Reasoning {
                provenance: ReasoningProvenance::Unspecified,
                item_id: None,
                ..
            }
        ));
        assert_eq!(serde_json::to_value(restored).unwrap(), original);
    }
}
