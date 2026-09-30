crabber_guest::generate!(world: "model-controls");

use crabber::extensions::types;
use exports::crabber::extensions::model_controls_api;

struct Controls;

impl model_controls_api::Guest for Controls {
    fn before_model_request(
        _turn: types::TurnMetadata,
        controls_json: String,
    ) -> types::Replacement {
        let Ok(value) = crabber_guest::serde_json::from_str::<crabber_guest::serde_json::Value>(&controls_json) else {
            return types::Replacement::Error(types::StructuredError { code: "json".into(), message: "invalid controls".into(), retryable: false });
        };
        if ["top-p", "max-tokens", "tool-choice"].iter().any(|key| value.get(*key).is_none()) {
            return types::Replacement::Error(types::StructuredError { code: "keys".into(), message: "missing documented key".into(), retryable: false });
        }
        types::Replacement::Unchanged
    }
}

crabber_guest::export_extension!(Controls; id = "external-controls", version = "0.1.0", roles = [model_controls]);
