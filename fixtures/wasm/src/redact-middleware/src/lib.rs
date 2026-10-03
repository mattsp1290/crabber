crabber_guest::generate!(world: "tool-middleware");
use crabber::extensions::types;
use exports::crabber::extensions::{manifest_api, tool_middleware_api};
struct Fixture;
impl manifest_api::Guest for Fixture {
    fn describe() -> types::ExtensionManifest {
        types::ExtensionManifest {
            id: "redact-middleware".into(),
            version: "0.1.0".into(),
            roles: vec![types::Role::ToolMiddleware],
            config_json_schema: None,
        }
    }
    fn configure(_config_json: String) -> Result<(), types::StructuredError> {
        Ok(())
    }
}
impl tool_middleware_api::Guest for Fixture {
    fn before_tool_call(
        _tool_name: String,
        _tool_call_id: String,
        _input_json: String,
        _turn: types::TurnMetadata,
    ) -> types::Replacement {
        types::Replacement::Unchanged
    }
    fn after_tool_call(
        _tool_name: String,
        _tool_call_id: String,
        _executed_input_json: String,
        output_json: String,
        _is_error: bool,
        _turn: types::TurnMetadata,
    ) -> types::Replacement {
        let mut envelope: crabber_guest::serde_json::Value =
            match crabber_guest::serde_json::from_str(&output_json) {
                Ok(envelope) => envelope,
                Err(_) => return types::Replacement::Error(types::StructuredError {
                    code: "envelope".into(), message: "invalid envelope".into(), retryable: false,
                }),
            };
        let redacted = envelope["result"].to_string().replace("secret", "[REDACTED]");
        envelope["result"] = crabber_guest::serde_json::from_str(&redacted).unwrap();
        types::Replacement::Json(envelope.to_string())
    }
}
export!(Fixture);
