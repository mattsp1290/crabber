crabber_guest::generate!(world: "tool-middleware");
use crabber::extensions::types;
use exports::crabber::extensions::{manifest_api, tool_middleware_api};

/// Echoes the result-transform arguments inside a valid envelope. Result markers
/// exercise rejected replies without changing the WIT contract.
struct Fixture;

impl manifest_api::Guest for Fixture {
    fn describe() -> types::ExtensionManifest {
        types::ExtensionManifest {
            id: "echo-middleware".into(),
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
        tool_name: String,
        tool_call_id: String,
        executed_input_json: String,
        output_json: String,
        is_error: bool,
        _turn: types::TurnMetadata,
    ) -> types::Replacement {
        use crabber_guest::serde_json::{self, Value, json};
        let mut envelope: Value = match serde_json::from_str(&output_json) {
            Ok(value) => value,
            Err(_) => return types::Replacement::Error(types::StructuredError {
                code: "decode".into(), message: "SECRET-DEPTH-ERROR".into(), retryable: false,
            }),
        };
        let marker = envelope["result"].as_str().unwrap_or_default().to_owned();
        match marker.as_str() {
            "__unchanged__" => return types::Replacement::Unchanged,
            "__error__" => return types::Replacement::Error(types::StructuredError {
                code: "fixture".into(), message: "SECRET-GUEST-ERROR".into(), retryable: false,
            }),
            "__malformed__" => return types::Replacement::Json("{".into()),
            "__non_envelope__" => return types::Replacement::Json("null".into()),
            "__missing_mark_error__" => { envelope.as_object_mut().unwrap().remove("mark_error"); }
            "__extra_key__" => { envelope["is_error"] = json!(false); }
            "__bad_mark_error__" => { envelope["mark_error"] = json!("true"); }
            "__mark_error__" => { envelope["mark_error"] = json!(true); }
            _ => {
                if let Some(field) = marker.strip_prefix("__tamper__") {
                    envelope["context"][field] = json!("SECRET-TAMPER");
                }
            }
        }
        let input: Value = match serde_json::from_str(&executed_input_json) {
            Ok(input) => input,
            Err(_) => return types::Replacement::Error(types::StructuredError {
                code: "input".into(), message: "SECRET-INPUT-ERROR".into(), retryable: false,
            }),
        };
        envelope["result"] = json!({
            "tool_name": tool_name,
            "tool_call_id": tool_call_id,
            "executed_input": input,
            "output_json": output_json,
            "is_error": is_error,
            "turn": {
                "session_id": _turn.session_id,
                "run_id": _turn.run_id,
                "epoch_id": _turn.epoch_id,
                "turn_index": _turn.turn_index,
                "agent_name": _turn.agent_name,
                "agent_mode": _turn.agent_mode,
                "provider_id": _turn.provider_id,
                "model_id": _turn.model_id,
                "tool_names": _turn.tool_names,
                "message_count": _turn.message_count,
                "role_counts": {"system": _turn.role_counts.system, "user": _turn.role_counts.user, "assistant": _turn.role_counts.assistant, "tool": _turn.role_counts.tool},
                "has_system_prompt": _turn.has_system_prompt,
                "workspace_id": _turn.workspace_id,
            },
        });
        types::Replacement::Json(envelope.to_string())
    }
}
export!(Fixture);
