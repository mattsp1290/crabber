crabber_guest::generate!(world: "tool-middleware");
use crabber::extensions::types;
use exports::crabber::extensions::{manifest_api, tool_middleware_api};

/// Test fixture for characterizing the host's after-tool adapter. By default
/// `after-tool-call` replies `json(..)` with an object echoing every argument it
/// received. If the output contains `__unchanged__` it replies `unchanged`; if
/// it contains `__error__` it replies `error(..)`.
struct Fixture;

fn escape(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

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
        if output_json.contains("__unchanged__") {
            return types::Replacement::Unchanged;
        }
        if output_json.contains("__error__") {
            return types::Replacement::Error(types::StructuredError {
                code: "fixture".into(),
                message: "echo rejected".into(),
                retryable: false,
            });
        }
        types::Replacement::Json(format!(
            "{{\"tool_name\":{},\"tool_call_id\":{},\"executed_input_json\":{},\"output_json\":{},\"is_error\":{}}}",
            escape(&tool_name),
            escape(&tool_call_id),
            escape(&executed_input_json),
            escape(&output_json),
            is_error
        ))
    }
}
export!(Fixture);
