crabber_guest::generate!(world: "tool-and-sink", path: "wit");
use crabber::extensions::types;
use exports::crabber::extensions::{event_sink_api, manifest_api, tool_api};
struct Fixture;
impl manifest_api::Guest for Fixture {
    fn describe() -> types::ExtensionManifest {
        types::ExtensionManifest {
            id: "tool-and-sink".into(),
            version: "0.1.0".into(),
            roles: vec![types::Role::Tool, types::Role::EventSink],
            config_json_schema: None,
        }
    }
    fn configure(_config_json: String) -> Result<(), types::StructuredError> {
        Ok(())
    }
}
impl tool_api::Guest for Fixture {
    fn tools() -> Vec<types::ToolMetadata> {
        vec![types::ToolMetadata {
            name: "echo".into(),
            description: "Returns its input".into(),
            parameters_json_schema: r#"{"type":"object"}"#.into(),
            retry_safe: true,
            required_permissions: vec![],
            prompt_snippet: None,
            prompt_guidelines: vec![],
        }]
    }

    fn permission_pattern(
        _tool_name: String,
        input_json: String,
    ) -> Result<String, types::StructuredError> {
        Ok(format!("echo:{}", input_json.len()))
    }

    fn execute(
        _tool_name: String,
        _tool_call_id: String,
        input_json: String,
        _turn: types::TurnMetadata,
    ) -> Result<String, types::StructuredError> {
        Ok(input_json)
    }
}

impl event_sink_api::Guest for Fixture {
    fn emit(_event: types::BoundedEvent) -> Result<(), types::StructuredError> {
        let count = crabber::host::state::get("count")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        crabber::host::state::set("count", &(count + 1).to_string()).map_err(|message| {
            types::StructuredError {
                code: "state".into(),
                message,
                retryable: false,
            }
        })
    }
}
export!(Fixture);
