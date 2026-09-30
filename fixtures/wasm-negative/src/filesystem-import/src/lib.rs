crabber_guest::generate!(world: "tool");

use crabber::extensions::types;
use exports::crabber::extensions::tool_api;

struct Echo;

impl tool_api::Guest for Echo {
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
        let _ = std::fs::read("/tmp/fixture");
        Ok(input_json)
    }
}

crabber_guest::export_extension!(Echo; id = "filesystem-import", version = "0.1.0", roles = [tool]);
