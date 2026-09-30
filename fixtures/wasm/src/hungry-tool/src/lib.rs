crabber_guest::generate!(world: "tool");

use crabber::extensions::types;
use exports::crabber::extensions::{manifest_api, tool_api};

struct Echo;

impl manifest_api::Guest for Echo {
    fn describe() -> types::ExtensionManifest {
        types::ExtensionManifest {
            id: "hungry-tool".into(),
            version: "0.1.0".into(),
            roles: vec![types::Role::Tool],
            config_json_schema: None,
        }
    }

    fn configure(_config_json: String) -> Result<(), types::StructuredError> {
        Ok(())
    }
}

impl tool_api::Guest for Echo {
    fn tools() -> Vec<types::ToolMetadata> {
        vec![types::ToolMetadata {
            name: "hungry-tool".into(),
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
        let _ = input_json;
        let mut data = Vec::new();
        loop {
            data.extend(std::iter::repeat_n(0_u8, 1024 * 1024));
            std::hint::black_box(&data);
        }
    }
}

export!(Echo);
