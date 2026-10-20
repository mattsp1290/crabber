crabber_guest::generate!(world: "tool");

use crabber::extensions::types;
use exports::crabber::extensions::tool_api;

struct Mixed;

impl tool_api::Guest for Mixed {
    fn tools() -> Vec<types::ToolMetadata> {
        vec![types::ToolMetadata {
            name: "mixed".into(),
            description: "Returns typed mixed content".into(),
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
        use crabber_guest::{MediaKind, MediaSource, MessageContent, ToolOutput};
        Ok(ToolOutput::new(vec![
            MessageContent::Text { text: "PRIVATE_CAPTION".into() },
            MessageContent::Media { kind: MediaKind::Image, mime_type: "image/png".into(), source: MediaSource::File { file_id: "opaque-image".into() } },
            MessageContent::Text { text: "trailing text".into() },
        ], false).unwrap().into_envelope().to_string())
    }
}

crabber_guest::export_extension!(Mixed; id = "typed-output-tool", version = "0.1.0", roles = [tool]);
