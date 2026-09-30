crabber_guest::generate!(world: "bundle");
use crabber::extensions::types;
use exports::crabber::extensions::{
    context_source_api, event_sink_api, hook_api, manifest_api, model_controls_api,
    permissions_policy_api, prompt_section_api, tool_api, tool_middleware_api,
};
struct Fixture;
impl manifest_api::Guest for Fixture {
    fn describe() -> types::ExtensionManifest {
        types::ExtensionManifest {
            id: "all-in-one".into(),
            version: "0.1.0".into(),
            roles: vec![
                types::Role::Tool,
                types::Role::PermissionsPolicy,
                types::Role::ContextSource,
                types::Role::PromptSection,
                types::Role::EventSink,
                types::Role::Hook,
                types::Role::ToolMiddleware,
                types::Role::ModelControls,
            ],
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

impl permissions_policy_api::Guest for Fixture {
    fn decide(
        request: types::PermissionRequest,
    ) -> Result<types::PermissionDecision, types::StructuredError> {
        let action = match request.tool_name.as_str() {
            "dangerous" => types::PermissionAction::Deny,
            "ask_me" => types::PermissionAction::Ask,
            _ => types::PermissionAction::Allow,
        };
        Ok(types::PermissionDecision {
            action,
            reason: "fixture".into(),
        })
    }
}

impl context_source_api::Guest for Fixture {
    fn load_context(
        turn: types::TurnMetadata,
    ) -> Result<Vec<types::Message>, types::StructuredError> {
        Ok(vec![types::Message {
            role: types::TextRole::System,
            blocks: vec![types::ContentBlock::Text(format!(
                "banner turn {}",
                turn.turn_index
            ))],
        }])
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
        types::Replacement::Json(output_json.replace("secret", "[REDACTED]"))
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
impl prompt_section_api::Guest for Fixture {
    fn sections() -> Vec<types::SectionMetadata> {
        vec![types::SectionMetadata {
            name: "banner".into(),
            order: 0,
        }]
    }
    fn render(_name: String, _turn: types::TurnMetadata) -> Result<String, types::StructuredError> {
        Ok("all-in-one prompt".into())
    }
}
impl hook_api::Guest for Fixture {
    fn before_run(_turn: types::TurnMetadata) -> Result<(), types::StructuredError> {
        Ok(())
    }
    fn after_run(
        _turn: types::TurnMetadata,
        _status: String,
    ) -> Result<(), types::StructuredError> {
        Ok(())
    }
    fn before_turn(_turn: types::TurnMetadata) -> Result<(), types::StructuredError> {
        Ok(())
    }
    fn after_turn(_turn: types::TurnMetadata) -> Result<(), types::StructuredError> {
        Ok(())
    }
}
impl model_controls_api::Guest for Fixture {
    fn before_model_request(
        _turn: types::TurnMetadata,
        _controls_json: String,
    ) -> types::Replacement {
        types::Replacement::Unchanged
    }
}
export!(Fixture);
