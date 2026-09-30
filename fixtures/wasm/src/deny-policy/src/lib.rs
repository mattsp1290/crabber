crabber_guest::generate!(world: "permissions-policy");
use crabber::extensions::types;
use exports::crabber::extensions::{manifest_api, permissions_policy_api};
struct Fixture;
impl manifest_api::Guest for Fixture {
    fn describe() -> types::ExtensionManifest {
        types::ExtensionManifest {
            id: "deny-policy".into(),
            version: "0.1.0".into(),
            roles: vec![types::Role::PermissionsPolicy],
            config_json_schema: None,
        }
    }
    fn configure(_config_json: String) -> Result<(), types::StructuredError> {
        Ok(())
    }
}
impl permissions_policy_api::Guest for Fixture {
    fn decide(
        request: types::PermissionRequest,
    ) -> Result<types::PermissionDecision, types::StructuredError> {
        let action = match request.tool_name.as_str() {
            "dangerous" => types::PermissionAction::Deny,
            "ask_me" => types::PermissionAction::Ask,
            "contextual" => {
                if request.permission == "network"
                    && request.pattern == "network"
                    && !request.tool_call_id.is_empty()
                    && !request.session_id.is_empty()
                    && !request.run_id.is_empty()
                { types::PermissionAction::Allow } else { types::PermissionAction::Deny }
            }
            _ => types::PermissionAction::Allow,
        };
        Ok(types::PermissionDecision {
            action,
            reason: "fixture".into(),
        })
    }
}
export!(Fixture);
