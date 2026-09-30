crabber_guest::generate!(world: "context-source");
use crabber::extensions::types;
use exports::crabber::extensions::{context_source_api, manifest_api};
struct Fixture;
impl manifest_api::Guest for Fixture {
    fn describe() -> types::ExtensionManifest {
        types::ExtensionManifest {
            id: "banner-context".into(),
            version: "0.1.0".into(),
            roles: vec![types::Role::ContextSource],
            config_json_schema: None,
        }
    }
    fn configure(_config_json: String) -> Result<(), types::StructuredError> {
        Ok(())
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
export!(Fixture);
