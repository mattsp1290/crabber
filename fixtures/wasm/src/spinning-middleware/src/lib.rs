crabber_guest::generate!(world: "tool-middleware");
use crabber::extensions::types;
use exports::crabber::extensions::{manifest_api, tool_middleware_api};

struct Fixture;
impl manifest_api::Guest for Fixture {
    fn describe() -> types::ExtensionManifest {
        types::ExtensionManifest {
            id: "spinning-middleware".into(),
            version: "0.1.0".into(),
            roles: vec![types::Role::ToolMiddleware],
            config_json_schema: None,
        }
    }
    fn configure(_: String) -> Result<(), types::StructuredError> {
        Ok(())
    }
}
impl tool_middleware_api::Guest for Fixture {
    fn before_tool_call(
        _: String,
        _: String,
        _: String,
        _: types::TurnMetadata,
    ) -> types::Replacement {
        types::Replacement::Unchanged
    }
    fn after_tool_call(
        _: String,
        _: String,
        _: String,
        _: String,
        _: bool,
        _: types::TurnMetadata,
    ) -> types::Replacement {
        crabber::host::log::log(crabber::host::log::Level::Info, "spin-ready");
        loop {
            std::hint::black_box(42_u64);
        }
    }
}
export!(Fixture);
