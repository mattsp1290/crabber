crabber_guest::generate!(world: "event-sink");
use crabber::extensions::types;
use exports::crabber::extensions::{event_sink_api, manifest_api};
struct Fixture;
impl manifest_api::Guest for Fixture {
    fn describe() -> types::ExtensionManifest {
        types::ExtensionManifest {
            id: "counter-sink".into(),
            version: "0.1.0".into(),
            roles: vec![types::Role::EventSink],
            config_json_schema: None,
        }
    }
    fn configure(_config_json: String) -> Result<(), types::StructuredError> {
        Ok(())
    }
}
impl event_sink_api::Guest for Fixture {
    fn emit(event: types::BoundedEvent) -> Result<(), types::StructuredError> {
        // Direct host-boundary tests write through the same real state import;
        // ordinary Agent events retain the counter behavior below.
        if event.kind == "test-state-write" {
            return crabber::host::state::set(&event.run_id, &event.payload_summary)
                .map_err(state_error);
        }
        let count = crabber::host::state::get("count")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        crabber::host::state::set("count", &(count + 1).to_string()).map_err(state_error)
    }
}
fn state_error(message: String) -> types::StructuredError {
    types::StructuredError {
        code: "state".into(),
        message,
        retryable: false,
    }
}
export!(Fixture);
