//! Lossy tool-result reduction using host-provisioned RTK v0.51.0.
//!
//! RTK runs with host-user authority; private directories are not a sandbox.
//! Hosts supply an immutable executable and trusted temporary root. Digest
//! verification occurs only at construction. Mount a final redactor to protect
//! recognizable secrets after reduction; lossy filters can truncate secrets
//! into prefixes a redactor cannot recognize. Closing any mount closes the shared
//! instance permanently. A killed host may leave children and private roots;
//! hosts own stale-state cleanup and descendants that leave the process group.
//! Reap-timeout and panic quarantine are proved with private unit-test seams.

mod child;
mod config;
mod engine;
mod private_dirs;
#[cfg(all(test, unix))]
mod tests;
mod transform;

pub use config::{
    Binding, DEFAULT_ORDER, Field, Filter, InputMatch, Limits, Options, RTK_VERSION, Selection,
};
use crabber::extension::{Extension, ExtensionError, Registrar, TransformOutput};
pub use engine::Stats;
use std::sync::Arc;

/// Ordinary-phase result-transform identity.
pub const HANDLER_ID: &str = "rtk-reducer";
/// Prefix for an accepted replacement; original_bytes counts input UTF-8 bytes.
pub fn marker(filter: Filter, original_bytes: usize) -> String {
    format!(
        "[Reduced by RTK {}; original_bytes={original_bytes}]\n",
        filter.cli_name()
    )
}
/// One shared instance with bounded, non-queueing transform capacity.
///
/// Install one per registry. Crabber permits repeated mounts, which run each
/// handler independently; shared mounts share capacity and close together.
pub struct RtkReducer {
    engine: Arc<engine::Engine>,
    transformer: Arc<transform::Transformer>,
    hash: String,
}
impl RtkReducer {
    /// Validate policy and stream-verify the executable digest without spawning.
    /// Non-Unix platforms reject construction before touching paths or limits.
    pub fn new(options: Options) -> Result<Self, ExtensionError> {
        let (configuration, hash) = config::validate(options)?;
        let engine = Arc::new(engine::Engine::new(configuration));
        Ok(Self {
            transformer: Arc::new(transform::Transformer::new(engine.clone())),
            engine,
            hash,
        })
    }
    /// Starting children, unreaped survivors and directories awaiting removal.
    /// No text, paths or PIDs are exposed.
    pub fn live_children(&self) -> usize {
        self.engine.live_children()
    }
    /// Independently read wrapping counters and lifecycle flags.
    pub fn stats(&self) -> Stats {
        self.engine.stats()
    }
}
#[async_trait::async_trait]
impl Extension for RtkReducer {
    fn id(&self) -> &str {
        "crabber-extensions/rtk-reducer"
    }
    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    fn config_hash(&self) -> String {
        self.hash.clone()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let transformer = self.transformer.clone();
        registrar.on_result_transform(
            self.engine.configuration.order,
            HANDLER_ID,
            Arc::new(move |context, value| {
                let transformer = transformer.clone();
                Box::pin(async move {
                    let Ok(json) = value.json_value() else {
                        return Ok(TransformOutput::new(value));
                    };
                    let transformed = transformer.transform(context, json).await;
                    let output = crabber::core::ToolOutput::from_json(&transformed)
                        .unwrap_or_else(|_| value.clone());
                    Ok(TransformOutput::new(if value.is_error() {
                        output.with_error()
                    } else {
                        output
                    }))
                })
            }),
        );
        Ok(())
    }
    async fn shutdown(&self) {
        let _ = self
            .engine
            .close(self.engine.configuration.limits.shutdown_grace)
            .await;
    }
}
