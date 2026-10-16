//! Pure fixed callback inventory; opaque callbacks never gain cleanup credit.
use super::{Orchestrator, OrchestratorBuilder, RuntimeError};
use crabber_core::InputFingerprint;
use crabber_extension::ComponentIdentity;
use crabber_session::{StoreError, admission_config_hash};
use serde_json::json;
pub(super) const COMPONENT: &str = "contract:crabber/runtime-host-callbacks.v1";

/// Changing the fixed slots or builtin lifetime behavior requires a new domain.
/// The host behavior identity independently versions opaque callback semantics.
pub(super) fn capture(builder: &OrchestratorBuilder) -> InputFingerprint {
    let observer = builder.observer.is_some();
    #[cfg(test)]
    let observer = observer || builder.context_observer.is_some();
    let declaration = json!([
        "crabber.runtime.host-callback-inventory.v1",
        [
            ["wall-clock", builder.clock.is_some()],
            ["monotonic-clock", builder.monotonic_clock.is_some()],
            ["observer", observer],
            ["model-stream", builder.model_stream.is_some()],
            ["permission-policy", builder.policy.is_some()],
            ["tool-pipeline", builder.tool_pipeline.is_some()],
            ["host-services", builder.host_services.is_some()],
            [
                "workspace-readers",
                builder.workspace_reader_resolver.is_some()
            ]
        ]
    ]);
    InputFingerprint::new(admission_config_hash(&declaration)).expect("SHA256 fingerprint")
}
impl Orchestrator {
    pub(super) fn verify_host_callbacks(
        &self,
        components: &[ComponentIdentity],
    ) -> Result<(), RuntimeError> {
        let mut declarations = components
            .iter()
            .filter(|component| component.id == COMPONENT);
        let original = declarations.next().ok_or(StoreError::AdmissionConflict)?;
        if declarations.next().is_some() || original.version != self.host_callbacks.as_str() {
            return Err(StoreError::AdmissionConflict.into());
        }
        Ok(())
    }
}
