#![doc = include_str!("../../../README.md")]

pub use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};

mod agent;

pub use agent::{Admission, Agent, AgentBuilder, AgentConfig, BuildError, RunEvents, RunHandle};
pub use crabber_core::{
    AdmissionKey, AdmissionOptions, AdmissionReceipt, EventKind, EventRecord, InputFingerprint,
    SessionId,
};
pub use crabber_extension::{ExtensionError, ToolDefinition, ToolExecutor};
pub use crabber_providers::{FakeProvider, Selection, StreamDelta};
pub use crabber_runtime::{PermissionDecision, RunResult, RuntimeError, StaticPolicy};

#[cfg(feature = "codex")]
pub use crabber_auth as auth;
pub use crabber_core as core;
pub use crabber_extension as extension;
pub use crabber_providers as providers;
pub use crabber_runtime as runtime;
pub use crabber_session as session;
#[cfg(feature = "wasm")]
pub use crabber_wasm as wasm;

#[cfg(test)]
mod tests {
    #[test]
    fn core_is_available() {
        assert_eq!(crate::core::CRATE_NAME, "crabber-core");
    }
}
