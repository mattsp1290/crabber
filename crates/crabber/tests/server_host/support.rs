//! Shared public-only host fixture vocabulary; no test cases live here.
pub use async_trait::async_trait;
pub use crabber::{
    Admission, AdmissionKey, AdmissionOptions, Agent, AgentConfig, ExecutionContext,
    ExtensionError, FakeProvider, InputFingerprint, JsonToolExecutor, PermissionDecision,
    RuntimeError, Scope, Selection, SessionContext, SessionId, StaticPolicy, StreamDelta,
    ToolDefinition,
    core::{CoreError, RunStatus, ToolCallId, ToolInfo},
    extension::{Extension, Registrar, ToolContext},
    session::{MemoryStore, Store},
};
pub use crabber::{ExtensionFactory, FactoryContext, FactoryScope, extension::ComponentIdentity};
pub use serde_json::{Value, json};
pub use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[must_use]
pub fn selection(model: &str) -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: model.into(),
    }
}
#[must_use]
pub fn context(session: &str, workspace: &str, directory: &str, model: &str) -> ExecutionContext {
    ExecutionContext::new(
        SessionContext::new(session.into(), workspace, directory),
        selection(model),
    )
}
/// # Panics
/// Panics if a constant fixture fingerprint or key becomes invalid.
#[must_use]
pub fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("host-input").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}
#[must_use]
pub fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
#[must_use]
pub fn call(id: &str) -> Vec<StreamDelta> {
    vec![
        StreamDelta::ToolCallStart {
            call_id: ToolCallId::from(id),
            name: "identity".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: ToolCallId::from(id),
            text: "{\"directory\":\"forged\"}".into(),
        },
        StreamDelta::ToolCallDone {
            call_id: ToolCallId::from(id),
        },
        StreamDelta::Completed,
    ]
}

pub struct ForbiddenFactory(pub Arc<AtomicUsize>);
#[async_trait]
impl ExtensionFactory for ForbiddenFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "management-factory".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, _: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("metadata control must never initialize a factory");
    }
}

/// Shared fixed-profile limits for actual public lifecycle fixtures.
#[must_use]
pub fn aggregate_limits() -> crabber::PublicSessionLimitsV2 {
    crabber::PublicSessionLimitsV2 {
        header_bytes: 65536,
        proof_bytes: 32 << 20,
        approval_interactions: 64,
        tool_requirements: 64,
        queue_entries: 64,
        registered_owners: 1024,
        run_controls: 64,
        history: crabber::session::SnapshotLimits {
            messages: 1,
            tool_calls: 1,
            parts: 256,
            text_bytes: 4096,
            encoded_bytes: 4096,
        },
    }
}
