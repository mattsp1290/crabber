//! Credential-free public embedding journeys, without a server or projection.
//! These fixtures do not certify whole-runtime resource coverage or delivery.
mod controls;
mod fixed_memory;
mod frontend;
mod interactive;
mod ordinary;
mod shared_services;
pub mod store;
mod unsupported_store;
use crabber::{
    core::{ControlId, SessionId, SessionRevision, ToolCallId},
    session::{DeleteSession, DeletionError, Store, StoreError},
};
use serde::Serialize;
use std::sync::Arc;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Serialize)]
pub struct Report {
    pub contexts: usize,
    pub scoped_instances: usize,
    pub ordinary_effects: usize,
    pub interactive_starts: usize,
    pub interactive_resumes: usize,
    pub interactive_effects: usize,
    pub frontend_resumes: usize,
    pub fixed_sessions: usize,
    pub shared_service_reads: usize,
}

/// Execute public host flows against an injected Store. The Store alone remains
/// authoritative. Host authentication is deliberately outside this offline probe.
/// # Errors
/// Returns failed Store/runtime controls or an unavailable selected backend.
/// # Panics
/// Panics if an identity, effect count, lifecycle or replay invariant fails.
pub async fn run(store: Arc<dyn Store>) -> Result<Report, Error> {
    let cwd = std::env::current_dir()?;
    unsupported(store.clone()).await?;
    let (scoped_instances, ordinary_effects, source) = ordinary::run(store.clone()).await?;
    let (interactive_starts, interactive_resumes, interactive_effects) =
        interactive::run(store.clone()).await?;
    let frontend_resumes = frontend::run(store.clone()).await?;
    let shared_service_reads = shared_services::run(store.clone()).await?;
    controls::run(store, source).await?;
    let fixed_sessions = fixed_memory::run().await?;
    assert_eq!(std::env::current_dir()?, cwd);
    Ok(Report {
        contexts: 2,
        scoped_instances,
        ordinary_effects,
        interactive_starts,
        interactive_resumes,
        interactive_effects,
        frontend_resumes,
        fixed_sessions,
        shared_service_reads,
    })
}
async fn unsupported(inner: Arc<dyn Store>) -> Result<(), Error> {
    let store = unsupported_store::UnsupportedStore { inner };
    assert_eq!(
        store
            .delete_session(DeleteSession {
                id: ControlId::new(),
                session_id: SessionId::new(),
                expected_revision: SessionRevision::INITIAL
            })
            .await
            .unwrap_err(),
        DeletionError::Unsupported
    );
    let provider = crabber::FakeProvider::scripted(vec![]);
    let agent = crabber::Agent::builder()
        .behavior_version("unsupported-probe.v1")
        .store(Arc::new(store))
        .provider(Arc::new(provider.clone()))
        .config(crabber::AgentConfig::new(selection()))
        .build()?;
    let error = agent
        .prompt(None, "unsupported optional host binding")
        .await
        .err()
        .expect("unsupported adapter must not admit");
    assert!(
        matches!(
            error,
            crabber::RuntimeError::Store(StoreError::HostUnsupported)
        ),
        "{error:?}"
    );
    assert_eq!(provider.requests().len(), 0);
    agent.close_extensions().await?;
    Ok(())
}
pub(crate) fn selection() -> crabber::Selection {
    crabber::Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    }
}
pub(crate) fn call(name: &str) -> Vec<crabber::StreamDelta> {
    let id = ToolCallId::new();
    vec![
        crabber::StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: name.into(),
        },
        crabber::StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: "{}".into(),
        },
        crabber::StreamDelta::ToolCallDone { call_id: id },
        crabber::StreamDelta::Completed,
    ]
}
pub(crate) fn done() -> Vec<crabber::StreamDelta> {
    vec![
        crabber::StreamDelta::TextDelta("done".into()),
        crabber::StreamDelta::Completed,
    ]
}
