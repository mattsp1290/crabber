//! The actual registered callback returns, but its literal future fails disposal.
use super::super::{Error, done, selection};
use async_trait::async_trait;
use crabber::{
    ExtensionError, FakeProvider, RuntimeError,
    core::{EventKind, InputFingerprint, ManualClock},
    extension::{Extension, Point, Registrar, Registry, Scope, TurnPrepare},
    runtime::{Orchestrator, RecoverySkipReason, Request},
    session::{
        AdmissionSelectionPolicy, AttemptCleanup, AttemptError, AttemptState, MemoryStore, Store,
    },
};
use serde_json::Value;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

struct FailedDisposal(Arc<AtomicUsize>);
impl Future for FailedDisposal {
    type Output = Result<Value, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(Value::Null))
    }
}
impl Drop for FailedDisposal {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("public probe original callback disposal failure");
    }
}
struct Hook(Arc<AtomicUsize>);
#[async_trait]
impl Extension for Hook {
    fn id(&self) -> &'static str {
        "probe-failed-callback"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "literal-drop-failure.v1".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let drops = self.0.clone();
        registrar.on_hook(
            TurnPrepare::ID,
            0,
            "literal-drop-failure",
            Arc::new(move |_| Box::pin(FailedDisposal(drops.clone()))),
        );
        Ok(())
    }
}
pub(super) async fn run(store: Arc<dyn Store>) -> Result<(), Error> {
    observe(store, None).await?;
    // The recovery discovery roster belongs to this dedicated native Memory store,
    // not to other probe journeys or to a synthetic Store response.
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let memory = Arc::new(MemoryStore::with_clock(clock.clone()));
    observe(memory, Some(clock)).await
}
async fn observe(store: Arc<dyn Store>, clock: Option<Arc<ManualClock>>) -> Result<(), Error> {
    let registry = Arc::new(Registry::new().with_close_timeout(Duration::from_secs(1)));
    let drops = Arc::new(AtomicUsize::new(0));
    registry
        .mount_owned(Arc::new(Hook(drops.clone())), Scope::Global)
        .await?;
    let provider = FakeProvider::scripted(vec![done()]);
    let builder = Orchestrator::builder()
        .behavior_fingerprint(InputFingerprint::new("c".repeat(64))?)
        .store(store.clone())
        .resolver(Arc::new(provider.clone()))
        .plan_provider(registry.clone());
    let runtime = match &clock {
        Some(clock) => builder.clock(clock.clone()),
        None => builder,
    }
    .build()?;
    let handle = runtime.start(request()).await?;
    let run = handle.run_id().clone();
    let session = handle.session_id().clone();
    let result = tokio::time::timeout(Duration::from_secs(30), handle.done()).await?;
    assert!(
        matches!(result, Err(RuntimeError::AttemptCleanupIncomplete { run_id }) if run_id == run)
    );
    let attempt = store.latest_execution_attempt(&run).await?.unwrap().attempt;
    assert_eq!(attempt.state, AttemptState::Started);
    assert_ne!(attempt.cleanup, AttemptCleanup::Complete);
    assert_eq!(provider.requests().len(), 0);
    assert!(
        !store
            .list_events(&session, None, 1000)
            .await?
            .iter()
            .any(|event| event.kind == EventKind::RunSettled)
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    if let Some(clock) = clock {
        skipped_original(&runtime, &*store, &clock, &session, &run).await?;
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(provider.requests().len(), 0);
    }
    // A failed literal Drop is observed as failure, never converted into a clean Join.
    assert!(registry.close_all().await.is_err());
    Ok(())
}

async fn skipped_original(
    runtime: &Orchestrator,
    store: &dyn Store,
    clock: &ManualClock,
    session: &crabber::core::SessionId,
    run: &crabber::core::RunId,
) -> Result<(), Error> {
    let original = store.get_run(run).await?.unwrap();
    let attempt = store.latest_execution_attempt(run).await?.unwrap();
    let events = store.list_events(session, None, 1000).await?;
    let calls = store.list_unfinished_tool_calls(run).await?;
    clock.set(original.lease_until + time::Duration::seconds(1));
    let report = tokio::time::timeout(Duration::from_secs(30), runtime.recover()).await??;
    assert_eq!(report.recovered.len(), 0);
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(&report.skipped[0].run_id, run);
    assert_eq!(&report.skipped[0].session_id, session);
    assert_eq!(
        report.skipped[0].reason,
        RecoverySkipReason::Attempt(AttemptError::Busy)
    );
    assert_eq!(store.get_run(run).await?, Some(original));
    assert_eq!(store.latest_execution_attempt(run).await?, Some(attempt));
    assert_eq!(store.list_events(session, None, 1000).await?, events);
    assert_eq!(store.list_unfinished_tool_calls(run).await?, calls);
    Ok(())
}

fn request() -> Request {
    Request {
        selection_policy: AdmissionSelectionPolicy::Explicit,
        captured_revision: None,
        input_envelope: None,
        session_id: None,
        workspace_id: "callback-probe".into(),
        directory: "/callback-probe".into(),
        title: "failed original disposal".into(),
        text: "must not reach provider".into(),
        selection: selection(),
        system_prompt: None,
        max_output_tokens: None,
        model_controls: crabber::core::ModelControls::default(),
    }
}
