//! Withhold the ACK only after the original inner native conclusion commits.
use super::super::{
    Error, done, selection,
    store::{AuditedStore, BoundaryAck},
};
use crabber::{
    Agent, AgentConfig, FakeProvider, Observer, RuntimeError,
    core::{EventKind, EventRecord, RunStatus},
    session::{AttemptCleanup, AttemptError, AttemptState, Store},
};
use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

#[derive(Default)]
struct Capture(Mutex<Vec<EventKind>>);
impl Observer for Capture {
    fn emit(&self, event: &EventRecord) {
        self.0.lock().unwrap().push(event.kind.clone());
    }
}
pub(super) async fn run(inner: Arc<dyn Store>) -> Result<(), Error> {
    let ack = Arc::new(BoundaryAck::default());
    let store = Arc::new(AuditedStore::with_boundary_ack_loss(
        inner.clone(),
        ack.clone(),
    ));
    let provider = FakeProvider::scripted(vec![done()]);
    let capture = Arc::new(Capture::default());
    let host = Agent::builder()
        .behavior_version("public-native-conclusion-response-loss.v1")
        .store(store)
        .provider(Arc::new(provider.clone()))
        .observer(capture.clone())
        .config(AgentConfig::new(selection()))
        .build()?;
    let handle = host
        .prompt(None, "one terminal commit, unknown original response")
        .await?;
    let run = handle.run_id().clone();
    let session = handle.session_id().clone();
    let result = tokio::time::timeout(Duration::from_secs(30), handle.done()).await?;
    assert!(
        matches!(result, Err(RuntimeError::AttemptBoundaryUnknown {run_id, reason: AttemptError::UnknownStoreOutcome}) if run_id == run)
    );
    // Local supervisors may still retire after the business result; observe their actual joins before the read-only comparison.
    let drained = host.drain_execution_tasks(Duration::from_secs(30)).await;
    assert_eq!(drained.pending, Vec::<crabber::core::RunId>::new());
    assert_eq!(
        drained.pending_preparations,
        Vec::<crabber::runtime::PreparationTaskId>::new()
    );
    assert_eq!(ack.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(
        inner.get_run(&run).await?.unwrap().status,
        RunStatus::Completed
    );
    let attempt = inner.latest_execution_attempt(&run).await?.unwrap().attempt;
    assert_eq!(attempt.state, AttemptState::Completed);
    assert_eq!(attempt.cleanup, AttemptCleanup::Complete);
    let events = inner.list_events(&session, None, 1000).await?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::RunSettled)
            .count(),
        1
    );
    assert!(!capture.0.lock().unwrap().contains(&EventKind::RunSettled));
    let (original, actual_receipt) = ack.committed.lock().unwrap().take().unwrap();
    let revision = inner.session_revision(&session).await?;
    for _ in 0..2 {
        let receipt = inner
            .lookup_attempt_mutation(&session, &original.finish.id)
            .await?
            .unwrap();
        assert_eq!(receipt, actual_receipt);
        original.verify_replay(&receipt)?;
        assert_eq!(
            receipt.semantic_digest,
            original.fingerprint(&session, &run)?
        );
    }
    assert_eq!(inner.session_revision(&session).await?, revision);
    assert_eq!(inner.list_events(&session, None, 1000).await?, events);
    assert_eq!(ack.calls.load(Ordering::SeqCst), 1);
    host.close_extensions().await?;
    Ok(())
}
