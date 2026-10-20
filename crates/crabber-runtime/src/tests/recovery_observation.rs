//! Existing private fixture/suite, preserving actual native cuts and joins.
use super::*;

#[derive(Default)]
struct AttemptObserver(Mutex<Vec<(RunId, Option<crabber_core::TraceContext>)>>);
impl Observer for AttemptObserver {
    fn emit(&self, event: &EventRecord) {
        self.emit_with_context(event, None);
    }
    fn emit_with_context(&self, event: &EventRecord, context: Option<&crabber_core::TraceContext>) {
        self.0
            .lock()
            .unwrap()
            .push((event.run_id.clone(), context.cloned()));
    }
    fn model_completed_with_context(
        &self,
        event: &EventRecord,
        context: Option<&crabber_core::TraceContext>,
    ) {
        self.emit_with_context(event, context);
    }
}
#[tokio::test]
async fn recovery_selects_current_attempt_per_run_without_ambient_contamination() {
    let now = time::OffsetDateTime::now_utc();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let capture = Arc::new(AttemptObserver::default());
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .clock(clock.clone())
        .observer(capture.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"one"}"#),
            call_script(ToolCallId::new(), r#"{"text":"two"}"#),
            text_script("done"),
            text_script("done"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(executed.clone())))],
            Vec::new(),
        )))
        .policy(Arc::new(PausePolicy))
        .build()
        .unwrap();
    let prior = crabber_core::TraceContext::new("1234567890abcdef", "1234567890abcdef").unwrap();
    let mut ids = Vec::new();
    for _ in 0..2 {
        let handle = runtime
            .start_with_context(request(), Some(prior.clone()))
            .await
            .unwrap();
        ids.push(handle.run_id().clone());
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    }
    capture.0.lock().unwrap().clear();
    clock.set(now + time::Duration::seconds(31));
    let contexts = ["0000000000000001", "0000000000000002"].map(|trace| {
        crabber_core::TraceContext::new(trace, "1234567890abcdef")
            .unwrap()
            .linked_to(&prior)
            .unwrap()
    });
    let results = runtime
        .recover_with_context(|run| {
            Some(contexts[ids.iter().position(|id| id == &run.id).unwrap()].clone())
        })
        .await
        .unwrap()
        .recovered;
    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|result| result.status == RunStatus::Completed)
    );
    assert_eq!(executed.load(Ordering::SeqCst), 2);
    for (i, id) in ids.iter().enumerate() {
        let events = capture.0.lock().unwrap();
        assert!(events.iter().any(|(run, _)| run == id));
        assert!(
            events
                .iter()
                .filter(|(run, _)| run == id)
                .all(|(_, context)| context.as_ref() == Some(&contexts[i]))
        );
    }
}

#[tokio::test]
async fn linked_expired_legacy_recovery_cannot_grant_dispatch() {
    for running in [false, true] {
        let capture = Arc::new(AttemptObserver::default());
        let (runtime, store, executed, run_id) =
            crashed_run_with_observer(running, capture.clone()).await;
        let before = store.list_unfinished_tool_calls(&run_id).await.unwrap();
        let current =
            crabber_core::TraceContext::new("9876543210abcdef", "9876543210abcdef").unwrap();
        let report = runtime
            .recover_with_context(|_| Some(current.clone()))
            .await
            .unwrap();
        assert_eq!(report.recovered, Vec::new());
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(
            report.skipped[0].reason,
            crate::RecoverySkipReason::Attempt(crabber_session::AttemptError::InvalidTransition)
        );
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.list_unfinished_tool_calls(&run_id).await.unwrap(),
            before
        );
        assert!(capture.0.lock().unwrap().is_empty());
    }
}
