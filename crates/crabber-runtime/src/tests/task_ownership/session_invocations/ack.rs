//! Joined physical work does not corroborate changed native acknowledgement fields.
use super::*;
struct JoinAckTool {
    gate: Arc<SettlementGate>,
    fault: usize,
    effects: Arc<AtomicUsize>,
}
#[async_trait]
impl JsonToolExecutor for JoinAckTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("context required")
    }
    async fn execute_with_context(
        &self,
        context: crabber_extension::ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let gate = self.gate.clone();
        let fault = self.fault;
        let effects = self.effects.clone();
        let request = SessionInvocationRequest::new(
            "runtime.detached.join-ack".into(),
            crabber_core::InputFingerprint::new("f".repeat(64)).unwrap(),
        );
        assert_eq!(
            context
                .session_invocations()
                .spawn(request, move |_| async move {
                    effects.fetch_add(1, Ordering::SeqCst);
                    gate.malformed_owned_join.store(fault, Ordering::SeqCst);
                    Ok(())
                })
                .await,
            Ok(SessionInvocationAdmission::Started)
        );
        Ok(json!({"detached":true}))
    }
}
#[tokio::test]
async fn detached_join_rejects_changed_native_ack_fields_without_repeating_work() {
    for fault in [3, 5, 6] {
        changed_ack(fault).await;
    }
}
async fn changed_ack(fault: usize) {
    let memory = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let effects = Arc::new(AtomicUsize::new(0));
    let runtime = orchestrator(
        Arc::new(DelayedTerminalStore {
            inner: memory,
            gate: gate.clone(),
        }),
        FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"detach"}"#),
            text_script("done"),
        ]),
        Arc::new(JoinAckTool {
            gate,
            fault,
            effects: effects.clone(),
        }),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let report = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let reports = runtime
                .drain_session_invocations(&session, false, Duration::from_millis(1))
                .await;
            if reports[0].local_joined
                || reports[0].status == crate::SessionInvocationStatus::Uncertain
            {
                break reports.into_iter().next().unwrap();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        report.status,
        crate::SessionInvocationStatus::Uncertain,
        "fault {fault}"
    );
    assert!(!report.local_joined);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let joined = runtime
        .drain_session_invocations(&session, false, Duration::from_secs(2))
        .await;
    assert!(joined[0].local_joined);
    assert_eq!(joined[0].status, crate::SessionInvocationStatus::Joined);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}
