use super::*;
use crabber_extension::{
    SessionInvocationAdmission, SessionInvocationError, SessionInvocationRequest,
};
use crabber_session::session_owner::SessionOwnerState;
struct DetachedTool {
    fault: usize,
    effects: Arc<AtomicUsize>,
    request: Arc<Mutex<Option<SessionInvocationRequest>>>,
}
#[async_trait]
impl JsonToolExecutor for DetachedTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("context required")
    }
    async fn execute_with_context(
        &self,
        context: crabber_extension::ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let request = SessionInvocationRequest::new(
            "runtime.detached.fault".into(),
            crabber_core::InputFingerprint::new("c".repeat(64)).unwrap(),
        );
        *self.request.lock().unwrap() = Some(request.clone());
        let effects = self.effects.clone();
        let outcome = context
            .session_invocations()
            .spawn(request.clone(), move |_| async move {
                effects.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await;
        if self.fault < 3 {
            assert_eq!(outcome, Err(SessionInvocationError::UnknownOutcome));
        } else {
            assert_eq!(outcome, Ok(SessionInvocationAdmission::Started));
        }
        assert_eq!(
            context
                .session_invocations()
                .spawn(request, |_| async { panic!("receipt replay dispatched") })
                .await,
            Ok(SessionInvocationAdmission::Replayed)
        );
        Ok(json!({"reconciled":true}))
    }
}
#[tokio::test(start_paused = true)]
async fn detached_owner_ack_loss_never_dispatches_from_metadata() {
    for fault in 1..=3 {
        let memory = Arc::new(MemoryStore::new());
        let gate = Arc::new(SettlementGate::default());
        gate.release.notify_one();
        gate.session_owner_ack_lost.store(fault, Ordering::SeqCst);
        let store = Arc::new(DelayedTerminalStore {
            inner: memory.clone(),
            gate,
        });
        let effects = Arc::new(AtomicUsize::new(0));
        let request_id = Arc::new(Mutex::new(None));
        let runtime = orchestrator(
            store,
            FakeProvider::scripted(vec![
                call_script(ToolCallId::new(), r#"{"text":"detach"}"#),
                text_script("done"),
            ]),
            Arc::new(DetachedTool {
                fault,
                effects: effects.clone(),
                request: request_id.clone(),
            }),
            Arc::new(RecordingObserver::default()),
        );
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
        let reports = runtime
            .drain_session_invocations(&session, false, Duration::from_secs(2))
            .await;
        assert!(reports[0].local_joined);
        let request = request_id.lock().unwrap().clone().unwrap();
        let owner = memory
            .get_session_owner(&session, &request.id)
            .await
            .unwrap();
        if fault < 3 {
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            assert_eq!(
                owner.state,
                if fault == 1 {
                    SessionOwnerState::ReservedUnstarted
                } else {
                    SessionOwnerState::Active
                }
            );
            assert_eq!(reports[0].status, crate::SessionInvocationStatus::Uncertain);
        } else {
            assert_eq!(effects.load(Ordering::SeqCst), 1);
            assert_eq!(owner.state, SessionOwnerState::Joined);
            assert_eq!(reports[0].status, crate::SessionInvocationStatus::Joined);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn pending_detached_cancel_before_begin_ack_skips_factory_and_reconciles_observe_ack() {
    let memory = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    gate.session_owner_cancel_on_begin
        .store(2, Ordering::SeqCst);
    gate.owner_observe_ack_lost.store(true, Ordering::SeqCst);
    let store = Arc::new(DelayedTerminalStore {
        inner: memory.clone(),
        gate: gate.clone(),
    });
    let effects = Arc::new(AtomicUsize::new(0));
    let request_id = Arc::new(Mutex::new(None));
    let runtime = orchestrator(
        store,
        FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"detach"}"#),
            text_script("done"),
        ]),
        Arc::new(DetachedTool {
            fault: 3,
            effects: effects.clone(),
            request: request_id.clone(),
        }),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let reports = runtime
        .drain_session_invocations(&session, false, Duration::from_secs(2))
        .await;
    assert!(reports[0].local_joined);
    assert_eq!(reports[0].status, crate::SessionInvocationStatus::Joined);
    let id = request_id.lock().unwrap().clone().unwrap();
    let owner = memory.get_session_owner(&session, &id.id).await.unwrap();
    assert_eq!(owner.state, SessionOwnerState::Joined);
    assert!(owner.cancellation.unwrap().observed.is_some());
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let keys = gate.owner_observe_keys.lock().unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
}

#[tokio::test]
async fn pending_preparation_cancel_before_begin_ack_preserves_blocker_without_factory_or_observe()
{
    let memory = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    gate.session_owner_cancel_on_begin
        .store(1, Ordering::SeqCst);
    let store = Arc::new(DelayedTerminalStore {
        inner: memory.clone(),
        gate,
    });
    let provider = FakeProvider::scripted(vec![text_script("never")]);
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .resolver(Arc::new(provider.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    let error = runtime
        .start(request())
        .await
        .err()
        .expect("pending cancellation");
    assert!(matches!(
        error.operation_error(),
        RuntimeError::SessionOwner(
            crabber_session::session_owner::SessionOwnerError::CancellationPending
        )
    ));
    let descriptor = error.preparation_reconciliation().unwrap();
    let owner = memory
        .get_session_owner(&descriptor.session_id, &descriptor.owner.id)
        .await
        .unwrap();
    assert_eq!(owner.state, SessionOwnerState::Active);
    assert!(owner.cancellation.unwrap().observed.is_none());
    assert!(
        memory
            .get_session(&descriptor.session_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(provider.requests().len(), 0);
}
