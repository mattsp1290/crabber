//! A real joined business failure retains only its exact root cancellation retry.
use super::{
    Arc, AtomicUsize, DelayedTerminalStore, Duration, ExtensionError, FakeProvider,
    InputFingerprint, MemoryStore, Orchestrator, Ordering, RunPlan, RunPlanProvider,
    SessionContext, SessionId, SessionOwnerState, SettlementGate, StaticPlanProvider, Store,
    async_trait, options, request, text_script,
};
use crabber_session::session_owner::{SessionOwnerOperation, SessionOwnerTermination};

struct FailingLegacyPlan {
    base: StaticPlanProvider,
    calls: AtomicUsize,
}
#[async_trait]
impl RunPlanProvider for FailingLegacyPlan {
    fn registration_fingerprint(
        &self,
        context: &SessionContext,
    ) -> Result<InputFingerprint, ExtensionError> {
        self.base.registration_fingerprint(context)
    }
    async fn acquire_plan(&self, _: &SessionId) -> Result<RunPlan, ExtensionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(ExtensionError::RegistryClosed)
    }
}

#[tokio::test]
async fn joined_legacy_failure_reconciles_original_cancelled_ack_without_factory_retry() {
    let memory = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    gate.session_owner_ack_lost.store(4, Ordering::SeqCst);
    let store = Arc::new(DelayedTerminalStore {
        inner: memory.clone(),
        gate: gate.clone(),
    });
    let plans = Arc::new(FailingLegacyPlan {
        base: StaticPlanProvider::new(vec![], vec![]),
        calls: AtomicUsize::new(0),
    });
    let fake = FakeProvider::scripted(vec![text_script("never invoked")]);
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(plans.clone())
        .build()
        .unwrap();
    let mut input = request();
    input.session_id = Some(SessionId::new());
    let error = runtime.start_keyed(input, options()).await.err().unwrap();
    assert!(error.admission_limit().is_none());
    assert!(matches!(
        error.operation_error(),
        crate::RuntimeError::PreparationRecoveryRequired
    ));
    let descriptor = error.preparation_reconciliation().unwrap().clone();
    let receipt = memory
        .lookup_session_owner(&descriptor.session_id, &descriptor.join_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.operation, SessionOwnerOperation::Join);
    assert_eq!(receipt.owner.target, descriptor.owner);
    assert_eq!(receipt.owner.state, SessionOwnerState::Joined);
    assert_eq!(
        receipt.owner.termination,
        Some(SessionOwnerTermination::Cancelled)
    );
    assert_eq!(gate.legacy_cancel_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        runtime
            .retained_preparation_status(&descriptor.session_id)
            .len(),
        1
    );
    assert!(
        runtime
            .retained_preparation_heartbeat_stop(&descriptor)
            .unwrap()
            .is_cancelled()
    );
    let before = runtime.begin_close().snapshot();
    assert_eq!(
        before.uncertain_preparations.as_slice(),
        std::slice::from_ref(&descriptor)
    );
    let observed = runtime.close(Duration::from_secs(2)).await;
    assert!(observed.local_drained(), "{observed:?}");
    assert_eq!(observed.uncertain_preparations, []);
    assert_eq!(observed.pending_enrollments, []);
    assert_eq!(
        runtime.retained_preparation_status(&descriptor.session_id),
        []
    );
    assert_eq!(
        memory
            .lookup_session_owner(&descriptor.session_id, &descriptor.join_id)
            .await
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(gate.legacy_cancel_calls.load(Ordering::SeqCst), 1);
    assert_eq!(plans.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.requests().len(), 0);
    assert!(runtime.close(Duration::from_secs(2)).await.local_drained());
    assert_eq!(plans.calls.load(Ordering::SeqCst), 1);
}
