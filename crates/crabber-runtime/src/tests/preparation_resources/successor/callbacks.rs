//! The pending task uses the same actual nested callback owner as normal workers.
use super::*;
struct Provider {
    cleanup: Arc<ScopedProvider>,
    registry: Arc<crabber_extension::Registry>,
}
#[async_trait]
impl RunPlanProvider for Provider {
    fn registration_fingerprint(
        &self,
        context: &SessionContext,
    ) -> Result<InputFingerprint, ExtensionError> {
        self.registry.registration_fingerprint(context)
    }
    fn preparation_resource_contract(
        &self,
        context: &SessionContext,
    ) -> Result<Option<PreparationResourceContract>, ExtensionError> {
        self.cleanup.preparation_resource_contract(context)
    }
    async fn acquire_plan(&self, _: &SessionId) -> Result<RunPlan, ExtensionError> {
        panic!("owned provider must use actual preparation")
    }
    async fn acquire_plan_in_preparation(
        &self,
        scope: &PreparationResourceContext,
    ) -> Result<RunPlan, ExtensionError> {
        self.cleanup.acquire_plan_in_preparation(scope).await?;
        self.registry
            .acquire_plan(scope.session().session_id())
            .await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_first_two_retain_nested_callback_before_native_join() {
    for expired in [false, true] {
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let memory = Arc::new(MemoryStore::with_clock(clock.clone()));
        let gate = Arc::new(SettlementGate::default());
        gate.release.notify_one();
        let wrapped = Arc::new(DelayedTerminalStore {
            inner: memory.clone(),
            gate: gate.clone(),
        });
        let mut h =
            Harness::on_store_with_clock(wrapped.clone(), memory, gate, Some(clock.clone()));
        let mut nested = crate::tests::callback_ownership::nested::NestedFixture::new().await;
        h.runtime = Orchestrator::builder()
            .behavior_fingerprint(options().behavior_fingerprint)
            .store(wrapped)
            .resolver(Arc::new(h.fake.clone()))
            .plan_provider(Arc::new(Provider {
                cleanup: h.provider.clone(),
                registry: nested.registry.clone(),
            }))
            .clock(clock.clone())
            .build()
            .unwrap();
        let receipt = initial(&h, &clock, expired).await;
        let handle = start(&h, &receipt, expired).await;
        nested.wait_disposing().await;
        assert_eq!(state(&h).await, SessionOwnerState::Active);
        let report = h
            .runtime
            .drain_execution_tasks(Duration::from_millis(30))
            .await;
        assert!(report.pending.contains(&receipt.run_id));
        nested.release();
        assert!(handle.done().await.is_err());
        // The business error does not replace physical cleanup. The original
        // callback was actually disposed before this native driver Join.
        assert_eq!(state(&h).await, SessionOwnerState::Joined);
        cleanup(&h, expired).await;
        nested.registry.close_all().await.unwrap();
    }
}
