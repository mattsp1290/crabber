//! Actual bridge acquisition, native ACK loss and independent physical cleanup.
#[path = "preparation_resources/cleanup_failure.rs"]
mod cleanup_failure;
#[path = "preparation_resources/driver.rs"]
mod driver;
#[path = "preparation_resources/faults.rs"]
mod faults;
#[path = "preparation_resources/heartbeat.rs"]
pub(super) mod heartbeat;
#[path = "preparation_resources/pending_approval.rs"]
mod pending_approval;
#[path = "preparation_resources/provider_binding.rs"]
mod provider_binding;
#[path = "preparation_resources/recovery.rs"]
mod recovery;
#[path = "preparation_resources/services.rs"]
mod services;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[path = "preparation_resources/sql.rs"]
mod sql;
#[path = "preparation_resources/successor.rs"]
mod successor;
use super::*;
use crabber_core::{AdmissionKey, AdmissionOptions, InputFingerprint, SessionContext};
use crabber_extension::{
    ComponentIdentity, PreparationResourceContext, PreparationResourceContract, RunPlan,
};
use crabber_session::session_owner::{
    SessionOwnerState,
    preparation::{PreparationKey, PreparationReconciliation, PreparationResourceLink},
};
use std::time::Duration;

struct ScopedProvider {
    base: StaticPlanProvider,
    calls: AtomicUsize,
    policy: AtomicUsize,
    cleanup_started: Arc<Notify>,
    cleanup_release: Arc<Notify>,
    cleanup_completed: Arc<AtomicUsize>,
    cleanup_failure: Arc<AtomicUsize>,
    wait_for_close: std::sync::atomic::AtomicBool,
    entered: Notify,
}
impl ScopedProvider {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            base: StaticPlanProvider::new(vec![], vec![]),
            calls: AtomicUsize::new(0),
            policy: AtomicUsize::new(1),
            cleanup_started: Arc::new(Notify::new()),
            cleanup_release: Arc::new(Notify::new()),
            cleanup_completed: Arc::new(AtomicUsize::new(0)),
            cleanup_failure: Arc::new(AtomicUsize::new(0)),
            wait_for_close: std::sync::atomic::AtomicBool::new(false),
            entered: Notify::new(),
        })
    }
}
#[async_trait]
impl RunPlanProvider for ScopedProvider {
    fn registration_fingerprint(
        &self,
        context: &SessionContext,
    ) -> Result<InputFingerprint, ExtensionError> {
        self.base.registration_fingerprint(context)
    }
    fn preparation_resource_contract(
        &self,
        _: &SessionContext,
    ) -> Result<Option<PreparationResourceContract>, ExtensionError> {
        Ok(Some(PreparationResourceContract::new(
            ComponentIdentity {
                id: "owned-test-provider".into(),
                version: "1".into(),
            },
            InputFingerprint::new(format!("{:064x}", self.policy.load(Ordering::SeqCst))).unwrap(),
        )?))
    }
    async fn acquire_plan(&self, _: &SessionId) -> Result<RunPlan, ExtensionError> {
        panic!("declared bridge must never use legacy acquisition")
    }
    async fn acquire_plan_in_preparation(
        &self,
        scope: &PreparationResourceContext,
    ) -> Result<RunPlan, ExtensionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let release = self.cleanup_release.clone();
        let completed = self.cleanup_completed.clone();
        let failure = self.cleanup_failure.clone();
        // Actual cleanup is registered before acquiring any resource. The
        // separate witness below verifies that disposal alone does not join it.
        let (started, started_rx) = oneshot::channel();
        scope
            .cleanup()
            .defer(move || async move {
                let _ = started.send(());
                release.notified().await;
                completed.fetch_add(1, Ordering::SeqCst);
                match failure.load(Ordering::SeqCst) {
                    1 => Err(crabber_extension::ResourceFailure::CleanupFailed),
                    2 => panic!("actual scoped cleanup panicked"),
                    _ => Ok(()),
                }
            })
            .unwrap();
        // This notification task is also owned by the supplied scope, including
        // its real join; it is not a detached cleanup stand-in.
        let notify = self.cleanup_started.clone();
        scope
            .cleanup()
            .spawn(async move {
                let _ = started_rx.await;
                notify.notify_one();
                Ok(())
            })
            .unwrap();
        self.entered.notify_one();
        if self.wait_for_close.load(Ordering::SeqCst) {
            scope.closing().await;
            return Err(ExtensionError::RegistryClosed);
        }
        self.base.acquire_plan(scope.session().session_id()).await
    }
}
struct Harness {
    runtime: Orchestrator,
    memory: Arc<dyn Store>,
    provider: Arc<ScopedProvider>,
    gate: Arc<SettlementGate>,
    fake: FakeProvider,
    input: Request,
}
impl Harness {
    fn new() -> Self {
        let memory = Arc::new(MemoryStore::new());
        let gate = Arc::new(SettlementGate::default());
        gate.release.notify_one();
        let store = Arc::new(DelayedTerminalStore {
            inner: memory.clone(),
            gate: gate.clone(),
        });
        Self::on_store(store, memory, gate)
    }
    fn on_store(store: Arc<dyn Store>, memory: Arc<dyn Store>, gate: Arc<SettlementGate>) -> Self {
        Self::on_store_with_clock(store, memory, gate, None)
    }
    fn on_store_with_clock(
        store: Arc<dyn Store>,
        memory: Arc<dyn Store>,
        gate: Arc<SettlementGate>,
        clock: Option<Arc<dyn crabber_core::Clock>>,
    ) -> Self {
        let provider = ScopedProvider::new();
        let fake = FakeProvider::scripted(vec![text_script("done")]);
        let mut builder = Orchestrator::builder()
            .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
            .store(store)
            .resolver(Arc::new(fake.clone()))
            .plan_provider(provider.clone());
        if let Some(clock) = clock {
            builder = builder.clock(clock);
        }
        let runtime = builder.build().unwrap();
        let mut input = request();
        input.session_id = Some(SessionId::new());
        Self {
            runtime,
            memory,
            provider,
            gate,
            fake,
            input,
        }
    }
    async fn descriptor(&self) -> PreparationReconciliation {
        self.runtime
            .lookup_preparation_operation(
                self.input.session_id.as_ref().unwrap(),
                &PreparationKey::Admission(options().key),
            )
            .await
            .unwrap()
            .unwrap()
    }
    async fn child_state(&self, descriptor: &PreparationReconciliation) -> SessionOwnerState {
        let original = self
            .memory
            .lookup_session_owner(&descriptor.session_id, &descriptor.reserve_id)
            .await
            .unwrap()
            .unwrap();
        let link = PreparationResourceLink {
            session_id: descriptor.session_id.clone(),
            root: descriptor.owner.clone(),
            root_reserve: descriptor.reserve_id.clone(),
            root_begin: descriptor.begin_id.clone(),
            root_digest: original.semantic_digest,
        };
        self.memory
            .get_session_owner(&descriptor.session_id, &link.identities().unwrap().owner)
            .await
            .unwrap()
            .state
    }
    async fn finish_cleanup(&self, descriptor: &PreparationReconciliation) {
        self.finish_cleanup_state(descriptor, false).await;
    }
    async fn finish_cleanup_state(&self, descriptor: &PreparationReconciliation, uncertain: bool) {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.provider.cleanup_started.notified(),
        )
        .await
        .unwrap();
        assert_eq!(
            self.child_state(descriptor).await,
            SessionOwnerState::Active
        );
        let calls = self.provider.calls.load(Ordering::SeqCst);
        assert_eq!(
            self.provider.cleanup_completed.load(Ordering::SeqCst),
            calls - 1
        );
        self.provider.cleanup_release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.child_state(descriptor).await != SessionOwnerState::Joined {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            self.provider.cleanup_completed.load(Ordering::SeqCst),
            calls
        );
        if uncertain {
            let statuses = self
                .runtime
                .retained_preparation_status(&descriptor.session_id);
            assert!(
                statuses
                    .iter()
                    .any(|status| status.preparation == *descriptor
                        && status.status == crate::RetainedPreparationStatus::CleanupUncertain),
                "expected retained uncertainty: {statuses:?}"
            );
            return;
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while self
                .runtime
                .retained_preparation_status(&descriptor.session_id)
                .iter()
                .any(|status| status.preparation == *descriptor)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("owned-factory").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}

#[tokio::test]
async fn scoped_plan_lifetime_keeps_child_active_until_actual_cleanup_joins() {
    let h = Harness::new();
    let crate::Admission::Started { handle, receipt } = h
        .runtime
        .start_keyed(h.input.clone(), options())
        .await
        .unwrap()
    else {
        panic!("fresh admission")
    };
    let descriptor = h.descriptor().await;
    let binding = h
        .memory
        .execution_binding(&receipt.session_id, &receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        crabber_session::session_owner::preparation::resource_contract(
            &binding.capsule.runtime_semantics
        )
        .unwrap()
        .is_some()
    );
    handle.done().await.unwrap();
    h.finish_cleanup(&descriptor).await;
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.fake.requests().len(), 1);
    h.provider.policy.store(2, Ordering::SeqCst);
    assert!(
        h.runtime
            .start_keyed(h.input.clone(), options())
            .await
            .is_err()
    );
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unknown_native_commit_retry_observes_without_reminting_authority() {
    for point in [1, 2] {
        let h = Harness::new();
        h.gate.preparation_fault.store(point, Ordering::SeqCst);
        let error = h
            .runtime
            .start_keyed(h.input.clone(), options())
            .await
            .err()
            .unwrap();
        let descriptor = error.preparation_reconciliation().unwrap().clone();
        let status = h
            .runtime
            .retained_preparation_status(&descriptor.session_id);
        assert_eq!(status.len(), 1);
        assert_eq!(
            status[0].status,
            crate::RetainedPreparationStatus::CleanupUncertain
        );
        assert_eq!(h.runtime.retained_preparation_status(&SessionId::new()), []);
        assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Active);
        let calls = h.gate.preparation_calls.load(Ordering::SeqCst);
        assert!(
            h.runtime
                .retry_preparation_completion(&descriptor)
                .await
                .is_err()
        );
        assert_eq!(h.gate.preparation_calls.load(Ordering::SeqCst), calls);
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.fake.requests().len(), 0);
        h.finish_cleanup_state(&descriptor, true).await;
        if point == 2 {
            assert!(matches!(
                h.runtime
                    .start_keyed(h.input.clone(), options())
                    .await
                    .unwrap(),
                crate::Admission::Replayed(_)
            ));
        } else {
            assert!(
                h.memory
                    .lookup_admission(&descriptor.session_id, &options().key)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                h.runtime
                    .start_keyed(h.input.clone(), options())
                    .await
                    .is_err()
            );
        }
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.fake.requests().len(), 0);
    }
}
