//! Actual initial provider acquisition stays in native retained preparation.
use super::*;
use crabber_providers::{
    BoundProvider, ProviderBindingError, ProviderError, ProviderResourceContext,
    ProviderResourceContract, Resolver, Selection, Streamer,
};
struct OwnedResolver {
    fake: FakeProvider,
    calls: AtomicUsize,
    started: Arc<Notify>,
    release: Arc<Notify>,
    finished: Arc<AtomicUsize>,
}
#[async_trait]
impl Resolver for OwnedResolver {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber_core::ProviderBindingDescriptor, ProviderBindingError> {
        Resolver::binding_descriptor(&self.fake, selection)
    }
    fn binding_resource_contract(
        &self,
        _: &Selection,
    ) -> Result<Option<ProviderResourceContract>, ProviderBindingError> {
        Ok(Some(ProviderResourceContract::new(
            "owned-resolver-test".into(),
            "1".into(),
            InputFingerprint::new("d".repeat(64)).unwrap(),
        )?))
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        panic!("owned provider must never use unscoped fallback")
    }
    async fn bind_owned(
        &self,
        selection: &Selection,
        context: &ProviderResourceContext,
    ) -> Result<BoundProvider, ProviderBindingError> {
        let started = self.started.clone();
        let release = self.release.clone();
        let finished = self.finished.clone();
        context.defer(move || async move {
            started.notify_one();
            release.notified().await;
            finished.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.fake.bind(selection).await
    }
}
fn resolver(h: &mut Harness) -> Arc<OwnedResolver> {
    let resolver = Arc::new(OwnedResolver {
        fake: h.fake.clone(),
        calls: AtomicUsize::new(0),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        finished: Arc::new(AtomicUsize::new(0)),
    });
    h.runtime = Orchestrator::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(Arc::new(DelayedTerminalStore {
            inner: h.memory.clone(),
            gate: h.gate.clone(),
        }))
        .resolver(resolver.clone())
        .plan_provider(h.provider.clone())
        .build()
        .unwrap();
    resolver
}
#[tokio::test]
async fn ordinary_and_keyed_initial_workers_use_captured_owned_provider_once() {
    for keyed in [false, true] {
        let mut h = Harness::new();
        let resolver = resolver(&mut h);
        let handle = if keyed {
            let crate::Admission::Started { handle, .. } = h
                .runtime
                .start_keyed(h.input.clone(), options())
                .await
                .unwrap()
            else {
                panic!("fresh admission")
            };
            handle
        } else {
            h.input.session_id = None;
            h.runtime.start(h.input.clone()).await.unwrap()
        };
        let descriptor = if keyed {
            h.descriptor().await
        } else {
            h.runtime.retained_preparation_status(handle.session_id())[0]
                .preparation
                .clone()
        };
        handle.done().await.unwrap();
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.fake.requests().len(), 1);
        tokio::time::timeout(Duration::from_secs(2), resolver.started.notified())
            .await
            .unwrap();
        assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Active);
        h.provider.cleanup_release.notify_one();
        assert_eq!(resolver.finished.load(Ordering::SeqCst), 0);
        resolver.release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while h.child_state(&descriptor).await != SessionOwnerState::Joined {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(resolver.finished.load(Ordering::SeqCst), 1);
        assert_eq!(h.provider.cleanup_completed.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn owned_provider_refuses_unscoped_preparation_before_provider_factory() {
    let mut h = Harness::new();
    let resolver = resolver(&mut h);
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(h.memory.clone())
        .resolver(resolver.clone())
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    assert!(runtime.start(h.input.clone()).await.is_err());
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    assert_eq!(h.fake.requests().len(), 0);
}

#[tokio::test]
async fn lost_native_commit_ack_retains_captured_provider_and_never_reacquires_on_retry() {
    for point in [1, 2] {
        let mut h = Harness::new();
        let resolver = resolver(&mut h);
        h.gate.preparation_fault.store(point, Ordering::SeqCst);
        let error = h
            .runtime
            .start_keyed(h.input.clone(), options())
            .await
            .err()
            .unwrap();
        let descriptor = error.preparation_reconciliation().unwrap().clone();
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.fake.requests().len(), 0);
        assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Active);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), resolver.started.notified())
                .await
                .is_err()
        );
        let progress = h
            .runtime
            .retry_preparation_completion(&descriptor)
            .await
            .unwrap();
        assert_eq!(progress.replayed, point == 2);
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.fake.requests().len(), 0);
        tokio::time::timeout(Duration::from_secs(2), resolver.started.notified())
            .await
            .unwrap();
        h.provider.cleanup_release.notify_one();
        assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Active);
        resolver.release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while h.child_state(&descriptor).await != SessionOwnerState::Joined {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(resolver.finished.load(Ordering::SeqCst), 1);
    }
}
