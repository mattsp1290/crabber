//! Known constructor failure can retry only after real original cleanup joins.
use super::*;
struct FailOnce(Arc<Seen>, AtomicUsize);
#[async_trait::async_trait]
impl ExtensionFactory for FailOnce {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "local-producer-fixture".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "fixture.v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        let closing = context.cleanup().closing();
        let seen = self.0.clone();
        context
            .cleanup()
            .spawn_checked(async move {
                closing.await;
                seen.stopped.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        self.0.created.fetch_add(1, Ordering::SeqCst);
        if self.1.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ExtensionError::ContextInitializationIncomplete);
        }
        Ok(Arc::new(Instance(self.0.clone())))
    }
}
#[tokio::test]
async fn known_failed_constructor_retries_only_after_actual_cleanup() {
    let host = Host::new(Mode::Admit, false);
    let seen = Seen::new(false, false, false);
    let provider = ContextualPlanProvider::new_with_generation_host(
        Registry::new(),
        vec![],
        vec![Arc::new(FailOnce(seen.clone(), AtomicUsize::new(0)))],
        Duration::from_secs(3),
        host.clone(),
    )
    .unwrap();
    assert!(provider.acquire_plan_in(&context("a")).await.is_err());
    assert_eq!(seen.stopped.load(Ordering::SeqCst), 1);
    host.release.add_permits(1);
    provider
        .acquire_plan_in(&context("a"))
        .await
        .unwrap()
        .release();
    assert_eq!(seen.created.load(Ordering::SeqCst), 2);
    let generations = host.generations.lock().unwrap().clone();
    assert_eq!(generations.len(), 2);
    assert_ne!(generations[0].id(), generations[1].id());
    assert_eq!(
        generations[0].registration_fingerprint(),
        generations[1].registration_fingerprint()
    );
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert_eq!(seen.stopped.load(Ordering::SeqCst), 2);
}
