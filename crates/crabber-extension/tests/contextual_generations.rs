mod contextual_support;
use async_trait::async_trait;
use contextual_support::*;
use crabber_extension::*;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct RecordingFactory {
    factory: Factory,
    generations: Arc<Mutex<Vec<ServiceGenerationDescriptor>>>,
    fail_once: AtomicBool,
}
#[async_trait]
impl ExtensionFactory for RecordingFactory {
    fn identity(&self) -> ComponentIdentity {
        self.factory.identity()
    }
    fn config_hash(&self) -> String {
        self.factory.config_hash()
    }
    fn scope(&self) -> FactoryScope {
        self.factory.scope()
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.generations
            .lock()
            .unwrap()
            .push(context.generation().clone());
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(ExtensionError::Plan("fixture creation failed".into()));
        }
        self.factory.create(context).await
    }
}

fn factory(
    scope: FactoryScope,
    generations: Arc<Mutex<Vec<ServiceGenerationDescriptor>>>,
    fail: bool,
) -> Arc<dyn ExtensionFactory> {
    Arc::new(RecordingFactory {
        factory: Factory {
            name: match scope {
                FactoryScope::Workspace => "workspace_counter",
                FactoryScope::Session => "session_counter",
            },
            scope,
            observed: Arc::new(Observed::default()),
        },
        generations,
        fail_once: AtomicBool::new(fail),
    })
}

#[tokio::test]
async fn generations_are_shared_by_real_owner_without_changing_plan_fingerprints() {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let provider = provider(vec![
        factory(FactoryScope::Workspace, recorded.clone(), false),
        factory(FactoryScope::Session, recorded.clone(), false),
    ]);
    assert_eq!(provider.service_generations(), Vec::new());
    let a = context("a", "workspace");
    let b = context("b", "workspace");
    let plan_a = provider.acquire_plan_in(&a).await.unwrap();
    let first = provider.service_generations();
    let plan_b = provider.acquire_plan_in(&b).await.unwrap();
    assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    let second = provider.service_generations();
    assert_eq!(second.len(), 4);
    for generation in &first {
        assert!(second.contains(generation));
    }
    provider.acquire_plan_in(&a).await.unwrap().release();
    assert_eq!(provider.service_generations(), second);
    let created = recorded.lock().unwrap().clone();
    assert_eq!(created.len(), 3);
    assert_eq!(
        created
            .iter()
            .filter(|g| matches!(g.scope(), ServiceGenerationScope::Workspace { .. }))
            .count(),
        1
    );
    assert!(created.iter().all(|g| {
        g.cleanup_policy() == ServiceGenerationCleanupPolicy::SealedFactoryLegacyMount
    }));
    plan_a.release();
    provider
        .close_session(&a, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(count(&plan_b, "workspace_counter").await, 1);
    plan_b.release();
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn retried_and_reconstructed_generations_get_new_ids_with_frozen_behavior() {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let provider = provider(vec![factory(FactoryScope::Session, recorded.clone(), true)]);
    let context = context("a", "workspace");
    assert!(provider.acquire_plan_in(&context).await.is_err());
    let plan = provider.acquire_plan_in(&context).await.unwrap();
    let generations = recorded.lock().unwrap().clone();
    assert_eq!(generations.len(), 2);
    assert_ne!(generations[0].id(), generations[1].id());
    assert_eq!(generations[0].scope(), generations[1].scope());
    assert_eq!(
        generations[0].registration_fingerprint(),
        generations[1].registration_fingerprint()
    );
    plan.release();
    provider.close_all(Duration::from_secs(1)).await.unwrap();
    let reconstructed = contextual_support::provider(vec![factory(
        FactoryScope::Session,
        recorded.clone(),
        false,
    )]);
    reconstructed
        .acquire_plan_in(&context)
        .await
        .unwrap()
        .release();
    let generations = recorded.lock().unwrap().clone();
    assert_ne!(generations[1].id(), generations[2].id());
    assert_eq!(
        generations[1].registration_fingerprint(),
        generations[2].registration_fingerprint()
    );
    reconstructed
        .close_all(Duration::from_secs(1))
        .await
        .unwrap();
}
