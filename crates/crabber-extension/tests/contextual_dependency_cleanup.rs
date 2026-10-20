mod contextual_support;
use async_trait::async_trait;
use contextual_support::*;
use crabber_extension::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

struct NestedFactory(Arc<Mutex<Option<ContextualPlanProvider>>>);
#[async_trait]
impl ExtensionFactory for NestedFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "nested".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "nested-v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, factory: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        let held = if factory.session_id().is_some_and(|id| id.0 == "a") {
            let provider = self.0.lock().unwrap().as_ref().unwrap().clone();
            Some(provider.acquire_plan_in(&context("b", "workspace")).await?)
        } else {
            None
        };
        Ok(Arc::new(ReleaseOnShutdown(Mutex::new(held))))
    }
}

struct ReleaseOnShutdown(Mutex<Option<RunPlan>>);
#[async_trait]
impl Extension for ReleaseOnShutdown {
    fn id(&self) -> &'static str {
        "nested"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "nested-v1".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        Ok(())
    }
    async fn shutdown(&self) {
        drop(self.0.lock().unwrap().take());
    }
}

#[tokio::test]
async fn cleanup_can_release_another_sessions_acquisition_while_dependencies_drain() {
    for whole_provider in [false, true] {
        let slot = Arc::new(Mutex::new(None));
        let provider = provider(vec![Arc::new(NestedFactory(slot.clone()))]);
        *slot.lock().unwrap() = Some(provider.clone());
        let a = context("a", "workspace");
        provider.acquire_plan_in(&a).await.unwrap().release();
        if whole_provider {
            provider.close_all(Duration::from_secs(1)).await.unwrap();
        } else {
            provider
                .close_workspace(&a, Duration::from_secs(1))
                .await
                .unwrap();
            provider.close_all(Duration::from_secs(1)).await.unwrap();
        }
        // Break the fixture's provider/factory reference cycle.
        slot.lock().unwrap().take();
    }
}
