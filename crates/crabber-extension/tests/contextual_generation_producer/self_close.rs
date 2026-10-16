//! New retained tasks must preserve the original initializer's self-close guard.
use super::*;

struct Probe {
    provider: Arc<Mutex<Option<ContextualPlanProvider>>>,
    scope: FactoryScope,
    all: bool,
    result: Mutex<Option<Result<(), ExtensionError>>>,
}
impl Probe {
    async fn close(&self) {
        let provider = self.provider.lock().unwrap().as_ref().unwrap().clone();
        let bound = Duration::from_millis(20);
        let result = if self.all {
            provider.close_all(bound).await
        } else {
            match self.scope {
                FactoryScope::Workspace => provider.close_workspace(&context("a"), bound).await,
                FactoryScope::Session => provider.close_session(&context("a"), bound).await,
            }
        };
        *self.result.lock().unwrap() = Some(result);
    }
}
struct ClosingFactory(Arc<Probe>, Arc<Seen>);
#[async_trait::async_trait]
impl ExtensionFactory for ClosingFactory {
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
        self.0.scope
    }
    async fn create(&self, _: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.0.close().await;
        Ok(Arc::new(Instance(self.1.clone())))
    }
}
struct ClosingHost(Arc<Probe>);
impl ContextualGenerationHost for ClosingHost {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission> {
        let probe = self.0.clone();
        Box::pin(async move {
            probe.close().await;
            ContextualGenerationAdmission::Admitted(work)
        })
    }
}
async fn probe(at_host: bool) {
    for scope in [FactoryScope::Session, FactoryScope::Workspace] {
        for all in [false, true] {
            let holder = Arc::new(Mutex::new(None));
            let probe = Arc::new(Probe {
                provider: holder.clone(),
                scope,
                all,
                result: Mutex::new(None),
            });
            let seen = Seen::new(false, false, false);
            let host: Arc<dyn ContextualGenerationHost> = if at_host {
                Arc::new(ClosingHost(probe.clone()))
            } else {
                Host::new(Mode::Admit, false)
            };
            let factory: Arc<dyn ExtensionFactory> = if at_host {
                Arc::new(Factory { seen, scope })
            } else {
                Arc::new(ClosingFactory(probe.clone(), seen))
            };
            let provider = ContextualPlanProvider::new_with_generation_host(
                Registry::new(),
                vec![],
                vec![factory],
                Duration::from_secs(3),
                host,
            )
            .unwrap();
            *holder.lock().unwrap() = Some(provider.clone());
            let acquired = provider.acquire_plan_in(&context("a")).await;
            holder.lock().unwrap().take();
            let succeeded = acquired.is_ok();
            if let Ok(plan) = acquired {
                plan.release();
            }
            provider.close_all(Duration::from_secs(3)).await.unwrap();
            assert_eq!(
                probe.result.lock().unwrap().take().unwrap(),
                Err(ExtensionError::SelfClose)
            );
            assert!(
                succeeded,
                "self-close must not mutate the initialization owner"
            );
        }
    }
}
#[tokio::test]
async fn factory_self_close_is_rejected_before_mutation() {
    probe(false).await;
}
#[tokio::test]
async fn host_self_close_is_rejected_before_mutation() {
    probe(true).await;
}
