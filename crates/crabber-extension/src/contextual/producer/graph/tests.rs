use super::*;
use crate::RunPlanProvider;
use crate::{
    ComponentIdentity, ContextualGenerationAdmission, ContextualGenerationHost,
    ContextualGenerationWork, ContextualPlanProvider, Extension, ExtensionFactory, FactoryContext,
    FactoryScope, Registrar, Registry,
};
use crabber_core::{SessionContext, SessionId};
use futures::future::BoxFuture;
use tokio::sync::{Notify, oneshot};

struct Host {
    observation: Mutex<Option<ContextualGenerationObservation>>,
    tail: Mutex<Option<oneshot::Receiver<()>>>,
    entered: Notify,
}
impl ContextualGenerationHost for Host {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission> {
        let observation = work.observation().unwrap();
        *observation
            .registration
            .graph
            .initializer
            .tail
            .lock()
            .unwrap() = self.tail.lock().unwrap().take();
        *self.observation.lock().unwrap() = Some(observation);
        self.entered.notify_one();
        Box::pin(async { ContextualGenerationAdmission::Admitted(work) })
    }
}
struct Factory;
#[async_trait::async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "tail".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "tail".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, _: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        Ok(Arc::new(Instance))
    }
}
struct Instance;
#[async_trait::async_trait]
impl Extension for Instance {
    fn id(&self) -> &'static str {
        "tail"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "tail".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        Ok(())
    }
}
#[tokio::test]
async fn published_initializer_result_does_not_replace_original_task_join() {
    let (release, tail) = oneshot::channel();
    let host = Arc::new(Host {
        observation: Mutex::new(None),
        tail: Mutex::new(Some(tail)),
        entered: Notify::new(),
    });
    let provider = ContextualPlanProvider::new_with_generation_host(
        Registry::new(),
        vec![],
        vec![Arc::new(Factory)],
        Duration::from_secs(1),
        host.clone(),
    )
    .unwrap();
    let task = tokio::spawn({
        let provider = provider.clone();
        async move {
            provider
                .acquire_plan_in(&SessionContext::new(
                    SessionId("tail".into()),
                    "workspace",
                    "/workspace",
                ))
                .await
        }
    });
    host.entered.notified().await;
    let observation = host.observation.lock().unwrap().clone().unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if observation
                .registration
                .graph
                .initializer
                .result
                .lock()
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    observation.request_close().unwrap();
    let pending = observation
        .registered_graph_join(Duration::from_millis(20))
        .await;
    release.send(()).unwrap();
    let acquired = task.await.unwrap();
    drop(acquired);
    let joined = observation
        .registered_graph_join(Duration::from_secs(1))
        .await
        .unwrap();
    provider.close_all(Duration::from_secs(1)).await.unwrap();
    assert!(
        pending.is_err(),
        "logical result must not substitute for initializer join"
    );
    assert_eq!(
        joined.dispositions(),
        &[ContextualGenerationMemberDisposition::MountedJoined]
    );
}
