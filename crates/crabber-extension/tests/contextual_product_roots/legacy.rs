use super::*;
use async_trait::async_trait;
use serde_json::Value;
struct Alive(Arc<AtomicUsize>);
impl Drop for Alive {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Service(Arc<Alive>);
#[async_trait]
impl Subprocess for Service {
    async fn run(&self, _: &str, _: &[String]) -> Result<Value, String> {
        let _held = &self.0;
        Ok(Value::Null)
    }
}
struct OldExtension(Arc<Alive>);
#[async_trait]
impl Extension for OldExtension {
    fn id(&self) -> &'static str {
        "legacy-root"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "legacy-root".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.host_services(HostServices {
            subprocess: Some(Arc::new(Service(self.0.clone()))),
            workspace_fs: None,
        });
        Ok(())
    }
}
struct OldFactory(Arc<AtomicUsize>);
#[async_trait]
impl ExtensionFactory for OldFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "legacy-root".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "legacy-root".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, _: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        Ok(Arc::new(OldExtension(Arc::new(Alive(self.0.clone())))))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn genuine_legacy_graph_join_does_not_dispose_an_escaped_original_object() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let host = Arc::new(Host::default());
    let registry = Registry::new();
    let provider = ContextualPlanProvider::new_with_generation_host(
        registry.clone(),
        vec![],
        vec![Arc::new(OldFactory(dropped.clone()))],
        Duration::from_secs(3),
        host.clone(),
    )
    .unwrap();
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let escaped = plan.host_services.subprocess.clone().unwrap();
    drop(plan);
    let graph = graph(&host);
    graph.request_close().unwrap();
    let joined = graph
        .registered_graph_join(Duration::from_secs(3))
        .await
        .unwrap();
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    let invocation = escaped.run("closed", &[]).await;
    let while_held = dropped.load(Ordering::SeqCst);
    drop(provider);
    drop(registry);
    drop(host);
    drop(graph);
    drop(joined);
    let after_library_release = dropped.load(Ordering::SeqCst);
    drop(escaped);
    assert!(invocation.is_err());
    assert_eq!(
        while_held, 0,
        "actual graph join cannot substitute for object disposal"
    );
    assert_eq!(
        after_library_release, 0,
        "escaped invocation facade still owns original object"
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
