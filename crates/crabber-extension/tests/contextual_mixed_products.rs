//! Public composition uses existing original nodes, never aggregate policy credit.
use async_trait::async_trait;
use crabber_core::{
    InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits, SessionContext, SessionId,
    ToolInfo, ToolOutput,
};
use crabber_extension::*;
use crabber_providers::{ProviderBindingError, ProviderInfo, Selection};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
const BOUND: Duration = Duration::from_secs(3);
#[derive(Clone, Copy, Debug)]
enum Family {
    Root,
    Tool,
    Provider,
    Fs,
    Process,
    Reader,
    Guard,
}
const FAMILIES: [Family; 7] = [
    Family::Root,
    Family::Tool,
    Family::Provider,
    Family::Fs,
    Family::Process,
    Family::Reader,
    Family::Guard,
];
#[derive(Default)]
struct Seen {
    metadata: AtomicUsize,
    made: AtomicUsize,
    drops: AtomicUsize,
    effects: AtomicUsize,
}
struct Factory {
    id: String,
    scope: FactoryScope,
    config: String,
    seen: Arc<Seen>,
    fail: bool,
}
impl Factory {
    fn new(id: &str, scope: FactoryScope) -> Arc<Self> {
        Arc::new(Self {
            id: id.into(),
            scope,
            config: "{}".into(),
            seen: Arc::new(Seen::default()),
            fail: false,
        })
    }
    fn body(&self) -> Result<Body, ExtensionError> {
        self.seen.made.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(ExtensionError::Tool("genuine constructor refusal".into()));
        }
        Ok(Body(self.id.clone(), self.seen.clone()))
    }
    fn entry(self: &Arc<Self>, family: Family) -> ContextualOwnedProductFactory {
        match family {
            Family::Root => ContextualOwnedProductFactory::Root(self.clone()),
            Family::Tool => ContextualOwnedProductFactory::Tool(self.clone()),
            Family::Provider => ContextualOwnedProductFactory::Provider(self.clone()),
            Family::Fs => ContextualOwnedProductFactory::WorkspaceFs(self.clone()),
            Family::Process => ContextualOwnedProductFactory::Subprocess(self.clone()),
            Family::Reader => ContextualOwnedProductFactory::WorkspaceReaderResolver(self.clone()),
            Family::Guard => ContextualOwnedProductFactory::ToolGuard(self.clone()),
        }
    }
}
struct Body(String, Arc<Seen>);
impl Drop for Body {
    fn drop(&mut self) {
        self.1.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl ToolExecutor for Body {
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        self.1.effects.fetch_add(1, Ordering::SeqCst);
        ToolOutput::from_json(&json!(self.0)).map_err(|e| ExtensionError::Tool(e.to_string()))
    }
}
#[async_trait]
impl WorkspaceFs for Body {
    async fn read(&self, path: &str) -> Result<Vec<u8>, String> {
        self.1.effects.fetch_add(1, Ordering::SeqCst);
        Ok(path.as_bytes().to_vec())
    }
}
#[async_trait]
impl Subprocess for Body {
    async fn run(&self, program: &str, arguments: &[String]) -> Result<Value, String> {
        Ok(json!([program, arguments]))
    }
}
#[async_trait]
impl ContextualWorkspaceReaderResolverBody for Body {
    async fn resolve_box(
        &self,
        _: &WorkspaceContext,
    ) -> Result<ContextualWorkspaceReaderProduct, WorkspaceReadError> {
        Ok(ContextualWorkspaceReaderProduct::new(Box::new(Body(
            self.0.clone(),
            self.1.clone(),
        ))))
    }
}
#[async_trait]
impl WorkspaceReader for Body {
    async fn read_limited(
        &self,
        path: &str,
        maximum: usize,
    ) -> Result<Vec<u8>, WorkspaceReadError> {
        Ok(path.as_bytes().iter().take(maximum).copied().collect())
    }
}
impl ToolGuard for Body {
    fn id(&self) -> &str {
        &self.0
    }
    fn check(&self, _: &str, _: &Value) -> GuardDecision {
        panic!("exact contextual override required")
    }
    fn check_with_context(&self, context: GuardContext<'_>) -> GuardDecision {
        assert_eq!(context.tool.name, "tool");
        self.1.effects.fetch_add(1, Ordering::SeqCst);
        GuardDecision::Allow
    }
}
impl ContextualProviderBindingBody for Body {
    fn bind_box(
        &self,
        _: Selection,
    ) -> BoxFuture<'_, Result<ContextualStreamerProduct, ProviderBindingError>> {
        panic!("metadata-only provider fixture; Runtime exercises actual binding")
    }
}
fn descriptor() -> ProviderBindingDescriptor {
    ProviderBindingDescriptor::new(
        "mixed",
        "model",
        "mixed",
        "1",
        InputFingerprint::new("a".repeat(64)).unwrap(),
        ProviderBindingLimits::default(),
    )
    .unwrap()
}
macro_rules! factory {
    ($trait:ident, $method:ident, $result:ty, $make:expr) => {
        impl $trait for Factory {
            fn identity(&self) -> ComponentIdentity {
                self.seen.metadata.fetch_add(1, Ordering::SeqCst);
                ComponentIdentity {
                    id: self.id.clone(),
                    version: "1".into(),
                }
            }
            fn config_hash(&self) -> String {
                self.seen.metadata.fetch_add(1, Ordering::SeqCst);
                self.config.clone()
            }
            fn scope(&self) -> FactoryScope {
                self.seen.metadata.fetch_add(1, Ordering::SeqCst);
                self.scope
            }
            fn $method(&self, _: FactoryContext) -> BoxFuture<'_, Result<$result, ExtensionError>> {
                Box::pin(async move { self.body().map($make) })
            }
        }
    };
}
factory!(
    ContextualProductFactory,
    create_root,
    Box<dyn ContextualProductRoot>,
    |body| Box::new(body) as Box<dyn ContextualProductRoot>
);
factory!(
    ContextualToolProductFactory,
    create_tool,
    ContextualToolProduct,
    |body: Body| ContextualToolProduct::new(
        ToolInfo {
            name: body.0.clone(),
            description: String::new(),
            parameters: Value::Null,
            retry_safe: false,
            required_permissions: vec![]
        },
        Box::new(body)
    )
);
factory!(
    ContextualProviderProductFactory,
    create_provider,
    ContextualProviderProduct,
    |body| ContextualProviderProduct::new(
        ProviderInfo {
            id: "mixed".into(),
            name: "Mixed".into()
        },
        vec![descriptor()],
        Box::new(body)
    )
);
factory!(
    ContextualWorkspaceFsProductFactory,
    create_workspace_fs,
    ContextualWorkspaceFsProduct,
    |body| ContextualWorkspaceFsProduct::new(Box::new(body))
);
factory!(
    ContextualSubprocessProductFactory,
    create_subprocess,
    ContextualSubprocessProduct,
    |body| ContextualSubprocessProduct::new(Box::new(body))
);
factory!(
    ContextualWorkspaceReaderResolverProductFactory,
    create_workspace_reader_resolver,
    ContextualWorkspaceReaderResolverProduct,
    |body| ContextualWorkspaceReaderResolverProduct::new(Box::new(body))
);
factory!(
    ContextualToolGuardProductFactory,
    create_tool_guard,
    ContextualToolGuardProduct,
    |body| ContextualToolGuardProduct::new(Box::new(body))
);
#[derive(Default)]
struct Host(Mutex<Vec<ContextualGenerationObservation>>);
impl ContextualGenerationHost for Host {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission> {
        self.0.lock().unwrap().push(work.observation().unwrap());
        Box::pin(async { ContextualGenerationAdmission::Admitted(work) })
    }
}
fn context() -> SessionContext {
    SessionContext::new(SessionId::new(), "workspace", "/workspace")
}
fn old(factory: Arc<Factory>, family: Family, host: Arc<Host>) -> ContextualPlanProvider {
    let registry = Registry::new();
    match family {
        Family::Root => {
            ContextualPlanProvider::new_with_product_roots(registry, vec![factory], BOUND, host)
        }
        Family::Tool => {
            ContextualPlanProvider::new_with_tool_products(registry, vec![factory], BOUND, host)
        }
        Family::Provider => {
            ContextualPlanProvider::new_with_provider_products(registry, vec![factory], BOUND, host)
        }
        Family::Fs => ContextualPlanProvider::new_with_workspace_fs_products(
            registry,
            vec![factory],
            BOUND,
            host,
        ),
        Family::Process => ContextualPlanProvider::new_with_subprocess_products(
            registry,
            vec![factory],
            BOUND,
            host,
        ),
        Family::Reader => ContextualPlanProvider::new_with_workspace_reader_resolver_products(
            registry,
            vec![factory],
            BOUND,
            host,
        ),
        Family::Guard => ContextualPlanProvider::new_with_tool_guard_products(
            registry,
            vec![factory],
            BOUND,
            host,
        ),
    }
    .unwrap()
}
async fn disposed(graph: &ContextualGenerationObservation, family: Family, member: usize) {
    match family {
        Family::Root => {
            graph
                .product_root(member)
                .unwrap()
                .disposed(BOUND)
                .await
                .unwrap();
        }
        Family::Tool => {
            graph
                .tool_product(member)
                .unwrap()
                .disposed(BOUND)
                .await
                .unwrap();
        }
        Family::Provider => {
            graph.registered_graph_join(BOUND).await.unwrap();
        }
        Family::Fs => {
            graph
                .workspace_fs_product(member)
                .unwrap()
                .disposed(BOUND)
                .await
                .unwrap();
        }
        Family::Process => {
            graph
                .subprocess_product(member)
                .unwrap()
                .disposed(BOUND)
                .await
                .unwrap();
        }
        Family::Reader => {
            graph
                .workspace_reader_resolver_product(member)
                .unwrap()
                .disposed(BOUND)
                .await
                .unwrap();
        }
        Family::Guard => {
            graph
                .tool_guard_product(member)
                .unwrap()
                .disposed(BOUND)
                .await
                .unwrap();
        }
    }
}

#[path = "contextual_mixed_products/capacity.rs"]
mod capacity;
#[path = "contextual_mixed_products/compatibility.rs"]
mod compatibility;
#[path = "contextual_mixed_products/lifetime.rs"]
mod lifetime;
