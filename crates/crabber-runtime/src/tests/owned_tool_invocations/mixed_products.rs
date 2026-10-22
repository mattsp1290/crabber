//! Two-scope public composition through actual Runtime worker/callback owners.
use super::*;
use crabber_core::{InputFingerprint, SessionContext, ToolInfo, ToolOutput};
use crabber_extension::{
    ComponentIdentity, ContextualGenerationAdmission, ContextualGenerationHost,
    ContextualGenerationObservation, ContextualGenerationWork, ContextualOwnedProductFactory,
    ContextualPlanProvider, ContextualProviderBindingBody, ContextualProviderProduct,
    ContextualProviderProductFactory, ContextualStreamerProduct, ContextualToolGuardProduct,
    ContextualToolGuardProductFactory, ContextualToolProduct, ContextualToolProductFactory,
    ContextualWorkspaceFsProduct, ContextualWorkspaceFsProductFactory, FactoryContext,
    FactoryScope, GuardContext, GuardDecision, ToolContext, ToolExecutor, ToolGuard, WorkspaceFs,
};
use crabber_providers::{ProviderBindingError, ProviderInfo};
use futures::{Stream, future::BoxFuture};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
const BOUND: Duration = Duration::from_secs(5);
#[derive(Clone, Copy)]
enum Kind {
    Tool = 0,
    Provider = 1,
    Fs = 2,
    Guard = 3,
}
struct Gate {
    entered: Notify,
    open: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            open: Mutex::new(false),
            changed: Condvar::new(),
        })
    }
    fn block(&self) {
        // Capture before block_in_place: Tokio task context may be unavailable
        // inside the synchronous closure. Match this opaque ID to Store spans.
        let task = tokio::task::try_id();
        eprintln!("mixed products gate entering task={task:?}");
        tokio::task::block_in_place(|| {
            self.entered.notify_one();
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.changed.wait(open).unwrap();
            }
        });
        eprintln!("mixed products gate exited task={task:?}");
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.open.lock().unwrap() = true;
        self.0.changed.notify_all();
    }
}
struct Payload(Arc<Gate>);
impl Drop for Payload {
    fn drop(&mut self) {
        self.0.block();
    }
}
struct Seen {
    made: [AtomicUsize; 4],
    dropped: [AtomicUsize; 4],
    effects: AtomicUsize,
    reads: AtomicUsize,
    binds: AtomicUsize,
    streams: AtomicUsize,
    stream_drops: AtomicUsize,
    streamer_drops: AtomicUsize,
    turns: Mutex<BTreeMap<RunId, usize>>,
    calls: Mutex<BTreeMap<RunId, ToolCallId>>,
    observed: Mutex<Vec<(SessionId, RunId, ToolCallId)>>,
    mode: usize,
    target: Mutex<Option<SessionId>>,
    gate: Arc<Gate>,
}
impl Seen {
    fn new(mode: usize) -> Arc<Self> {
        Arc::new(Self {
            made: std::array::from_fn(|_| AtomicUsize::new(0)),
            dropped: std::array::from_fn(|_| AtomicUsize::new(0)),
            effects: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            binds: AtomicUsize::new(0),
            streams: AtomicUsize::new(0),
            stream_drops: AtomicUsize::new(0),
            streamer_drops: AtomicUsize::new(0),
            turns: Mutex::new(BTreeMap::new()),
            calls: Mutex::new(BTreeMap::new()),
            observed: Mutex::new(vec![]),
            mode,
            target: Mutex::new(None),
            gate: Gate::new(),
        })
    }
}
struct Body(Kind, Arc<Seen>);
impl Drop for Body {
    fn drop(&mut self) {
        self.1.dropped[self.0 as usize].fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl ToolExecutor for Body {
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        panic!("actual contextual override required")
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<ToolOutput, ExtensionError> {
        let bytes = context
            .host
            .workspace_fs
            .as_ref()
            .unwrap()
            .read(arguments["text"].as_str().unwrap())
            .await
            .map_err(ExtensionError::Tool)?;
        self.1.effects.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::from_json(&json!(String::from_utf8(bytes).unwrap())).unwrap())
    }
}
#[async_trait]
impl WorkspaceFs for Body {
    async fn read(&self, path: &str) -> Result<Vec<u8>, String> {
        assert_eq!(path, "mixed");
        self.1.reads.fetch_add(1, Ordering::SeqCst);
        Ok(path.as_bytes().to_vec())
    }
}
impl ToolGuard for Body {
    fn id(&self) -> &'static str {
        "mixed-guard"
    }
    fn check(&self, _: &str, _: &Value) -> GuardDecision {
        panic!("actual contextual override required")
    }
    fn check_with_context(&self, c: GuardContext<'_>) -> GuardDecision {
        assert_eq!(c.tool.name, "echo");
        assert_eq!(c.arguments, &json!({"text":"mixed"}));
        assert_eq!(self.1.calls.lock().unwrap().get(c.run_id), Some(c.call_id));
        self.1.observed.lock().unwrap().push((
            c.session_id.clone(),
            c.run_id.clone(),
            c.call_id.clone(),
        ));
        let first = {
            let mut target = self.1.target.lock().unwrap();
            target.get_or_insert_with(|| c.session_id.clone()) == c.session_id
        };
        if first {
            if self.1.mode < 2 {
                self.1.gate.entered.notify_one();
            }
            match self.1.mode {
                1 => return GuardDecision::Deny,
                2 => self.1.gate.block(),
                3 => std::panic::panic_any(Payload(self.1.gate.clone())),
                _ => {}
            }
        }
        GuardDecision::Allow
    }
}
fn descriptor() -> crabber_core::ProviderBindingDescriptor {
    Resolver::binding_descriptor(&FakeProvider::scripted(vec![]), &request().selection).unwrap()
}
impl ContextualProviderBindingBody for Body {
    fn bind_box(
        &self,
        selection: Selection,
    ) -> BoxFuture<'_, Result<ContextualStreamerProduct, ProviderBindingError>> {
        assert_eq!(selection, request().selection);
        self.1.binds.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(ContextualStreamerProduct::new(
                descriptor(),
                Box::new(Backend(self.1.clone())),
            ))
        })
    }
}
struct Backend(Arc<Seen>);
impl Drop for Backend {
    fn drop(&mut self) {
        self.0.streamer_drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl Streamer for Backend {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        self.0.streams.fetch_add(1, Ordering::SeqCst);
        let first = {
            let mut turns = self.0.turns.lock().unwrap();
            let count = turns.entry(request.identity.run_id.clone()).or_default();
            *count += 1;
            *count == 1
        };
        let script = if first {
            let call = ToolCallId::new();
            self.0
                .calls
                .lock()
                .unwrap()
                .insert(request.identity.run_id.clone(), call.clone());
            call_script(call, r#"{"text":"mixed"}"#)
        } else {
            text_script("mixed complete")
        };
        let stream = FakeProvider::scripted(vec![script]).stream(request).await?;
        Ok(Box::pin(OriginalStream {
            stream,
            seen: self.0.clone(),
        }))
    }
}
struct OriginalStream {
    stream: DeltaStream,
    seen: Arc<Seen>,
}
impl Stream for OriginalStream {
    type Item = StreamDelta;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}
impl Drop for OriginalStream {
    fn drop(&mut self) {
        self.seen.stream_drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Factory {
    kind: Kind,
    scope: FactoryScope,
    seen: Arc<Seen>,
}
impl Factory {
    fn body(&self) -> Body {
        self.seen.made[self.kind as usize].fetch_add(1, Ordering::SeqCst);
        Body(self.kind, self.seen.clone())
    }
}
macro_rules! factory {
    ($trait:ident,$method:ident,$result:ty,$make:expr) => {
        impl $trait for Factory {
            fn identity(&self) -> ComponentIdentity {
                ComponentIdentity {
                    id: format!("mixed-{}", self.kind as usize),
                    version: "1".into(),
                }
            }
            fn config_hash(&self) -> String {
                format!("mode={}", self.seen.mode)
            }
            fn scope(&self) -> FactoryScope {
                self.scope
            }
            fn $method(&self, _: FactoryContext) -> BoxFuture<'_, Result<$result, ExtensionError>> {
                Box::pin(async { Ok(($make)(self.body())) })
            }
        }
    };
}
factory!(
    ContextualToolProductFactory,
    create_tool,
    ContextualToolProduct,
    |body| ContextualToolProduct::new(
        ToolInfo {
            name: "echo".into(),
            description: String::new(),
            parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
            retry_safe: true,
            required_permissions: vec![]
        },
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
    ContextualToolGuardProductFactory,
    create_tool_guard,
    ContextualToolGuardProduct,
    |body| ContextualToolGuardProduct::new(Box::new(body))
);
factory!(
    ContextualProviderProductFactory,
    create_provider,
    ContextualProviderProduct,
    |body| ContextualProviderProduct::new(
        ProviderInfo {
            id: "fake".into(),
            name: "Fake".into()
        },
        vec![descriptor()],
        Box::new(body)
    )
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
fn build(
    store: Arc<dyn Store>,
    inverted: bool,
    seen: &Arc<Seen>,
) -> (Orchestrator, Arc<ContextualPlanProvider>, Arc<Host>) {
    let scope = |kind| {
        if matches!(kind, Kind::Tool | Kind::Guard) == inverted {
            FactoryScope::Workspace
        } else {
            FactoryScope::Session
        }
    };
    let factory = |kind| {
        Arc::new(Factory {
            kind,
            scope: scope(kind),
            seen: seen.clone(),
        })
    };
    let host = Arc::new(Host::default());
    let plans = Arc::new(
        ContextualPlanProvider::new_with_owned_products(
            Registry::new(),
            vec![
                ContextualOwnedProductFactory::Tool(factory(Kind::Tool)),
                ContextualOwnedProductFactory::Provider(factory(Kind::Provider)),
                ContextualOwnedProductFactory::WorkspaceFs(factory(Kind::Fs)),
                ContextualOwnedProductFactory::ToolGuard(factory(Kind::Guard)),
            ],
            BOUND,
            host.clone(),
        )
        .unwrap(),
    );
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .resolver(Arc::new(FakeProvider::scripted(vec![])))
        .plan_provider(plans.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    (runtime, plans, host)
}
#[path = "mixed_products/contracts.rs"]
mod contracts;
