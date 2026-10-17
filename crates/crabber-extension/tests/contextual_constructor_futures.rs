//! Literal factory future boundaries, distinct from opaque nested user code.
use crabber_core::{SessionContext, SessionId};
use crabber_extension::*;
use futures::future::BoxFuture;
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
const BOUND: Duration = Duration::from_secs(3);
#[derive(Default)]
struct Seen {
    constructed: AtomicUsize,
    polled: AtomicUsize,
    disposed: AtomicUsize,
}
#[derive(Clone, Copy)]
enum Mode {
    DoublePanic,
    SemanticError,
}
struct Literal<T> {
    seen: Arc<Seen>,
    mode: Mode,
    output: PhantomData<fn() -> T>,
}
impl<T> Future for Literal<T> {
    type Output = Result<T, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.seen.polled.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            Mode::DoublePanic => panic!("literal factory future poll panic"),
            Mode::SemanticError => {
                Poll::Ready(Err(ExtensionError::Plan("ordinary factory error".into())))
            }
        }
    }
}
impl<T> Drop for Literal<T> {
    fn drop(&mut self) {
        self.seen.disposed.fetch_add(1, Ordering::SeqCst);
        if matches!(self.mode, Mode::DoublePanic) {
            panic!("literal factory future Drop panic");
        }
    }
}
struct Factory {
    seen: Arc<Seen>,
    mode: Mode,
}
impl Factory {
    fn create<T: 'static>(&self) -> BoxFuture<'static, Result<T, ExtensionError>> {
        self.seen.constructed.fetch_add(1, Ordering::SeqCst);
        Box::pin(Literal {
            seen: self.seen.clone(),
            mode: self.mode,
            output: PhantomData,
        })
    }
}
macro_rules! factory {
    ($trait:ident, $method:ident, $product:ty) => {
        impl $trait for Factory {
            fn identity(&self) -> ComponentIdentity {
                ComponentIdentity {
                    id: "literal-factory".into(),
                    version: "1".into(),
                }
            }
            fn config_hash(&self) -> String {
                "{}".into()
            }
            fn scope(&self) -> FactoryScope {
                FactoryScope::Session
            }
            fn $method(
                &self,
                _: FactoryContext,
            ) -> BoxFuture<'_, Result<$product, ExtensionError>> {
                self.create()
            }
        }
    };
}
factory!(
    ContextualProductFactory,
    create_root,
    Box<dyn ContextualProductRoot>
);
factory!(
    ContextualToolProductFactory,
    create_tool,
    ContextualToolProduct
);
factory!(
    ContextualWorkspaceFsProductFactory,
    create_workspace_fs,
    ContextualWorkspaceFsProduct
);
factory!(
    ContextualSubprocessProductFactory,
    create_subprocess,
    ContextualSubprocessProduct
);
factory!(
    ContextualWorkspaceReaderResolverProductFactory,
    create_workspace_reader_resolver,
    ContextualWorkspaceReaderResolverProduct
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
fn provider(family: &str, factory: Arc<Factory>, host: Arc<Host>) -> ContextualPlanProvider {
    let registry = Registry::new();
    match family {
        "root" => {
            ContextualPlanProvider::new_with_product_roots(registry, vec![factory], BOUND, host)
        }
        "tool" => {
            ContextualPlanProvider::new_with_tool_products(registry, vec![factory], BOUND, host)
        }
        "fs" => ContextualPlanProvider::new_with_workspace_fs_products(
            registry,
            vec![factory],
            BOUND,
            host,
        ),
        "subprocess" => ContextualPlanProvider::new_with_subprocess_products(
            registry,
            vec![factory],
            BOUND,
            host,
        ),
        "reader" => ContextualPlanProvider::new_with_workspace_reader_resolver_products(
            registry,
            vec![factory],
            BOUND,
            host,
        ),
        _ => panic!("invalid fixture family"),
    }
    .unwrap()
}
async fn exercise(family: &str, mode: Mode) {
    let seen = Arc::new(Seen::default());
    let host = Arc::new(Host::default());
    let provider = provider(
        family,
        Arc::new(Factory {
            seen: seen.clone(),
            mode,
        }),
        host.clone(),
    );
    let context = SessionContext::new(
        SessionId("literal-factory".into()),
        "workspace",
        "/workspace",
    );
    let result = provider.acquire_plan_in(&context).await;
    let graph = host.0.lock().unwrap().last().unwrap().clone();
    graph.request_close().unwrap();
    let joined = graph.registered_graph_join(BOUND).await;
    let closed = provider.close_all(BOUND).await;
    assert!(result.is_err());
    assert_eq!(seen.constructed.load(Ordering::SeqCst), 1);
    assert_eq!(seen.polled.load(Ordering::SeqCst), 1);
    assert_eq!(seen.disposed.load(Ordering::SeqCst), 1);
    match mode {
        Mode::DoublePanic => assert!(joined.is_err() && closed.is_err()),
        Mode::SemanticError => {
            assert!(
                matches!(result, Err(ExtensionError::Plan(ref message)) if message == "ordinary factory error")
            );
            assert_eq!(
                joined.unwrap().dispositions(),
                &[ContextualGenerationMemberDisposition::CleanupJoined]
            );
            closed.unwrap();
        }
    }
}
fn child(family: &str) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "constructor_child", "--nocapture"])
        .env("CRABBER_LITERAL_FACTORY_CHILD", family)
        .env("RUST_BACKTRACE", "0")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "family={family} status={}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}
macro_rules! regression {
    ($name:ident, $family:literal) => {
        #[test]
        fn $name() {
            child($family);
        }
    };
}
regression!(
    literal_root_factory_poll_and_drop_panics_fail_without_abort,
    "root"
);
regression!(
    literal_tool_factory_poll_and_drop_panics_fail_without_abort,
    "tool"
);
regression!(
    literal_fs_factory_poll_and_drop_panics_fail_without_abort,
    "fs"
);
regression!(
    literal_subprocess_factory_poll_and_drop_panics_fail_without_abort,
    "subprocess"
);
regression!(
    literal_reader_factory_poll_and_drop_panics_fail_without_abort,
    "reader"
);
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn constructor_child() {
    if let Ok(family) = std::env::var("CRABBER_LITERAL_FACTORY_CHILD") {
        exercise(&family, Mode::DoublePanic).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_factory_errors_keep_completed_original_cleanup_semantics() {
    for family in ["root", "tool", "fs", "subprocess", "reader"] {
        exercise(family, Mode::SemanticError).await;
    }
}
