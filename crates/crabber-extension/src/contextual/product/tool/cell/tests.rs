//! Actual captured product bind refusal must retain the original rollback owner.
use super::Node;
use crate::{
    Extension, ExtensionError, JsonToolExecutor, Registrar, Registry, Scope,
    contextual::{Owner, product::tool::ContextualToolProduct},
};
use async_trait::async_trait;
use crabber_core::{SessionId, ToolInfo};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::Duration,
};

struct ValueTool;
#[async_trait]
impl JsonToolExecutor for ValueTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("a refused mount never dispatches its actual tool")
    }
}
#[derive(Default)]
struct Rollback {
    calls: AtomicUsize,
    reentered_during_cleanup: AtomicBool,
    workers: Mutex<Vec<JoinHandle<()>>>,
}
struct ClosedProduct {
    node: Arc<Node>,
    registry: Registry,
    rollback: Arc<Rollback>,
    panic_cleanup: bool,
}
#[async_trait]
impl Extension for ClosedProduct {
    fn id(&self) -> &'static str {
        "test/closed-product-bind"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "closed-original-product".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        // Register the real successfully frozen node before closing that SAME
        // original. This reaches bind's actual closed-state refusal after capture.
        registrar.product_tool(self.node.clone())?;
        let registry = self.registry.clone();
        let observed = self.rollback.clone();
        let panic_cleanup = self.panic_cleanup;
        registrar.defer(move || {
            observed.calls.fetch_add(1, Ordering::SeqCst);
            let (returned, receipt) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                let plan = registry.acquire(&SessionId::new());
                drop(plan);
                let _ = returned.send(());
            });
            observed.workers.lock().unwrap().push(worker);
            // On the old path acquire waits for the registry lock. Returning
            // finitely here lets mount return and the test join the SAME thread,
            // exposing the bad lock boundary without hanging the test process.
            observed.reentered_during_cleanup.store(
                receipt.recv_timeout(Duration::from_secs(2)).is_ok(),
                Ordering::SeqCst,
            );
            assert!(!panic_cleanup, "actual original cleanup panic");
        });
        self.node.close();
        Ok(())
    }
}
async fn refused_product(panic_cleanup: bool) {
    let registry = Registry::new();
    let node = Node::new(Owner::Shared);
    node.install(ContextualToolProduct::new(
        ToolInfo {
            name: "refused-original-tool".into(),
            description: "value-only original product".into(),
            parameters: json!({"type":"object"}),
            retry_safe: false,
            required_permissions: Vec::new(),
        },
        Box::new(ValueTool),
    ));
    node.done(false);
    node.freeze().unwrap();
    let rollback = Arc::new(Rollback::default());
    let result = registry
        .mount_owned(
            Arc::new(ClosedProduct {
                node: node.clone(),
                registry: registry.clone(),
                rollback: rollback.clone(),
                panic_cleanup,
            }),
            Scope::Global,
        )
        .await;
    // Always observe every actual helper thread before any assertion can panic.
    let workers = std::mem::take(&mut *rollback.workers.lock().unwrap());
    for worker in workers {
        worker.join().unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), node.disposed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rollback.calls.load(Ordering::SeqCst), 1);
    assert!(registry.acquire(&SessionId::new()).tools.is_empty());
    if panic_cleanup {
        assert!(matches!(
            result,
            Err(ExtensionError::MountCleanupIncomplete { .. })
        ));
    } else {
        assert!(matches!(result, Err(ExtensionError::ContextOwnerClosed)));
    }
    assert!(rollback.reentered_during_cleanup.load(Ordering::SeqCst));
    registry.close_all().await.unwrap();
}
#[tokio::test]
async fn closed_product_bind_refusal_rolls_back_once_outside_registry_lock() {
    refused_product(false).await;
}
#[tokio::test]
async fn closed_product_bind_refusal_reports_original_cleanup_panic() {
    refused_product(true).await;
}
