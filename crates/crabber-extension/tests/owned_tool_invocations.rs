use async_trait::async_trait;
use crabber_core::{SessionId, ToolInfo, ToolOutput};
use crabber_extension::{
    Extension, ExtensionError, Registrar, Registry, Scope, ToolDefinition, ToolExecutor,
};
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

struct Counter(Arc<AtomicUsize>);
#[async_trait]
impl ToolExecutor for Counter {
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::from_json(&Value::Null).unwrap())
    }
}
struct Tools(Arc<dyn ToolExecutor>);
#[async_trait]
impl Extension for Tools {
    fn id(&self) -> &'static str {
        "owned-tools"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "count".into(),
                description: String::new(),
                parameters: Value::Null,
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: self.0.clone(),
        }));
        Ok(())
    }
}

#[tokio::test]
async fn extracted_executor_refuses_after_actual_owned_mount_close() {
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Tools(Arc::new(Counter(calls.clone())))),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let executor = plan.tools[0].executor.clone();
    drop(plan);
    mount.close(Duration::from_secs(1)).await.unwrap();
    assert!(executor.execute(Value::Null).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[path = "owned_tool_invocations/context.rs"]
mod context_tests;
#[path = "owned_tool_invocations/ingress.rs"]
mod ingress;
#[path = "owned_tool_invocations/resumable.rs"]
mod resumable;
#[path = "owned_tool_invocations/retention.rs"]
mod retention;

fn context() -> crabber_extension::ToolContext {
    crabber_extension::ToolContext::new(
        SessionId::new(),
        crabber_core::RunId::new(),
        crabber_core::ToolCallId::new(),
        tokio_util::sync::CancellationToken::new(),
        crabber_extension::HostServices::default(),
        crabber_extension::WorkspaceContext::from_persisted("w", "."),
        Arc::new(|_| {}),
    )
}

#[tokio::test]
async fn owned_sequential_invocations_reap_actual_joins_and_legacy_stays_callable() {
    for owned in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = Registry::new();
        let extension = Arc::new(Tools(Arc::new(Counter(calls.clone()))));
        let close = if owned {
            Some(
                registry
                    .mount_owned(extension, Scope::Global)
                    .await
                    .unwrap(),
            )
        } else {
            registry.mount(extension, Scope::Global).await.unwrap();
            None
        };
        let plan = registry.acquire(&SessionId::new());
        let executor = plan.tools[0].executor.clone();
        for _ in 0..130 {
            executor
                .execute_with_context(context(), Value::Null)
                .await
                .unwrap();
        }
        drop(plan);
        registry.close_all().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 130);
        assert_eq!(
            executor
                .execute_with_context(context(), Value::Null)
                .await
                .is_err(),
            owned
        );
        if let Some(close) = close {
            close.close(Duration::from_secs(1)).await.unwrap();
        }
    }
}
