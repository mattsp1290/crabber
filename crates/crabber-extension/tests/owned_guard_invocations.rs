use async_trait::async_trait;
use crabber_core::{RunId, SessionId, ToolCallId, ToolInfo};
use crabber_extension::{
    Extension, ExtensionError, GuardContext, GuardDecision, Registrar, Registry, Scope, ToolGuard,
};
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

struct Counter(Arc<AtomicUsize>);
impl ToolGuard for Counter {
    fn id(&self) -> &'static str {
        "count"
    }
    fn check(&self, _: &str, _: &Value) -> GuardDecision {
        self.0.fetch_add(1, Ordering::SeqCst);
        GuardDecision::Allow
    }
}
struct Guards(Arc<dyn ToolGuard>);
#[async_trait]
impl Extension for Guards {
    fn id(&self) -> &'static str {
        "owned-guards"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.guard(self.0.clone());
        Ok(())
    }
}

fn check_context(guard: &dyn ToolGuard) -> GuardDecision {
    guard.check_with_context(GuardContext {
        tool: &ToolInfo {
            name: "tool".into(),
            description: String::new(),
            parameters: Value::Null,
            retry_safe: false,
            required_permissions: vec![],
        },
        arguments: &Value::Null,
        call_id: &ToolCallId::new(),
        session_id: &SessionId::new(),
        run_id: &RunId::new(),
    })
}

#[tokio::test]
async fn extracted_guard_refuses_after_owned_mount_close() {
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Guards(Arc::new(Counter(calls.clone())))),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let guard = plan.guards[0].clone();
    drop(plan);
    mount.close(Duration::from_secs(1)).await.unwrap();
    let ordinary = guard.check("tool", &Value::Null);
    let contextual = check_context(guard.as_ref());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(ordinary, GuardDecision::Deny);
    assert_eq!(contextual, GuardDecision::Deny);
}

#[path = "owned_guard_invocations/retention.rs"]
mod retention;

#[tokio::test]
async fn sequential_guards_preserve_context_and_legacy_behavior() {
    for owned in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = Registry::new();
        let extension = Arc::new(Guards(Arc::new(Counter(calls.clone()))));
        if owned {
            registry
                .mount_owned(extension, Scope::Global)
                .await
                .unwrap();
        } else {
            registry.mount(extension, Scope::Global).await.unwrap();
        }
        let plan = registry.acquire(&SessionId::new());
        let guard = plan.guards[0].clone();
        for _ in 0..130 {
            assert_eq!(guard.id(), "count");
            assert_eq!(guard.check("tool", &Value::Null), GuardDecision::Allow);
            assert_eq!(check_context(guard.as_ref()), GuardDecision::Allow);
        }
        drop(plan);
        registry.close_all().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 260);
        assert_eq!(
            guard.check("tool", &Value::Null) == GuardDecision::Deny,
            owned
        );
    }
}
