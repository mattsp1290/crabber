use async_trait::async_trait;
use crabber_core::SessionId;
use crabber_extension::{
    Extension, ExtensionError, Point, Registrar, Registry, Scope, TurnPrepare,
};
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[path = "owned_dispatch_invocations/legacy.rs"]
mod legacy;
#[path = "owned_dispatch_invocations/modes.rs"]
mod modes;
#[path = "owned_dispatch_invocations/retention.rs"]
mod retention;
#[path = "owned_dispatch_invocations/task_context.rs"]
mod task_context;

struct Counter(Arc<AtomicUsize>);
#[async_trait]
impl Extension for Counter {
    fn id(&self) -> &'static str {
        "owned-dispatch"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let calls = self.0.clone();
        r.on_hook(
            TurnPrepare::ID,
            0,
            "counter",
            Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(Value::Null) })
            }),
        );
        Ok(())
    }
}

#[tokio::test]
async fn extracted_owned_dispatcher_cannot_construct_after_actual_mount_close() {
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = Registry::new();
    let handle = registry
        .mount_owned(Arc::new(Counter(calls.clone())), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let dispatcher = plan.dispatcher.clone();
    drop(plan);
    handle.close(Duration::from_secs(1)).await.unwrap();
    assert!(dispatcher.hook::<TurnPrepare>(Value::Null).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
