use super::{Registry, capture};
use crate::{Extension, ExtensionError, Registrar, Scope};
use async_trait::async_trait;
use crabber_core::SessionContext;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct ReentrantMetadata {
    registry: Registry,
    panic: Arc<AtomicBool>,
    cleanup: Arc<AtomicUsize>,
}
struct PanickingPayload;
impl Drop for PanickingPayload {
    fn drop(&mut self) {
        panic!("metadata payload drop");
    }
}
#[async_trait]
impl Extension for ReentrantMetadata {
    fn id(&self) -> &'static str {
        "reentrant"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        if self.panic.load(Ordering::SeqCst) {
            std::panic::panic_any(PanickingPayload);
        }
        let inner = self
            .registry
            .inner
            .try_lock()
            .expect("metadata must not hold registry lock");
        for mount in &inner.mounts {
            assert!(
                mount.registrar.try_lock().is_ok(),
                "metadata must not hold registrar lock"
            );
        }
        "opaque-config".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let cleanup = self.cleanup.clone();
        registrar.defer(move || {
            cleanup.fetch_add(1, Ordering::SeqCst);
        });
        Ok(())
    }
}
#[tokio::test]
async fn snapshot_metadata_holds_neither_registration_locks_nor_cleanup_callbacks() {
    let registry = Registry::new();
    let cleanup = Arc::new(AtomicUsize::new(0));
    let panic = Arc::new(AtomicBool::new(false));
    let handle = registry
        .mount(
            Arc::new(ReentrantMetadata {
                registry: registry.clone(),
                panic: panic.clone(),
                cleanup: cleanup.clone(),
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    let snapshot = capture(&registry).unwrap();
    assert_eq!(cleanup.load(Ordering::SeqCst), 0);
    panic.store(true, Ordering::SeqCst);
    assert!(matches!(
        capture(&registry),
        Err(ExtensionError::RegistrationDescriptorUnavailable)
    ));
    assert!(registry.inner.lock().is_ok());
    {
        let root = registry.inner.lock().unwrap();
        assert!(root.mounts[0].registrar.lock().is_ok());
    }
    handle.close().await.unwrap();
    assert_eq!(cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(
        snapshot
            .for_context(&SessionContext::new(
                "session".into(),
                "workspace",
                "/workspace"
            ))
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

struct Configuration(&'static str, bool);
#[async_trait]
impl Extension for Configuration {
    fn id(&self) -> &'static str {
        "configuration"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        self.0.into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let callback: crate::dispatch::Callback =
            Arc::new(|value| Box::pin(async move { Ok(value) }));
        if self.1 {
            registrar.on_gate("custom/point", 0, "same-id", callback);
        } else {
            registrar.on_transform("custom/point", 0, "same-id", callback);
        }
        Ok(())
    }
}
async fn frame(config: &'static str, gate: bool) -> serde_json::Value {
    let registry = Registry::new();
    let handle = registry
        .mount(Arc::new(Configuration(config, gate)), Scope::Global)
        .await
        .unwrap();
    let value = capture(&registry)
        .unwrap()
        .for_context(&SessionContext::new(
            "session".into(),
            "workspace",
            "/workspace",
        ));
    handle.close().await.unwrap();
    value
}
#[tokio::test]
async fn exact_scalar_configuration_and_dispatch_mode_are_fingerprinted() {
    let first = "1111111111111111111111111111111111111111111111111111111111111111";
    let second = "1111111111111111111111111111111111111111111111111111111111111112";
    assert_ne!(frame(first, false).await, frame(second, false).await);
    assert_ne!(frame(first, false).await, frame(first, true).await);
}
