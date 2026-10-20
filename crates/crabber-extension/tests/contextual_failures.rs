mod contextual_support;
use async_trait::async_trait;
use contextual_support::*;
use crabber_extension::*;
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

struct HeldFactory {
    created: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl ExtensionFactory for HeldFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "held".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "held-v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.created.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        tokio::select! {
            () = self.release.notified() => {},
            () = context.cleanup().closing() => return Err(ExtensionError::ContextOwnerClosed),
        }
        Ok(Arc::new(CounterExtension {
            name: "held_counter",
            label: "held".into(),
            counter: Arc::new(AtomicUsize::new(0)),
            observed: Arc::new(Observed::default()),
        }))
    }
}

#[tokio::test]
async fn dropping_or_timing_out_a_waiter_retains_one_initializer_and_no_execution_authority() {
    for cancel in [true, false] {
        let created = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let provider = ContextualPlanProvider::new(
            Registry::new(),
            vec![],
            vec![Arc::new(HeldFactory {
                created: created.clone(),
                entered: entered.clone(),
                release: release.clone(),
            })],
            if cancel {
                Duration::from_secs(1)
            } else {
                Duration::from_millis(20)
            },
        )
        .unwrap();
        let context = context("session", "workspace");
        let waiter = tokio::spawn({
            let provider = provider.clone();
            let context = context.clone();
            async move { provider.acquire_plan_in(&context).await }
        });
        entered.notified().await;
        if cancel {
            waiter.abort();
            let Err(error) = waiter.await else {
                panic!("waiter must be cancelled")
            };
            assert!(error.is_cancelled());
        } else {
            assert!(matches!(
                waiter.await.unwrap(),
                Err(ExtensionError::ContextInitializationIncomplete)
            ));
        }
        release.notify_one();
        let plan = provider.acquire_plan_in(&context).await.unwrap();
        assert_eq!(created.load(Ordering::SeqCst), 1);
        assert_eq!(count(&plan, "held_counter").await, 1);
        drop(plan);
        provider.close_all(Duration::from_secs(1)).await.unwrap();
    }
}

struct FailOnce {
    failed: Arc<AtomicBool>,
    rolled_back: Arc<AtomicUsize>,
    shutdown: Arc<AtomicUsize>,
}
#[async_trait]
impl Extension for FailOnce {
    fn id(&self) -> &'static str {
        "fails-once"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "v1".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let counter = self.rolled_back.clone();
        registrar.defer(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        });
        if !self.failed.swap(true, Ordering::SeqCst) {
            return Err(ExtensionError::Tool("PRIVATE_SETUP_SENTINEL".into()));
        }
        Ok(())
    }
    async fn shutdown(&self) {
        self.shutdown.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn partial_shared_readiness_rolls_back_and_failed_component_is_shut_down() {
    let observed = Arc::new(Observed::default());
    let rollback = Arc::new(AtomicUsize::new(0));
    let shutdown = Arc::new(AtomicUsize::new(0));
    let provider = ContextualPlanProvider::new(
        Registry::new(),
        vec![
            (
                Arc::new(CounterExtension {
                    name: "first",
                    label: "first".into(),
                    counter: Arc::new(AtomicUsize::new(0)),
                    observed: observed.clone(),
                }),
                Scope::Global,
            ),
            (
                Arc::new(FailOnce {
                    failed: Arc::new(AtomicBool::new(false)),
                    rolled_back: rollback.clone(),
                    shutdown: shutdown.clone(),
                }),
                Scope::Global,
            ),
        ],
        vec![],
        Duration::from_secs(1),
    )
    .unwrap();
    let error = provider.ready().await.unwrap_err();
    assert!(!error.to_string().contains("PRIVATE_SETUP_SENTINEL"));
    assert_eq!(rollback.load(Ordering::SeqCst), 1);
    assert_eq!(shutdown.load(Ordering::SeqCst), 1);
    assert_eq!(*observed.shutdown.lock().unwrap(), ["first"]);
    provider.ready().await.unwrap();
    let plan = provider
        .acquire_plan_in(&context("session", "workspace"))
        .await
        .unwrap();
    assert_eq!(count(&plan, "first").await, json!(1));
    drop(plan);
    provider.close_all(Duration::from_secs(1)).await.unwrap();
    assert_eq!(rollback.load(Ordering::SeqCst), 2);
    assert_eq!(shutdown.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn closing_during_creation_signals_the_owned_factory_before_joining() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let provider = provider(vec![Arc::new(HeldFactory {
        created: Arc::new(AtomicUsize::new(0)),
        entered: entered.clone(),
        release: Arc::new(tokio::sync::Notify::new()),
    })]);
    let waiter = tokio::spawn({
        let provider = provider.clone();
        async move {
            provider
                .acquire_plan_in(&context("session", "workspace"))
                .await
        }
    });
    entered.notified().await;
    provider.close_all(Duration::from_secs(1)).await.unwrap();
    assert!(waiter.await.unwrap().is_err());
    assert!(matches!(
        provider.ready().await,
        Err(ExtensionError::RegistryClosed)
    ));
}

struct SharedMetadata {
    panic: bool,
    generation: AtomicUsize,
    reads: AtomicUsize,
    shutdown: Arc<AtomicUsize>,
}
#[async_trait]
impl Extension for SharedMetadata {
    fn id(&self) -> &'static str {
        assert!(!self.panic, "fixture metadata panic");
        "shared-metadata"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.generation.load(Ordering::SeqCst).to_string()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        Ok(())
    }
    async fn shutdown(&self) {
        self.shutdown.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn shared_metadata_is_frozen_once_and_panics_rollback_before_error() {
    let shutdown = Arc::new(AtomicUsize::new(0));
    let shared = Arc::new(SharedMetadata {
        panic: false,
        generation: AtomicUsize::new(1),
        reads: AtomicUsize::new(0),
        shutdown: shutdown.clone(),
    });
    let provider = ContextualPlanProvider::new(
        Registry::new(),
        vec![(shared.clone(), Scope::Global)],
        vec![],
        Duration::from_secs(1),
    )
    .unwrap();
    provider.ready().await.unwrap();
    let first = provider
        .acquire_plan_in(&context("session", "workspace"))
        .await
        .unwrap();
    shared.generation.store(2, Ordering::SeqCst);
    let second = provider
        .acquire_plan_in(&context("session", "workspace"))
        .await
        .unwrap();
    assert_eq!(first.fingerprint(), second.fingerprint());
    assert_eq!(shared.reads.load(Ordering::SeqCst), 1);
    drop((first, second));
    provider.close_all(Duration::from_secs(1)).await.unwrap();
    assert_eq!(shutdown.load(Ordering::SeqCst), 1);

    let observed = Arc::new(Observed::default());
    let failed_shutdown = Arc::new(AtomicUsize::new(0));
    let provider = ContextualPlanProvider::new(
        Registry::new(),
        vec![
            (
                Arc::new(CounterExtension {
                    name: "first",
                    label: "first".into(),
                    counter: Arc::new(AtomicUsize::new(0)),
                    observed: observed.clone(),
                }),
                Scope::Global,
            ),
            (
                Arc::new(SharedMetadata {
                    panic: true,
                    generation: AtomicUsize::new(1),
                    reads: AtomicUsize::new(0),
                    shutdown: failed_shutdown.clone(),
                }),
                Scope::Global,
            ),
        ],
        vec![],
        Duration::from_secs(1),
    )
    .unwrap();
    assert!(matches!(
        provider.registration_fingerprint(&context("session", "workspace")),
        Err(ExtensionError::ContextInitializationFailed { .. })
    ));
    assert_eq!(failed_shutdown.load(Ordering::SeqCst), 0);
    assert!(observed.shutdown.lock().unwrap().is_empty());
    assert!(matches!(
        provider.ready().await,
        Err(ExtensionError::ContextInitializationFailed { .. })
    ));
    assert_eq!(*observed.shutdown.lock().unwrap(), ["first"]);
    assert_eq!(failed_shutdown.load(Ordering::SeqCst), 1);
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}

struct InvalidFactoryMetadata(u8);
#[async_trait]
impl ExtensionFactory for InvalidFactoryMetadata {
    fn identity(&self) -> ComponentIdentity {
        assert_ne!(self.0, 0, "fixture identity panic");
        ComponentIdentity {
            id: "invalid".into(),
            version: if self.0 == 3 { "invalid\nversion" } else { "1" }.into(),
        }
    }
    fn config_hash(&self) -> String {
        assert_ne!(self.0, 1, "fixture config panic");
        "config".into()
    }
    fn scope(&self) -> FactoryScope {
        assert_ne!(self.0, 2, "fixture scope panic");
        FactoryScope::Session
    }
    async fn create(&self, _: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        panic!("invalid metadata must fail before instantiation")
    }
}
#[test]
fn invalid_or_panicking_factory_metadata_fails_before_any_instance_creation() {
    for stage in 0..4 {
        assert!(
            ContextualPlanProvider::new(
                Registry::new(),
                vec![],
                vec![Arc::new(InvalidFactoryMetadata(stage))],
                Duration::from_secs(1)
            )
            .is_err()
        );
    }
}
