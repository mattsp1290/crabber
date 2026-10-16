mod contextual_support;
use async_trait::async_trait;
use contextual_support::context;
use crabber_core::{RunId, SessionId, ToolCallId, ToolOutput};
use crabber_extension::*;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
const BOUND: Duration = Duration::from_secs(2);
#[derive(Default)]
struct Seen {
    factory: Mutex<Option<CleanupTracker>>,
    mount: Mutex<Option<CleanupTracker>>,
    factory_release: Notify,
    mount_release: Notify,
    created: AtomicUsize,
    stopped: AtomicUsize,
    shutdown: AtomicUsize,
}
struct Factory {
    seen: Arc<Seen>,
    mode: u8,
}
#[async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "owned-instance".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        format!("mode{}", self.mode)
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        assert_eq!(
            context.generation().cleanup_policy(),
            ServiceGenerationCleanupPolicy::FullySealedFactory
        );
        self.seen.created.fetch_add(1, Ordering::SeqCst);
        *self.seen.factory.lock().unwrap() = Some(context.cleanup().clone());
        let closing = context.cleanup().closing();
        let seen = self.seen.clone();
        let wait = self.mode == 0;
        let (send, receive) = std::sync::mpsc::channel();
        context
            .cleanup()
            .spawn_checked(async move {
                closing.await;
                if wait {
                    seen.factory_release.notified().await;
                }
                seen.stopped.fetch_add(1, Ordering::SeqCst);
                let _ = send.send(());
                Ok(())
            })
            .unwrap();
        Ok(Arc::new(Instance {
            seen: self.seen.clone(),
            mode: self.mode,
            rollback: Mutex::new(Some(receive)),
        }))
    }
}
struct Instance {
    seen: Arc<Seen>,
    mode: u8,
    rollback: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}
#[async_trait]
impl Extension for Instance {
    fn id(&self) -> &'static str {
        "owned-instance"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        assert_ne!(self.mode, 4, "metadata failure");
        "v1".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        if matches!(self.mode, 1 | 2) {
            let receiver = self.rollback.lock().unwrap().take().unwrap();
            let panic = self.mode == 2;
            registrar.defer(move || {
                receiver
                    .recv_timeout(BOUND)
                    .expect("factory must be sealed before rollback");
                assert!(!panic, "rollback failure");
            });
            return Err(ExtensionError::Plan("known installation failure".into()));
        }
        let seen = self.seen.clone();
        registrar.on_result_transform(
            0,
            "owned-result",
            Arc::new(move |context, value| {
                *seen.mount.lock().unwrap() = Some(context.cleanup().clone());
                let closing = context.cleanup().closing();
                let seen = seen.clone();
                context
                    .cleanup()
                    .spawn_checked(async move {
                        closing.await;
                        seen.mount_release.notified().await;
                        Ok(())
                    })
                    .unwrap();
                Box::pin(async move { Ok(TransformOutput::new(value)) })
            }),
        );
        Ok(())
    }
    async fn shutdown(&self) {
        assert!(!matches!(self.mode, 3 | 4), "shutdown failure");
        self.seen.shutdown.fetch_add(1, Ordering::SeqCst);
    }
}
fn provider(seen: Arc<Seen>, mode: u8) -> ContextualPlanProvider {
    ContextualPlanProvider::new(
        Registry::new().with_close_timeout(BOUND),
        vec![],
        vec![Arc::new(Factory { seen, mode })],
        BOUND,
    )
    .unwrap()
}
#[tokio::test]
async fn factory_and_mount_cleanup_are_sealed_and_both_actual_joins_are_required() {
    let seen = Arc::new(Seen::default());
    let provider = provider(seen.clone(), 0);
    let context = context("a", "workspace");
    let declaration = provider.registration_fingerprint(&context).unwrap();
    let plan = provider.acquire_plan_in(&context).await.unwrap();
    assert_eq!(
        provider.registration_fingerprint(&context).unwrap(),
        declaration
    );
    assert!(
        provider
            .preparation_resource_contract(&context)
            .unwrap()
            .is_none()
    );
    let phase = ToolResourceOwner::new();
    let invocation = ToolResultContext::new(
        "test".into(),
        true,
        ToolInput::Normalized(serde_json::Value::Null),
        ToolCallId::new(),
        SessionId::new(),
        RunId::new(),
        ToolOutcomeClass::Succeeded,
    )
    .with_cleanup(phase.cleanup_tracker());
    assert!(matches!(
        plan.dispatcher
            .transform_tool_result(
                invocation,
                TransformOutput::new(ToolOutput::from_json(&serde_json::Value::Null).unwrap())
            )
            .await,
        ToolResultOutcome::Completed { .. }
    ));
    plan.release();
    assert!(
        provider
            .close_session(&context, Duration::ZERO)
            .await
            .is_err()
    );
    for retained in [&seen.factory, &seen.mount] {
        assert_eq!(
            retained
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .spawn_checked(async { panic!("sealed must reject before poll") }),
            Err(ResourceRegistrationError::Sealed)
        );
    }
    phase.close();
    seen.mount_release.notify_one();
    assert_eq!(phase.drain(BOUND).await.joined, 1);
    assert!(
        provider
            .close_session(&context, Duration::ZERO)
            .await
            .is_err()
    );
    seen.factory_release.notify_one();
    provider.close_session(&context, BOUND).await.unwrap();
    assert_eq!(seen.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(seen.shutdown.load(Ordering::SeqCst), 1);
    let generations = provider.service_generations();
    assert!(
        generations
            .iter()
            .any(|g| g.cleanup_policy() == ServiceGenerationCleanupPolicy::FullySealedFactory)
    );
    assert!(
        generations
            .iter()
            .any(|g| g.cleanup_policy() == ServiceGenerationCleanupPolicy::LegacySharedMount)
    );
    provider.close_all(BOUND).await.unwrap();
}
#[tokio::test]
async fn failed_install_seals_factory_before_awaiting_its_deferred_rollback() {
    let seen = Arc::new(Seen::default());
    let provider = provider(seen.clone(), 1);
    assert!(matches!(
        provider.acquire_plan_in(&context("a", "workspace")).await,
        Err(ExtensionError::ContextInitializationFailed { .. })
    ));
    assert_eq!(seen.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(seen.shutdown.load(Ordering::SeqCst), 1);
    provider.close_all(BOUND).await.unwrap();
}
#[tokio::test]
async fn rollback_or_shutdown_panics_stay_incomplete_on_retries() {
    for mode in [2, 3, 4] {
        let seen = Arc::new(Seen::default());
        let provider = provider(seen.clone(), mode);
        let context = context("a", "workspace");
        match provider.acquire_plan_in(&context).await {
            Ok(plan) => plan.release(),
            Err(_) => assert_ne!(mode, 3),
        }
        for _ in 0..2 {
            assert!(provider.close_session(&context, BOUND).await.is_err());
        }
        assert!(provider.close_all(BOUND).await.is_err());
        assert_eq!(seen.created.load(Ordering::SeqCst), 1);
        assert_eq!(seen.stopped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn an_owned_base_mount_does_not_upgrade_shared_generation_policy() {
    let seen = Arc::new(Seen::default());
    let registry = Registry::new();
    let external = registry
        .mount_owned(
            Arc::new(Instance {
                seen: seen.clone(),
                mode: 5,
                rollback: Mutex::new(None),
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    let provider = ContextualPlanProvider::new(registry, vec![], vec![], BOUND).unwrap();
    let context = context("a", "workspace");
    provider.acquire_plan_in(&context).await.unwrap().release();
    assert!(
        provider
            .service_generations()
            .iter()
            .all(|generation| generation.cleanup_policy()
                == ServiceGenerationCleanupPolicy::LegacySharedMount)
    );
    assert!(
        provider
            .preparation_resource_contract(&context)
            .unwrap()
            .is_none()
    );
    provider.close_session(&context, BOUND).await.unwrap();
    assert_eq!(seen.shutdown.load(Ordering::SeqCst), 0);
    provider.close_all(BOUND).await.unwrap();
    assert_eq!(external.close(BOUND).await.unwrap().close_task().joined, 1);
    assert_eq!(seen.shutdown.load(Ordering::SeqCst), 1);
}
