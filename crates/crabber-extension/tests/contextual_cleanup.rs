mod contextual_support;
use async_trait::async_trait;
use contextual_support::*;
use crabber_extension::*;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

struct CleanupFactory {
    tracker: Arc<Mutex<Option<CleanupTracker>>>,
    release: Arc<Notify>,
    failure: Option<bool>,
}

#[async_trait]
impl ExtensionFactory for CleanupFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "cleanup".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        format!("{:?}", self.failure)
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        *self.tracker.lock().unwrap() = Some(context.cleanup().clone());
        let closing = context.cleanup().closing();
        let release = self.release.clone();
        let failure = self.failure;
        context
            .cleanup()
            .spawn_checked(async move {
                closing.await;
                match failure {
                    Some(true) => panic!("factory cleanup panic"),
                    Some(false) => return Err(ResourceFailure::CleanupFailed),
                    None => release.notified().await,
                }
                Ok(())
            })
            .unwrap();
        Ok(Arc::new(CounterExtension {
            name: "cleanup",
            label: "cleanup".into(),
            counter: Arc::new(AtomicUsize::new(0)),
            observed: Arc::new(Observed::default()),
        }))
    }
}

#[tokio::test]
async fn timed_out_factory_cleanup_keeps_join_and_seals_retained_tracker() {
    let tracker = Arc::new(Mutex::new(None));
    let release = Arc::new(Notify::new());
    let provider = provider(vec![Arc::new(CleanupFactory {
        tracker: tracker.clone(),
        release: release.clone(),
        failure: None,
    })]);
    let context = context("session", "workspace");
    provider.acquire_plan_in(&context).await.unwrap().release();
    assert_eq!(
        provider.close_session(&context, Duration::ZERO).await,
        Err(ExtensionError::ContextCleanupIncomplete)
    );
    let tracker = tracker.lock().unwrap().clone().unwrap();
    assert!(tracker.is_closing());
    let polled = Arc::new(AtomicBool::new(false));
    let flag = polled.clone();
    assert_eq!(
        tracker.spawn_checked(async move {
            flag.store(true, Ordering::SeqCst);
            Ok(())
        }),
        Err(ResourceRegistrationError::Sealed)
    );
    assert!(!polled.load(Ordering::SeqCst));
    release.notify_one();
    provider
        .close_session(&context, Duration::from_secs(1))
        .await
        .unwrap();
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn factory_cleanup_failure_or_panic_never_becomes_success_on_retry() {
    for panic in [false, true] {
        let provider = provider(vec![Arc::new(CleanupFactory {
            tracker: Arc::new(Mutex::new(None)),
            release: Arc::new(Notify::new()),
            failure: Some(panic),
        })]);
        let context = context("session", "workspace");
        provider.acquire_plan_in(&context).await.unwrap().release();
        for _ in 0..2 {
            assert_eq!(
                provider
                    .close_session(&context, Duration::from_secs(1))
                    .await,
                Err(ExtensionError::ContextCleanupIncomplete)
            );
        }
        assert_eq!(
            provider.close_all(Duration::from_secs(1)).await,
            Err(ExtensionError::ContextCleanupIncomplete)
        );
    }
}
