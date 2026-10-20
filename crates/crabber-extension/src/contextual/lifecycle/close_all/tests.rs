//! Actual contextual initializer/shutdown ownership across synchronous close cuts.
use crate::{ContextualPlanProvider, Extension, ExtensionError, Registrar, Registry, Scope};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::Instant,
};

struct Seen {
    installed: AtomicUsize,
    shutdown_started: AtomicUsize,
    shutdown_finished: AtomicUsize,
    entered: Notify,
    release: Semaphore,
    provider: Mutex<Option<ContextualPlanProvider>>,
    independent: Mutex<Option<ContextualPlanProvider>>,
    independent_ready: Mutex<Option<Result<(), ExtensionError>>>,
    reentry: Mutex<Option<[Result<(), ExtensionError>; 3]>>,
}
impl Default for Seen {
    fn default() -> Self {
        Self {
            installed: AtomicUsize::new(0),
            shutdown_started: AtomicUsize::new(0),
            shutdown_finished: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Semaphore::new(0),
            provider: Mutex::new(None),
            independent: Mutex::new(None),
            independent_ready: Mutex::new(None),
            reentry: Mutex::new(None),
        }
    }
}
struct HeldExtension {
    seen: Arc<Seen>,
    reenter: bool,
}
#[async_trait::async_trait]
impl Extension for HeldExtension {
    fn id(&self) -> &'static str {
        "synchronous-close-fixture"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        format!("reenter:{}", self.reenter)
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        self.seen.installed.fetch_add(1, Ordering::SeqCst);
        if self.reenter {
            let provider = self.seen.provider.lock().unwrap().as_ref().unwrap().clone();
            let ready = provider.check_ready_allowed();
            let close = provider.begin_close_all(Duration::from_secs(1)).map(|_| ());
            let async_ready = provider.ready().await;
            *self.seen.reentry.lock().unwrap() = Some([ready, close, async_ready]);
            let independent = self.seen.independent.lock().unwrap().clone();
            if let Some(independent) = independent {
                let result = independent.ready().await;
                *self.seen.independent_ready.lock().unwrap() = Some(result);
            }
        }
        Ok(())
    }
    async fn shutdown(&self) {
        self.seen.shutdown_started.fetch_add(1, Ordering::SeqCst);
        self.seen.entered.notify_one();
        self.seen.release.acquire().await.unwrap().forget();
        self.seen.shutdown_finished.fetch_add(1, Ordering::SeqCst);
    }
}
fn fixture(reenter: bool) -> (ContextualPlanProvider, Arc<Seen>) {
    let seen = Arc::new(Seen::default());
    let provider = ContextualPlanProvider::new(
        Registry::new(),
        vec![(
            Arc::new(HeldExtension {
                seen: seen.clone(),
                reenter,
            }),
            Scope::Global,
        )],
        vec![],
        Duration::from_secs(1),
    )
    .unwrap();
    if reenter {
        *seen.provider.lock().unwrap() = Some(provider.clone());
    }
    (provider, seen)
}

#[tokio::test]
async fn synchronous_cut_retains_same_shutdown_after_deadline_and_caller_drop() {
    let (provider, seen) = fixture(false);
    provider.ready().await.unwrap();
    let observation = provider.begin_close_all(Duration::from_secs(1)).unwrap();
    assert!(provider.is_closed());
    assert_eq!(
        provider.check_ready_allowed(),
        Err(ExtensionError::RegistryClosed)
    );
    {
        let owners = provider.inner.owners.lock().unwrap();
        assert!(!owners.slots.is_empty());
        for slot in owners.slots.values() {
            let state = slot.state.lock().unwrap();
            assert!(state.closed);
            assert!(
                state
                    .records
                    .iter()
                    .all(|record| record.cleanup.tracker().is_closing())
            );
        }
    }
    tokio::time::timeout(Duration::from_secs(1), seen.entered.notified())
        .await
        .unwrap();
    assert_eq!(seen.shutdown_started.load(Ordering::SeqCst), 1);
    assert_eq!(seen.shutdown_finished.load(Ordering::SeqCst), 0);
    assert_eq!(
        observation
            .observe_until(Instant::now() + Duration::from_millis(15))
            .await,
        Err(ExtensionError::ContextCleanupIncomplete)
    );
    assert_eq!(observation.result(), None);
    let original = Arc::downgrade(&observation.pass);
    let mut cancelled_wait =
        Box::pin(observation.observe_until(Instant::now() + Duration::from_secs(1)));
    assert!(futures::poll!(&mut cancelled_wait).is_pending());
    drop(cancelled_wait);
    drop(observation);
    let first = provider.begin_close_all(Duration::from_secs(1)).unwrap();
    let second = provider.begin_close_all(Duration::from_secs(1)).unwrap();
    assert!(Arc::ptr_eq(&original.upgrade().unwrap(), &first.pass));
    assert!(Arc::ptr_eq(&first.pass, &second.pass));
    seen.release.add_permits(1);
    let deadline = Instant::now() + Duration::from_secs(1);
    let (left, right) = tokio::join!(
        first.observe_until(deadline),
        second.observe_until(deadline)
    );
    left.unwrap();
    right.unwrap();
    assert_eq!(first.result(), Some(Ok(())));
    provider.close_all(Duration::from_secs(1)).await.unwrap();
    assert_eq!(seen.installed.load(Ordering::SeqCst), 1);
    assert_eq!(seen.shutdown_started.load(Ordering::SeqCst), 1);
    assert_eq!(seen.shutdown_finished.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn real_shared_initializer_checks_reentry_before_moving_to_an_observer() {
    let (provider, seen) = fixture(true);
    let (independent, independent_seen) = fixture(false);
    *seen.independent.lock().unwrap() = Some(independent.clone());
    tokio::time::timeout(Duration::from_secs(1), provider.ready())
        .await
        .unwrap()
        .unwrap();
    let reentry = seen.reentry.lock().unwrap().take().unwrap();
    assert_eq!(
        reentry,
        [
            Err(ExtensionError::SelfClose),
            Err(ExtensionError::SelfClose),
            Err(ExtensionError::SelfClose)
        ]
    );
    assert_eq!(*seen.independent_ready.lock().unwrap(), Some(Ok(())));
    assert_eq!(independent_seen.installed.load(Ordering::SeqCst), 1);
    assert!(!provider.is_closed());
    assert_eq!(seen.installed.load(Ordering::SeqCst), 1);
    assert_eq!(seen.shutdown_started.load(Ordering::SeqCst), 0);
    // Break the fixture's intentional initializer-to-provider lookup after its
    // actual install returned; this value is never native ownership evidence.
    let lookup = seen.provider.lock().unwrap().take();
    drop(lookup);
    let independent_lookup = seen.independent.lock().unwrap().take();
    drop(independent_lookup);
    seen.release.add_permits(1);
    provider.close_all(Duration::from_secs(1)).await.unwrap();
    assert_eq!(seen.shutdown_started.load(Ordering::SeqCst), 1);
    assert_eq!(seen.shutdown_finished.load(Ordering::SeqCst), 1);
    independent_seen.release.add_permits(1);
    independent.close_all(Duration::from_secs(1)).await.unwrap();
    assert_eq!(independent_seen.shutdown_finished.load(Ordering::SeqCst), 1);
}
