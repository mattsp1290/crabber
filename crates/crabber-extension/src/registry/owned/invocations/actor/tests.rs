use super::*;
use crate::{Extension, Registrar, Registry, Scope, ToolResourceOwner};
use async_trait::async_trait;
use std::{
    sync::{
        Condvar,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

struct Empty;
#[async_trait]
impl Extension for Empty {
    fn id(&self) -> &'static str {
        "provider-actor-fixture"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        Ok(())
    }
}
struct Closing(CancellationToken);
impl ActorClose for Closing {
    fn close(&self) {
        self.0.cancel();
    }
    fn observed(&self, _: bool) {}
    fn released(&self, success: bool) -> BoxFuture<'static, bool> {
        Box::pin(async move { success })
    }
}
struct Disposal {
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
    drops: AtomicUsize,
}
impl Disposal {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
struct Original(Arc<Disposal>);
impl Drop for Original {
    fn drop(&mut self) {
        self.0.entered.notify_one();
        let mut released = self.0.released.lock().unwrap();
        while !*released {
            released = self.0.wake.wait(released).unwrap();
        }
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn actor_refuses_missing_original_caller_before_constructing_work() {
    let registry = Registry::new();
    let mount = registry
        .mount_owned(Arc::new(Empty), Scope::Global)
        .await
        .unwrap();
    let source = Source::new(&mount.inner.mount);
    let calls = Arc::new(AtomicUsize::new(0));
    let actual = calls.clone();
    let result = source
        .register_actor(Arc::new(Closing(CancellationToken::new())), move |_| {
            actual.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { true })
        })
        .await;
    mount.close(Duration::from_secs(1)).await.unwrap();
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_original_parents_retain_blocked_actual_actor_disposal() {
    let registry = Registry::new();
    let mount = registry
        .mount_owned(Arc::new(Empty), Scope::Global)
        .await
        .unwrap();
    let source = Source::new(&mount.inner.mount);
    let resources = ToolResourceOwner::new();
    let owner =
        super::super::super::CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let disposal = Arc::new(Disposal {
        entered: Notify::new(),
        released: Mutex::new(false),
        wake: Condvar::new(),
        drops: AtomicUsize::new(0),
    });
    let started = Arc::new(Notify::new());
    let begin = started.clone();
    let original = disposal.clone();
    let actor = super::super::super::with_callback_invocations(owner.capability(), async {
        source
            .register_actor(Arc::new(Closing(CancellationToken::new())), move |cancel| {
                Box::pin(async move {
                    let original = Original(original);
                    begin.notify_one();
                    cancel.cancelled().await;
                    drop(original);
                    true
                })
            })
            .await
            .unwrap()
    })
    .await;
    started.notified().await;
    drop(owner);
    disposal.entered.notified().await;
    let parent = resources.drain(Duration::from_millis(10)).await;
    let mount_pending = mount.close(Duration::from_millis(10)).await;
    disposal.release();
    actor.join().await.unwrap();
    let final_parent = resources.drain(Duration::from_secs(1)).await;
    mount.close(Duration::from_secs(1)).await.unwrap();
    assert_eq!(parent.pending, 1);
    assert!(mount_pending.is_err());
    assert_eq!(final_parent.pending, 0);
    assert_eq!(final_parent.failures, [] as [crate::ResourceFailure; 0]);
    assert_eq!(disposal.drops.load(Ordering::SeqCst), 1);
}

struct HeldJoin {
    close: CancellationToken,
    joined: AtomicUsize,
    released: AtomicUsize,
}
impl ActorClose for HeldJoin {
    fn close(&self) {
        self.close.cancel();
    }
    fn observed(&self, success: bool) {
        assert!(success);
        self.joined.fetch_add(1, Ordering::SeqCst);
    }
    fn released(&self, success: bool) -> BoxFuture<'static, bool> {
        self.released.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { success })
    }
}
struct RegisterDuringStream {
    source: Source,
    close: Arc<HeldJoin>,
    actor: tokio::sync::mpsc::UnboundedSender<Actor>,
}
#[async_trait]
impl crabber_providers::Streamer for RegisterDuringStream {
    async fn stream(
        &self,
        _: crabber_providers::ModelRequest,
    ) -> Result<crabber_providers::DeltaStream, crabber_providers::ProviderError> {
        let close = self.close.clone();
        let actor = self
            .source
            .register_actor(self.close.clone(), move |_| {
                Box::pin(async move {
                    close.close.cancelled().await;
                    true
                })
            })
            .await
            .unwrap();
        self.actor.send(actor).ok().unwrap();
        Ok(Box::pin(futures::stream::empty()))
    }
}
impl Drop for Disposal {
    fn drop(&mut self) {
        assert_eq!(self.drops.load(Ordering::SeqCst), 1);
    }
}
struct HeldLease(Arc<Disposal>, bool);
impl Drop for HeldLease {
    fn drop(&mut self) {
        drop(Original(self.0.clone()));
        assert!(!self.1, "original extra lease Drop panic");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_actor_join_cannot_ack_or_reap_before_actual_extra_lease_disposal() {
    retained_lease(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_actor_join_does_not_turn_failed_extra_lease_disposal_into_ack() {
    retained_lease(true).await;
}
async fn retained_lease(panic: bool) {
    use crabber_core::{ProviderBindingDescriptor, ProviderBindingLimits};
    use crabber_providers::{BoundProvider, Selection};
    let registry = Registry::new();
    let mount = registry
        .mount_owned(Arc::new(Empty), Scope::Global)
        .await
        .unwrap();
    let source = Source::new(&mount.inner.mount);
    let resources = ToolResourceOwner::new();
    let owner =
        super::super::super::CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let disposal = Arc::new(Disposal {
        entered: Notify::new(),
        released: Mutex::new(false),
        wake: Condvar::new(),
        drops: AtomicUsize::new(0),
    });
    let closer = Arc::new(HeldJoin {
        close: CancellationToken::new(),
        joined: AtomicUsize::new(0),
        released: AtomicUsize::new(0),
    });
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let selection = Selection {
        provider_id: "actual".into(),
        model_id: "model".into(),
    };
    let descriptor = ProviderBindingDescriptor::new(
        "actual",
        "model",
        "lease-test",
        "1",
        crabber_core::InputFingerprint::new("a".repeat(64)).unwrap(),
        ProviderBindingLimits::default(),
    )
    .unwrap();
    let bound = BoundProvider::new(
        &selection,
        descriptor,
        Arc::new(RegisterDuringStream {
            source,
            close: closer.clone(),
            actor: send,
        }),
    )
    .unwrap()
    .retain_service_lease(Arc::new(HeldLease(disposal.clone(), panic)));
    let request = crabber_providers::ModelRequest {
        context: vec![],
        identity: crabber_providers::RequestIdentity {
            session_id: crabber_core::SessionId::new(),
            run_id: crabber_core::RunId::new(),
            turn_id: crabber_core::TurnId::new(),
        },
        selection,
        system: None,
        messages: vec![],
        tools: vec![],
        temperature: None,
        max_tokens: None,
        reasoning_effort: None,
        tool_choice: None,
    };
    super::super::super::with_callback_invocations(owner.capability(), async {
        let stream = bound.streamer().stream(request).await.unwrap();
        drop(stream);
        drop(bound);
    })
    .await;
    let actor = receive.recv().await.unwrap();
    closer.close.cancel();
    disposal.entered.notified().await;
    let original_joined = closer.joined.load(Ordering::SeqCst);
    let released_before = closer.released.load(Ordering::SeqCst);
    drop(owner);
    let before = resources.drain(Duration::from_millis(10)).await;
    let mount_before = mount.close(Duration::from_millis(10)).await;
    disposal.release();
    let actor_result = actor.join().await;
    let after = resources.drain(Duration::from_secs(1)).await;
    let mounted = mount.close(Duration::from_millis(10)).await;
    assert_eq!(actor_result.is_err(), panic);
    assert_eq!(mounted.is_err(), panic);
    assert_eq!(original_joined, 1);
    assert_eq!(released_before, 0);
    assert_eq!(before.pending, 1);
    assert!(mount_before.is_err());
    assert_eq!(after.pending, 0);
    assert_eq!(!after.failures.is_empty(), panic);
    assert_eq!(closer.released.load(Ordering::SeqCst), 1);
}
