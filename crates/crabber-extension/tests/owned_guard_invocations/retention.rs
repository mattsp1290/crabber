use super::*;
use crabber_extension::{CallbackInvocationOwner, ToolResourceOwner, with_callback_invocations};
use std::sync::{Condvar, Mutex};
use tokio::sync::oneshot;

struct Held {
    started: Mutex<Option<oneshot::Sender<()>>>,
    released: Mutex<bool>,
    changed: Condvar,
}
impl Held {
    fn block(&self) {
        self.started
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Payload(Arc<Held>);
impl Drop for Payload {
    fn drop(&mut self) {
        self.0.block();
    }
}
struct Blocking {
    held: Arc<Held>,
    panic: bool,
}
impl ToolGuard for Blocking {
    fn id(&self) -> &'static str {
        "blocked"
    }
    fn check(&self, _: &str, _: &Value) -> GuardDecision {
        if self.panic {
            std::panic::panic_any(Payload(self.held.clone()));
        }
        self.held.block();
        GuardDecision::Allow
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_inline_call_and_panic_payload_disposal_block_both_owners() {
    for panic in [false, true] {
        for contextual in [false, true] {
            let (entered, started) = oneshot::channel();
            let held = Arc::new(Held {
                started: Mutex::new(Some(entered)),
                released: Mutex::new(false),
                changed: Condvar::new(),
            });
            let registry = Registry::new();
            let mount = registry
                .mount_owned(
                    Arc::new(Guards(Arc::new(Blocking {
                        held: held.clone(),
                        panic,
                    }))),
                    Scope::Global,
                )
                .await
                .unwrap();
            let plan = registry.acquire(&SessionId::new());
            let guard = plan.guards[0].clone();
            drop(plan);
            let resources = ToolResourceOwner::new();
            let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
            let capability = owner.capability();
            let waiter = tokio::spawn(with_callback_invocations(capability.clone(), async move {
                let decision = if contextual {
                    check_context(guard.as_ref())
                } else {
                    guard.check("tool", &Value::Null)
                };
                tokio::task::yield_now().await;
                decision
            }));
            tokio::time::timeout(Duration::from_secs(2), started)
                .await
                .unwrap()
                .unwrap();
            waiter.abort();
            drop(owner);
            resources.close();
            let close = mount.close(Duration::from_millis(20)).await;
            let before = resources.snapshot();
            let observed = capability.check_observed_joins();
            held.release();
            let _actual_caller_join = waiter.await;
            let final_resources = resources.drain(Duration::from_secs(2)).await;
            let final_mount = mount.close(Duration::from_secs(2)).await;
            assert!(close.is_err());
            assert_eq!(before.pending, 1);
            assert!(observed.is_err());
            assert_eq!(final_resources.pending, 0);
            assert_eq!(!final_resources.failures.is_empty(), panic);
            assert_eq!(final_mount.is_err(), panic);
        }
    }
}

struct ClosePayload {
    mount: crabber_extension::OwnedMountHandle,
    self_close: Arc<std::sync::atomic::AtomicBool>,
}
impl ClosePayload {
    fn observe(&self) {
        let result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.mount.close(Duration::from_millis(20)))
        });
        self.self_close.store(
            matches!(result, Err(ExtensionError::SelfClose)),
            Ordering::SeqCst,
        );
    }
}
impl Drop for ClosePayload {
    fn drop(&mut self) {
        self.observe();
    }
}
struct Closing {
    payload: Arc<Mutex<Option<ClosePayload>>>,
    panic: bool,
}
impl ToolGuard for Closing {
    fn id(&self) -> &'static str {
        "self-close"
    }
    fn check(&self, _: &str, _: &Value) -> GuardDecision {
        let payload = self.payload.lock().unwrap().take().unwrap();
        if self.panic {
            std::panic::panic_any(payload);
        }
        drop(payload);
        GuardDecision::Allow
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_mount_frame_covers_guard_and_panic_payload_disposal() {
    for panic in [false, true] {
        let registry = Registry::new();
        let payload = Arc::new(Mutex::new(None));
        let self_close = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mount = registry
            .mount_owned(
                Arc::new(Guards(Arc::new(Closing {
                    payload: payload.clone(),
                    panic,
                }))),
                Scope::Global,
            )
            .await
            .unwrap();
        *payload.lock().unwrap() = Some(ClosePayload {
            mount: mount.clone(),
            self_close: self_close.clone(),
        });
        let plan = registry.acquire(&SessionId::new());
        let guard = plan.guards[0].clone();
        drop(plan);
        let decision = guard.check("tool", &Value::Null);
        let final_mount = mount.close(Duration::from_secs(2)).await;
        assert_eq!(decision == GuardDecision::Deny, panic);
        assert_eq!(final_mount.is_err(), panic);
        assert!(self_close.load(Ordering::SeqCst));
    }
}
