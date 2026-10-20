//! Observe the literal administrative future before releasing its local call.
use futures::{FutureExt, future::BoxFuture};
use std::panic::AssertUnwindSafe;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::session_owner::owned_scope::task::pending) enum Failure {
    Construct,
    Poll,
    Install,
    Dispose,
    Deadline,
}

struct Original<'a, T, A: FnOnce(), D: FnOnce()> {
    future: Option<BoxFuture<'a, T>>,
    abandoned: Option<A>,
    disposal: Option<D>,
}
impl<T, A: FnOnce(), D: FnOnce()> Original<'_, T, A, D> {
    fn before_dispose(&mut self) -> Option<Box<dyn std::any::Any + Send>> {
        self.disposal
            .take()
            .and_then(|notify| std::panic::catch_unwind(AssertUnwindSafe(notify)).err())
    }
    fn dispose(&mut self) -> Result<(), Box<dyn std::any::Any + Send>> {
        let notification = self.before_dispose();
        drop(self.abandoned.take());
        let future = self.future.take();
        let disposed = std::panic::catch_unwind(AssertUnwindSafe(|| drop(future)));
        if let Some(payload) = notification {
            if let Err(dispose_payload) = disposed {
                super::super::retain::discard(dispose_payload);
            }
            Err(payload)
        } else {
            disposed
        }
    }
}
impl<T, A: FnOnce(), D: FnOnce()> Drop for Original<'_, T, A, D> {
    fn drop(&mut self) {
        if self.future.is_none() {
            return;
        }
        // Cancellation or unwinding of the observer still owns the literal
        // future. Notify before Drop, and contain Drop independently even then.
        let disposal_notification = self.before_dispose();
        // Cancellation itself is already a real observation failure. Its
        // sticky Unresolved status may supersede DisposalPending before Drop.
        let notification = self
            .abandoned
            .take()
            .and_then(|notify| std::panic::catch_unwind(AssertUnwindSafe(notify)).err());
        let disposed = self.dispose();
        if let Some(payload) = disposal_notification {
            super::super::retain::discard(payload);
        }
        if let Some(payload) = notification {
            super::super::retain::discard(payload);
        }
        if let Err(payload) = disposed {
            super::super::retain::discard(payload);
        }
    }
}

/// `install` stages the actual result before literal-future disposal. An error
/// never accepts that staged result. Disposal is announced once before every
/// literal Drop, including Ready, panic, deadline and observer cancellation.
pub(in crate::session_owner::owned_scope::task::pending) async fn original<'a, T>(
    construct: impl FnOnce() -> BoxFuture<'a, T>,
    install: impl FnOnce(T),
    deadline: Option<Instant>,
    before_dispose: impl FnOnce(),
    on_abandon: impl FnOnce(),
) -> Result<(), Failure> {
    let future = match std::panic::catch_unwind(AssertUnwindSafe(construct)) {
        Ok(future) => future,
        Err(payload) => {
            super::super::retain::discard(payload);
            return Err(Failure::Construct);
        }
    };
    let mut original = Original {
        future: Some(future),
        abandoned: Some(on_abandon),
        disposal: Some(before_dispose),
    };
    let polled = match deadline {
        Some(deadline) => tokio::time::timeout_at(
            deadline,
            AssertUnwindSafe(original.future.as_mut().ok_or(Failure::Dispose)?).catch_unwind(),
        )
        .await
        .ok(),
        None => Some(
            AssertUnwindSafe(original.future.as_mut().ok_or(Failure::Dispose)?)
                .catch_unwind()
                .await,
        ),
    };
    let output = match polled {
        Some(Ok(value)) => std::panic::catch_unwind(AssertUnwindSafe(|| install(value)))
            .map_err(|payload| (Failure::Install, payload)),
        Some(Err(payload)) => Err((Failure::Poll, payload)),
        None => {
            let disposed = original.dispose();
            if let Err(payload) = disposed {
                super::super::retain::discard(payload);
                return Err(Failure::Dispose);
            }
            return Err(Failure::Deadline);
        }
    };
    // Keep the literal named future alive across a caught poll panic. Its Drop
    // must not execute while that panic is unwinding another async wrapper.
    let disposed = original.dispose();
    let output = match output {
        Ok(value) => Ok(value),
        Err((failure, payload)) => {
            super::super::retain::discard(payload);
            Err(failure)
        }
    };
    if let Err(payload) = disposed {
        super::super::retain::discard(payload);
        return Err(Failure::Dispose);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };

    struct Literal {
        poll_panic: bool,
        drop_panic: bool,
        staged: Arc<AtomicBool>,
        drops: Arc<AtomicUsize>,
    }
    impl Future for Literal {
        type Output = Result<u8, u8>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            assert!(!self.poll_panic, "literal native poll failure");
            Poll::Ready(Err(7))
        }
    }
    impl Drop for Literal {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            if !self.poll_panic {
                assert!(
                    self.staged.load(Ordering::SeqCst),
                    "output not installed before Drop"
                );
            }
            assert!(!self.drop_panic, "literal native Drop failure");
        }
    }

    #[tokio::test]
    async fn literal_poll_drop_and_semantic_results_are_separate() {
        for (poll_panic, drop_panic) in [(false, false), (true, false), (false, true), (true, true)]
        {
            let staged = Arc::new(AtomicBool::new(false));
            let drops = Arc::new(AtomicUsize::new(0));
            let disposal_notifications = Arc::new(AtomicUsize::new(0));
            let result = original(
                || {
                    Box::pin(Literal {
                        poll_panic,
                        drop_panic,
                        staged: staged.clone(),
                        drops: drops.clone(),
                    })
                },
                |value| {
                    staged.store(true, Ordering::SeqCst);
                    assert_eq!(value, Err(7));
                },
                None,
                || {
                    disposal_notifications.fetch_add(1, Ordering::SeqCst);
                },
                || panic!("not abandoned"),
            )
            .await;
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(disposal_notifications.load(Ordering::SeqCst), 1);
            assert_eq!(staged.load(Ordering::SeqCst), !poll_panic);
            assert_eq!(
                result,
                if drop_panic {
                    Err(Failure::Dispose)
                } else if poll_panic {
                    Err(Failure::Poll)
                } else {
                    Ok(())
                }
            );
        }
    }

    #[tokio::test]
    async fn deadline_is_visible_before_actual_disposal() {
        struct Pending(Arc<AtomicBool>);
        impl Future for Pending {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                Poll::Pending
            }
        }
        impl Drop for Pending {
            fn drop(&mut self) {
                assert!(self.0.load(Ordering::SeqCst));
            }
        }
        let notified = Arc::new(AtomicBool::new(false));
        let result = original(
            || Box::pin(Pending(notified.clone())),
            |()| panic!("pending output"),
            Some(Instant::now()),
            || {
                notified.store(true, Ordering::SeqCst);
            },
            || panic!("not abandoned"),
        )
        .await;
        assert_eq!(result, Err(Failure::Deadline));
    }

    #[tokio::test]
    async fn cancelled_observer_notifies_before_contained_literal_drop() {
        struct PendingDrop(Arc<AtomicBool>, Arc<AtomicBool>, Arc<Mutex<&'static str>>);
        impl Future for PendingDrop {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                Poll::Pending
            }
        }
        impl Drop for PendingDrop {
            fn drop(&mut self) {
                assert!(self.0.load(Ordering::SeqCst));
                assert_eq!(*self.2.lock().unwrap(), "unresolved");
                self.1.store(true, Ordering::SeqCst);
                panic!("literal cancelled Drop failure");
            }
        }
        let notified = Arc::new(AtomicBool::new(false));
        let disposed = Arc::new(AtomicBool::new(false));
        let disposal_notifications = Arc::new(AtomicUsize::new(0));
        let status = Arc::new(Mutex::new("polling"));
        let mut observer = Box::pin(original(
            || {
                Box::pin(PendingDrop(
                    notified.clone(),
                    disposed.clone(),
                    status.clone(),
                ))
            },
            |()| panic!("pending output"),
            None,
            || {
                disposal_notifications.fetch_add(1, Ordering::SeqCst);
                *status.lock().unwrap() = "disposal_pending";
            },
            || {
                notified.store(true, Ordering::SeqCst);
                *status.lock().unwrap() = "unresolved";
            },
        ));
        assert!(futures::poll!(&mut observer).is_pending());
        assert!(std::panic::catch_unwind(AssertUnwindSafe(|| drop(observer))).is_ok());
        assert!(notified.load(Ordering::SeqCst));
        assert!(disposed.load(Ordering::SeqCst));
        assert_eq!(disposal_notifications.load(Ordering::SeqCst), 1);
        assert_eq!(*status.lock().unwrap(), "unresolved");
    }
}
