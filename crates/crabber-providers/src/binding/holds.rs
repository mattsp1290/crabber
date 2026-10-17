//! Extra original release references, never a completion or ownership assertion.
use super::ProviderResourceRegistrationError;
use std::{
    any::Any,
    cell::RefCell,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

thread_local! {
    static CURRENT: RefCell<ProviderReleaseHolds> = const { RefCell::new(ProviderReleaseHolds { head: None }) };
}

/// Optional EXTRA clones of original provider release signals and service leases.
/// Dropping these cannot acknowledge earlier than the unchanged outer owners.
/// No join, resource policy or physical cleanup is certified by this value.
#[derive(Clone, Default)]
pub struct ProviderReleaseHolds {
    head: Option<Arc<Frame>>,
}
struct Frame {
    token: Option<Arc<dyn Any + Send + Sync>>,
    outer: Option<Arc<Frame>>,
    depth: Option<usize>,
}
impl Drop for Frame {
    fn drop(&mut self) {
        let mut token = self.token.take();
        let mut outer = self.outer.take();
        loop {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(token.take()))) {
                // An inner lease's failed disposal cannot release outer signals.
                std::mem::forget(outer);
                resume_unwind(payload);
            }
            let Some(frame) = outer.take() else { break };
            match Arc::try_unwrap(frame) {
                Ok(mut frame) => {
                    token = frame.token.take();
                    outer = frame.outer.take();
                }
                Err(shared) => {
                    drop(shared);
                    break;
                }
            }
        }
    }
}
struct Restore {
    previous: Option<ProviderReleaseHolds>,
    installed: Option<ProviderReleaseHolds>,
}
impl Drop for Restore {
    fn drop(&mut self) {
        // Dispose references outside the TLS RefCell borrow; lease Drop may reenter.
        let previous = self.previous.take().unwrap();
        match replace(previous) {
            Ok(current) => drop(current),
            Err(previous) => {
                std::mem::forget(previous);
                std::mem::forget(self.installed.take());
                panic!("provider release context restore unavailable");
            }
        }
    }
}
impl ProviderReleaseHolds {
    /// Capture the COMPLETE current frame, including in Drop. This never truncates
    /// or substitutes an empty frame on a capacity/overflow boundary.
    /// # Panics
    /// Refuses unavailable/tearing-down or reentrantly borrowed context; never
    /// substitutes an empty capture for unknown original references.
    #[must_use]
    pub fn capture_current() -> Self {
        current().unwrap_or_else(|| panic!("provider release context unavailable"))
    }
    /// Check only before NEW work admission. Existing-object Drop/close must
    /// retain the complete capture even when this admission check would refuse.
    /// # Errors
    /// Returns capacity for excessive or overflowed depth.
    pub fn check_admission_limit(
        &self,
        maximum: usize,
    ) -> Result<(), ProviderResourceRegistrationError> {
        if self
            .head
            .as_ref()
            .is_some_and(|head| head.depth.is_none_or(|depth| depth > maximum))
        {
            Err(ProviderResourceRegistrationError::Capacity)
        } else {
            Ok(())
        }
    }
    /// Construct, poll and dispose the actual future in this exact frame.
    pub fn scope<C, F>(&self, construct: C) -> impl Future<Output = F::Output> + use<C, F>
    where
        C: FnOnce() -> F,
        F: Future,
    {
        Scoped {
            future: Some(Box::pin(async move { construct().await })),
            holds: self.clone(),
        }
    }
    /// Install the frame through the original synchronous call and unwind.
    /// # Panics
    /// Propagates the original call panic. Unavailable context retains the call
    /// and holds conservatively rather than executing or disposing them unframed.
    pub fn sync_scope<T>(&self, call: impl FnOnce() -> T) -> T {
        let previous = match replace(self.clone()) {
            Ok(previous) => previous,
            Err(holds) => {
                std::mem::forget(holds);
                std::mem::forget(call);
                panic!("provider release context unavailable");
            }
        };
        let _restore = Restore {
            previous: Some(previous),
            installed: Some(self.clone()),
        };
        call()
    }
    pub(super) fn capture_with_original(token: Arc<dyn Any + Send + Sync>) -> Self {
        if let Some(current) = current() {
            current.with_original(token)
        } else {
            std::mem::forget(token);
            panic!("provider release context unavailable");
        }
    }
    fn with_original(self, token: Arc<dyn Any + Send + Sync>) -> Self {
        let depth = self.head.as_ref().map_or(Some(1), |head| {
            head.depth.and_then(|depth| depth.checked_add(1))
        });
        Self {
            head: Some(Arc::new(Frame {
                token: Some(token),
                outer: self.head,
                depth,
            })),
        }
    }
}
fn current() -> Option<ProviderReleaseHolds> {
    CURRENT
        .try_with(|slot| slot.try_borrow().ok().map(|holds| holds.clone()))
        .ok()
        .flatten()
}
fn replace(holds: ProviderReleaseHolds) -> Result<ProviderReleaseHolds, ProviderReleaseHolds> {
    let mut holds = Some(holds);
    let result = CURRENT
        .try_with(|slot| {
            slot.try_borrow_mut()
                .ok()
                .map(|mut current| std::mem::replace(&mut *current, holds.take().unwrap()))
        })
        .ok()
        .flatten();
    result.ok_or_else(|| holds.take().unwrap())
}
struct Scoped<F> {
    future: Option<Pin<Box<F>>>,
    holds: ProviderReleaseHolds,
}
impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.holds.sync_scope(|| {
            this.future
                .as_mut()
                .expect("live scoped future")
                .as_mut()
                .poll(cx)
        })
    }
}
impl<F> Drop for Scoped<F> {
    fn drop(&mut self) {
        let future = self.future.take();
        self.holds.sync_scope(|| drop(future));
    }
}

#[cfg(test)]
mod tests;
