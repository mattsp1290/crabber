//! Disposal of acquired references, including returned streams, is not service shutdown.
use super::{BoundProvider, ProviderReleaseHolds};
use crate::{DeltaStream, ModelRequest, ProviderError, Streamer};
use futures::{FutureExt, Stream, channel::oneshot, future::Shared};
use std::{
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

#[derive(Clone)]
pub struct ProviderReleaseObserver {
    released: Arc<AtomicBool>,
    finished: Shared<futures::future::BoxFuture<'static, ()>>,
}
impl ProviderReleaseObserver {
    #[must_use]
    pub fn is_released(&self) -> bool {
        self.released.load(Ordering::Acquire)
    }
    pub async fn released(&self) {
        self.finished.clone().await;
    }
}
pub(super) struct Signal {
    released: Arc<AtomicBool>,
    sender: Option<oneshot::Sender<()>>,
}
impl Drop for Signal {
    fn drop(&mut self) {
        self.released.store(true, Ordering::Release);
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(());
        }
    }
}
pub(super) fn completion() -> (Arc<Signal>, ProviderReleaseObserver) {
    let (sender, receiver) = oneshot::channel();
    let released = Arc::new(AtomicBool::new(false));
    let observer = ProviderReleaseObserver {
        released: released.clone(),
        finished: async move {
            if receiver.await.is_err() {
                std::future::pending::<()>().await;
            }
        }
        .boxed()
        .shared(),
    };
    (
        Arc::new(Signal {
            released,
            sender: Some(sender),
        }),
        observer,
    )
}
struct ObservedStreamer {
    inner: Option<Arc<dyn Streamer>>,
    signal: Option<Arc<Signal>>,
}
impl Drop for ObservedStreamer {
    fn drop(&mut self) {
        // A panicking original destructor leaves the signal retained. Ordinary
        // field drop during unwind must never turn that panic into an ACK.
        let holds = ProviderReleaseHolds::capture_with_original(
            self.signal.as_ref().expect("live provider signal").clone(),
        );
        let inner = self.inner.take();
        holds.sync_scope(|| dispose(inner, &mut self.signal));
    }
}
#[async_trait::async_trait]
impl Streamer for ObservedStreamer {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        let holds = ProviderReleaseHolds::capture_with_original(
            self.signal.as_ref().expect("live provider signal").clone(),
        );
        let (result, signal) = holds
            .scope(|| {
                super::future_release::observe(
                    async {
                        self.inner
                            .as_ref()
                            .expect("live observed provider")
                            .stream(request)
                            .await
                    },
                    self.signal.as_ref().expect("live provider signal").clone(),
                )
            })
            .await;
        let stream = result?;
        Ok(Box::pin(ObservedStream {
            inner: Some(stream),
            signal: Some(signal),
            holds,
        }))
    }
}
struct ObservedStream {
    inner: Option<DeltaStream>,
    signal: Option<Arc<Signal>>,
    holds: ProviderReleaseHolds,
}
impl Stream for ObservedStream {
    type Item = crate::StreamDelta;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.holds.sync_scope(|| {
            this.inner
                .as_mut()
                .expect("live observed stream")
                .as_mut()
                .poll_next(cx)
        })
    }
}
impl Drop for ObservedStream {
    fn drop(&mut self) {
        let inner = self.inner.take();
        self.holds.sync_scope(|| dispose(inner, &mut self.signal));
    }
}
impl BoundProvider {
    /// Observe disposal of this acquired reference and subsequent clones,
    /// extracted streamers and returned streams. The observer itself does not
    /// retain them. Previously extracted references remain independently owned.
    /// Disposal acknowledges no physical provider/global-service cleanup.
    #[must_use]
    pub fn observe_release(mut self) -> (Self, ProviderReleaseObserver) {
        let (signal, observer) = completion();
        self.streamer = Arc::new(ObservedStreamer {
            inner: Some(self.streamer),
            signal: Some(signal),
        });
        (self, observer)
    }
}

pub(super) fn dispose<T, S>(value: T, signal: &mut Option<Arc<S>>)
where
    S: ?Sized,
{
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
        std::mem::forget(signal.take());
        resume_unwind(payload);
    }
    drop(signal.take());
}
