//! Disposal of acquired references, including returned streams, is not service shutdown.
use super::BoundProvider;
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
struct Signal {
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
struct ObservedStreamer {
    inner: Option<Arc<dyn Streamer>>,
    signal: Option<Arc<Signal>>,
}
impl Drop for ObservedStreamer {
    fn drop(&mut self) {
        // A panicking original destructor leaves the signal retained. Ordinary
        // field drop during unwind must never turn that panic into an ACK.
        dispose(self.inner.take(), &mut self.signal);
    }
}
#[async_trait::async_trait]
impl Streamer for ObservedStreamer {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        let stream = self
            .inner
            .as_ref()
            .expect("live observed provider")
            .stream(request)
            .await?;
        Ok(Box::pin(ObservedStream {
            inner: Some(stream),
            signal: Some(self.signal.as_ref().expect("live provider signal").clone()),
        }))
    }
}
struct ObservedStream {
    inner: Option<DeltaStream>,
    signal: Option<Arc<Signal>>,
}
impl Stream for ObservedStream {
    type Item = crate::StreamDelta;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .inner
            .as_mut()
            .expect("live observed stream")
            .as_mut()
            .poll_next(cx)
    }
}
impl Drop for ObservedStream {
    fn drop(&mut self) {
        dispose(self.inner.take(), &mut self.signal);
    }
}
impl BoundProvider {
    /// Observe disposal of this acquired reference and subsequent clones,
    /// extracted streamers and returned streams. The observer itself does not
    /// retain them. Previously extracted references remain independently owned.
    /// Disposal acknowledges no physical provider/global-service cleanup.
    #[must_use]
    pub fn observe_release(mut self) -> (Self, ProviderReleaseObserver) {
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
        self.streamer = Arc::new(ObservedStreamer {
            inner: Some(self.streamer),
            signal: Some(Arc::new(Signal {
                released,
                sender: Some(sender),
            })),
        });
        (self, observer)
    }
}

fn dispose<T>(value: T, signal: &mut Option<Arc<Signal>>) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
        std::mem::forget(signal.take());
        resume_unwind(payload);
    }
    drop(signal.take());
}
