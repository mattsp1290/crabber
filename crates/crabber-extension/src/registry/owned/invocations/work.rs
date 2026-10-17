//! The same original callback body, separated from locked roster publication.
use super::{CallbackInvocationCapability, Mount, OriginalRoster, observe};
use futures::future::BoxFuture;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub(super) struct Work<T, F> {
    pub(super) mount: Arc<Mount>,
    pub(super) caller: Option<CallbackInvocationCapability>,
    pub(super) subset: Option<OriginalRoster>,
    pub(super) construct: F,
    pub(super) cancel: CancellationToken,
    pub(super) send: oneshot::Sender<T>,
}
impl<T: Send + 'static, F: FnOnce() -> BoxFuture<'static, T> + Send + 'static> Work<T, F> {
    pub(super) async fn run(self, ready: oneshot::Receiver<()>) -> bool {
        if ready.await.is_err() {
            return true;
        }
        // Actual-body admission: no await between the close checks and FnOnce.
        if !*self.mount.active.lock().unwrap() || self.cancel.is_cancelled() {
            return true;
        }
        if self
            .caller
            .as_ref()
            .is_some_and(CallbackInvocationCapability::is_closed)
        {
            return true;
        }
        if self
            .subset
            .as_ref()
            .is_some_and(|scope| scope.state.tasks.lock().unwrap().sealed)
        {
            return true;
        }
        let output = observe(self.construct, self.cancel).await;
        let success = output.is_ok();
        if let Ok(Some(output)) = output {
            let _ = self.send.send(output);
        }
        success
    }
}
