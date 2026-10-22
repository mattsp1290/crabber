//! Runtime-owned runner future. Business settlement is separate from joining.
use super::super::attribution::Attribution;
use super::*;

pub(super) struct RunnerLifecycle {
    policy: Arc<Policy>,
    record: Arc<TaskRecord>,
    _permit: tokio::sync::OwnedSemaphorePermit,
    _shared_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl RunnerLifecycle {
    pub(super) fn new(
        policy: Arc<Policy>,
        record: Arc<TaskRecord>,
        permit: tokio::sync::OwnedSemaphorePermit,
        shared_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            policy,
            record,
            _permit: permit,
            _shared_permit: shared_permit,
        }
    }
    pub(super) async fn run(
        self,
        inv: SessionInvocationContext,
        mut request: Request,
    ) -> Result<(), ResourceFailure> {
        request.invocations = inv.invocations.clone();
        let attribution = Attribution::new_from_request(
            &request,
            &self.record.id,
            inv.public_source.clone(),
            self.policy.publication_failures.clone(),
        );
        attribution.start("background", request.profile()).await;
        if self.record.token.is_cancelled() || inv.cancel.is_cancelled() {
            self.record.token.cancel();
            attribution.status("cancelling", "cancelling").await;
            self.finish(State::Cancelled, attribution).await;
            return Ok(());
        }
        {
            let mut state = lock(&self.record.state);
            if matches!(*state, State::Admitted) {
                *state = State::Running;
            }
        }
        let (reply, disposed) = match guarded(&self.policy.runner, request) {
            Err(error) => (Err(error), true),
            Ok(mut safe) => {
                let reply = {
                    let wait = safe.wait();
                    tokio::pin!(wait);
                    tokio::select! { biased;
                        reply = &mut wait => reply,
                        _ = inv.cancel.cancelled() => {
                            self.record.token.cancel();
                            self.policy.settle(&self.record, State::Cancelled);
                            attribution.status("cancelling", "cancelling").await;
                            wait.await
                        }
                        _ = self.record.token.cancelled() => {
                            self.policy.settle(&self.record, State::Cancelled);
                            attribution.status("cancelling", "cancelling").await;
                            wait.await
                        }
                        _ = tokio::time::sleep(self.policy.limits.max_background_duration) => {
                            self.policy.settle(&self.record, State::TimedOut);
                            self.record.token.cancel();
                            wait.await
                        }
                    }
                };
                (reply, safe.dispose())
            }
        };
        let state = if disposed {
            State::reply(reply, &self.policy.limits)
        } else {
            State::Error
        };
        self.finish(state, attribution).await;
        if disposed {
            Ok(())
        } else {
            Err(ResourceFailure::Panicked)
        }
    }
    async fn finish(self, value: State, attribution: Attribution) {
        self.policy.settle(&self.record, value);
        let status = lock(&self.record.state).status_output().0;
        attribution.status("settle", status).await;
        // Field ownership retains capacity through publication and Drop settlement.
    }
}
impl Drop for RunnerLifecycle {
    fn drop(&mut self) {
        self.policy.settle(&self.record, State::Error);
    }
}
