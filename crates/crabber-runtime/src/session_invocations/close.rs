//! Terminal insertion cut and observation of the actual invocation supervisors.
use super::{Invocation, SessionInvocationReport, SessionInvocationStatus, SessionInvocations};
use std::sync::{Arc, atomic::Ordering};

tokio::task_local! { static CURRENT_OWNER: Arc<Invocation>; }
pub(super) async fn scope<T>(
    owner: Arc<Invocation>,
    future: impl std::future::Future<Output = T>,
) -> T {
    CURRENT_OWNER.scope(owner, future).await
}

pub(crate) struct InvocationCloseSelection {
    owners: Vec<Arc<Invocation>>,
}
impl SessionInvocations {
    pub(crate) fn begin_close(&self) -> InvocationCloseSelection {
        let owners = self.owners.lock().unwrap();
        self.closed.store(true, Ordering::Release);
        InvocationCloseSelection {
            owners: owners.values().cloned().collect(),
        }
    }
    pub(crate) fn snapshot_close(&self) -> Vec<SessionInvocationReport> {
        let owners = self
            .owners
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        owners.iter().map(|owner| owner.close_report()).collect()
    }
    pub(crate) fn is_current_owner(&self) -> bool {
        let owners = self
            .owners
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        CURRENT_OWNER
            .try_with(|current| owners.iter().any(|owner| Arc::ptr_eq(current, owner)))
            .unwrap_or(false)
            || owners
                .iter()
                .any(|owner| owner.resources.is_current_callback_parent())
    }
}
impl Invocation {
    pub(super) fn close_report(&self) -> SessionInvocationReport {
        // Acquire completion first so subsequent failure/status reads see the
        // exact join result published before that completion.
        let local_joined = self.observed.load(Ordering::Acquire);
        SessionInvocationReport {
            session_id: self.session.clone(),
            id: self.request.id.clone(),
            status: if self.join_failed.load(Ordering::Acquire) {
                SessionInvocationStatus::Uncertain
            } else {
                *self.status.lock().unwrap()
            },
            local_joined,
            resources: self.resources.snapshot(),
        }
    }
}
impl InvocationCloseSelection {
    pub(crate) fn signal_all(&self) {
        for owner in &self.owners {
            owner.cancel.cancel();
        }
    }
    pub(crate) fn snapshot(&self) -> Vec<SessionInvocationReport> {
        self.owners
            .iter()
            .map(|owner| owner.close_report())
            .collect()
    }
    pub(crate) async fn observe_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Vec<SessionInvocationReport> {
        for owner in &self.owners {
            let Ok(mut join) = tokio::time::timeout_at(deadline, owner.join.lock()).await else {
                continue;
            };
            let error = if let Some(handle) = join.as_mut()
                && let Ok(result) = tokio::time::timeout_at(deadline, handle).await
            {
                owner.publish_join_result(&mut join, result)
            } else {
                None
            };
            let local_joined = join.is_none();
            drop(join);
            if let Some(error) = error {
                super::worker::discard_panic(error);
            }
            // Main owns sealing/cleanup registration until actual completion.
            if local_joined {
                owner
                    .resources
                    .drain(deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .await;
            }
        }
        self.snapshot()
    }
}

#[cfg(test)]
mod tests;
