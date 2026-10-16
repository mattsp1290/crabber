//! Retained registration and actual joins, independent of the parent run.
mod cancellation;
mod registration;
mod scoped;
mod worker;
use async_trait::async_trait;
use crabber_core::{SessionId, SessionOwnerId};
use crabber_extension::{
    SessionInvocationAdmission as Admission, SessionInvocationError as Error,
    SessionInvocationRequest as Request, SessionInvocationSpawner, SessionInvocationTask,
};
use crabber_session::{PhaseAuthority, PhaseTarget, Store};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionInvocationStatus {
    Registering,
    Active,
    Joined,
    Replayed,
    /// May have committed. Neither a task return nor expiry clears this state.
    Uncertain,
    Rejected,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInvocationReport {
    pub session_id: SessionId,
    pub id: SessionOwnerId,
    pub status: SessionInvocationStatus,
    /// Actual local supervisor join, independent of durable owner state.
    pub local_joined: bool,
    pub resources: crabber_extension::ResourceDrain,
}
struct Invocation {
    session: SessionId,
    request: Request,
    cancel: CancellationToken,
    cancel_observation: Mutex<Option<crabber_session::session_owner::ObserveSessionOwnerCancel>>,
    status: Mutex<SessionInvocationStatus>,
    resources: crabber_extension::ToolResourceOwner,
    join: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Invocation {
    fn status(&self, status: SessionInvocationStatus) {
        *self.status.lock().unwrap() = status;
    }
}
type Owners = BTreeMap<(SessionId, SessionOwnerId), Arc<Invocation>>;
#[derive(Default, Clone)]
pub(crate) struct SessionInvocations {
    owners: Arc<Mutex<Owners>>,
}
#[derive(Clone)]
pub(crate) struct Parent {
    pub session: SessionId,
    pub run: crabber_core::RunId,
    pub workspace: String,
    pub directory: String,
    pub phase: PhaseTarget,
    pub authority: PhaseAuthority,
}
struct BoundSpawner {
    owners: SessionInvocations,
    store: Arc<dyn Store>,
    parent: Parent,
}
impl SessionInvocations {
    async fn reap_finished(&self) {
        let candidates = self
            .owners
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for owner in candidates {
            let Ok(mut join) = owner.join.try_lock() else {
                continue;
            };
            if let Some(handle) = join.as_mut() {
                if !handle.is_finished() {
                    continue;
                }
                if let Err(error) = handle.await {
                    worker::discard_panic(error);
                    owner.status(SessionInvocationStatus::Uncertain);
                }
                *join = None;
            }
            let status = *owner.status.lock().unwrap();
            let resources = owner.resources.snapshot();
            if !matches!(
                status,
                SessionInvocationStatus::Joined
                    | SessionInvocationStatus::Replayed
                    | SessionInvocationStatus::Rejected
            ) || !resources.sealed
                || resources.pending != 0
                || !resources.failures.is_empty()
            {
                continue;
            }
            let mut owners = self.owners.lock().unwrap();
            let key = (owner.session.clone(), owner.request.id.clone());
            if owners
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, &owner))
            {
                owners.remove(&key);
            }
        }
    }
    pub(crate) fn bind(
        &self,
        store: Arc<dyn Store>,
        parent: Parent,
    ) -> crabber_extension::SessionInvocations {
        crabber_extension::SessionInvocations::from_host(Arc::new(BoundSpawner {
            owners: self.clone(),
            store,
            parent,
        }))
    }
    pub(crate) async fn drain(
        &self,
        session: &SessionId,
        cancel: bool,
        bound: Duration,
    ) -> Vec<SessionInvocationReport> {
        let owners = self
            .owners
            .lock()
            .unwrap()
            .values()
            .filter(|owner| owner.session == *session)
            .cloned()
            .collect::<Vec<_>>();
        // Signal all selected jobs before awaiting any one of them.
        if cancel {
            for owner in &owners {
                owner.cancel.cancel();
            }
        }
        let deadline = tokio::time::Instant::now() + bound;
        let mut reports = Vec::with_capacity(owners.len());
        for owner in owners {
            let Ok(mut join) = tokio::time::timeout_at(deadline, owner.join.lock()).await else {
                reports.push(SessionInvocationReport {
                    session_id: owner.session.clone(),
                    id: owner.request.id.clone(),
                    status: *owner.status.lock().unwrap(),
                    local_joined: false,
                    resources: owner.resources.snapshot(),
                });
                continue;
            };
            if let Some(handle) = join.as_mut()
                && let Ok(result) = tokio::time::timeout_at(deadline, handle).await
            {
                if let Err(error) = result {
                    worker::discard_panic(error);
                    owner.status(SessionInvocationStatus::Uncertain);
                }
                *join = None;
            }
            let resources = if join.is_none() || cancel {
                owner
                    .resources
                    .drain(deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .await
            } else {
                owner.resources.snapshot()
            };
            reports.push(SessionInvocationReport {
                session_id: owner.session.clone(),
                id: owner.request.id.clone(),
                status: *owner.status.lock().unwrap(),
                local_joined: join.is_none(),
                resources,
            });
        }
        reports
    }
}
#[async_trait]
impl SessionInvocationSpawner for BoundSpawner {
    async fn spawn(
        &self,
        request: Request,
        task: SessionInvocationTask,
    ) -> Result<Admission, Error> {
        if !self.store.supports_session_owners() {
            return Err(Error::Unsupported);
        }
        registration::validate_request(&self.parent, &request)?;
        self.owners.reap_finished().await;
        let (sender, receiver) = oneshot::channel();
        let existing = {
            let mut owners = self.owners.owners.lock().unwrap();
            let key = (self.parent.session.clone(), request.id.clone());
            if owners.contains_key(&key) {
                true
            } else {
                if owners.len() >= 1024 {
                    return Err(Error::Capacity);
                }
                let invocation = Arc::new(Invocation {
                    session: self.parent.session.clone(),
                    request: request.clone(),
                    cancel: CancellationToken::new(),
                    cancel_observation: Mutex::new(None),
                    status: Mutex::new(SessionInvocationStatus::Registering),
                    resources: crabber_extension::ToolResourceOwner::new(),
                    join: tokio::sync::Mutex::new(None),
                });
                let retained = invocation.clone();
                let store = self.store.clone();
                let parent = self.parent.clone();
                *invocation
                    .join
                    .try_lock()
                    .expect("new invocation join lock") = Some(tokio::spawn(async move {
                    worker::run(store, parent, retained, task, sender).await;
                }));
                owners.insert(key, invocation);
                false
            }
        };
        if existing {
            return registration::replay(&*self.store, &self.parent, &request).await;
        }
        receiver.await.map_err(|_| Error::UnknownOutcome)?
    }
}
