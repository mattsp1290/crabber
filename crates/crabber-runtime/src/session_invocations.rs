//! Retained registration and actual joins, independent of the parent run.
mod cancellation;
mod close;
pub(crate) use close::InvocationCloseSelection;
mod registration;
mod scoped;
mod worker;
use async_trait::async_trait;
use crabber_core::{SessionId, SessionOwnerId};
use crabber_extension::{
    SessionInvocationAdmission as Admission, SessionInvocationError as Error,
    SessionInvocationRequest as Request, SessionInvocationSpawner, SessionInvocationTask,
};
use crabber_session::{PhaseTarget, Store};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
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
    observed: AtomicBool,
    join_failed: AtomicBool,
}
impl Invocation {
    fn status(&self, status: SessionInvocationStatus) {
        *self.status.lock().unwrap() = status;
    }
    fn publish_join_result(
        &self,
        join: &mut Option<tokio::task::JoinHandle<()>>,
        result: Result<(), tokio::task::JoinError>,
    ) -> Option<tokio::task::JoinError> {
        *join = None;
        let error = result.err();
        if error.is_some() {
            self.join_failed.store(true, Ordering::Release);
            self.status(SessionInvocationStatus::Uncertain);
        }
        // Failure/status precede the publication read first by close snapshots.
        self.observed.store(true, Ordering::Release);
        error
    }
}
type Owners = BTreeMap<(SessionId, SessionOwnerId), Arc<Invocation>>;
#[derive(Default, Clone)]
pub(crate) struct SessionInvocations {
    owners: Arc<Mutex<Owners>>,
    closed: Arc<AtomicBool>,
}
#[derive(Clone)]
pub(crate) struct Parent {
    pub session: SessionId,
    pub run: crabber_core::RunId,
    pub workspace: String,
    pub directory: String,
    pub phase: PhaseTarget,
    pub authority: crabber_session::session_owner::SessionOwnerParentAuthority,
    pub invocation: Option<crabber_session::session_owner::SessionOwnerTarget>,
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
            let error = if let Some(handle) = join.as_mut() {
                if !handle.is_finished() {
                    continue;
                }
                let result = handle.await;
                owner.publish_join_result(&mut join, result)
            } else {
                None
            };
            drop(join);
            if let Some(error) = error {
                worker::discard_panic(error);
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
        let Some(deadline) = tokio::time::Instant::now().checked_add(bound) else {
            // Signalling above is real; an unrepresentable wait observes no new joins.
            return owners.iter().map(|owner| owner.close_report()).collect();
        };
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
                worker::discard_panic(error);
            }
            let resources = if local_joined || cancel {
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
                local_joined,
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
            if self.owners.closed.load(Ordering::Acquire) {
                drop(owners);
                return Err(Error::Rejected);
            }
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
                    observed: AtomicBool::new(false),
                    join_failed: AtomicBool::new(false),
                });
                let retained = invocation.clone();
                let store = self.store.clone();
                let parent = self.parent.clone();
                let original = invocation.clone();
                let manager = self.owners.clone();
                *invocation
                    .join
                    .try_lock()
                    .expect("new invocation join lock") =
                    Some(tokio::spawn(close::scope(original, async move {
                        worker::run(store, parent, manager, retained, task, sender).await;
                    })));
                owners.insert(key, invocation);
                false
            }
        };
        if existing {
            return store_call(async {
                Ok(registration::replay(&*self.store, &self.parent, &request).await)
            })
            .await
            .unwrap_or(Err(Error::UnknownOutcome));
        }
        receiver.await.map_err(|_| Error::UnknownOutcome)?
    }
}

/// Untrusted Store construction/polling cannot unwind through physical owners.
async fn store_call<T>(
    call: impl std::future::Future<
        Output = Result<T, crabber_session::session_owner::SessionOwnerError>,
    >,
) -> Result<T, crabber_session::session_owner::SessionOwnerError> {
    use futures::FutureExt;
    match std::panic::AssertUnwindSafe(call).catch_unwind().await {
        Ok(result) => result,
        Err(payload) => {
            if let Err(nested) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
            {
                std::mem::forget(nested);
            }
            Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome)
        }
    }
}

impl Parent {
    fn nested(&self, authority: &crabber_session::session_owner::SessionOwnerAuthority) -> Self {
        Self {
            session: self.session.clone(),
            run: self.run.clone(),
            workspace: self.workspace.clone(),
            directory: self.directory.clone(),
            phase: self.phase.clone(),
            authority: crabber_session::session_owner::SessionOwnerParentAuthority::Invocation(
                authority.clone(),
            ),
            invocation: Some(authority.target().clone()),
        }
    }
}
