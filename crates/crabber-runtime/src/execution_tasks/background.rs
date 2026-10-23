//! Worker background-task ownership is separate from in-main tool resource joins.
use super::{ExecutionOwners, ExecutionTasks, WorkerResourceId, WorkerResources};
use crabber_core::{RunId, SessionId};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{future::Future, sync::Arc, time::Duration};

/// A fixed library-owned background task class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerResourceKind {
    /// Lease renewal and durable-control observations, retained through original joins.
    LeaseHeartbeat,
    /// Original extension callback joins retained independently of waiter results.
    ExtensionCallbackInvocations,
    /// Model middleware joined before a native attempt can conclude.
    ModelCallbackInvocations,
    InterruptedSettlementNotification,
}

/// Actual retained background tasks, independent of the worker result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResourceDrain {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub kind: WorkerResourceKind,
    pub resources: crabber_extension::ResourceDrain,
}
pub(super) struct OwnedBackgroundResources {
    session_id: SessionId,
    run_id: RunId,
    worker: WorkerResourceId,
    kind: WorkerResourceKind,
    owner: crabber_extension::ToolResourceOwner,
}
impl WorkerResources {
    pub(crate) fn background_resources(
        &self,
        kind: WorkerResourceKind,
    ) -> crabber_extension::ToolResourceOwner {
        let owner = crabber_extension::ToolResourceOwner::new();
        self.tasks.refresh_resources();
        let mut retained = self.tasks.owners.lock().unwrap();
        if self
            .scope
            .as_ref()
            .is_some_and(|scope| !scope.belongs_to(&self.tasks) || scope.verify(&retained).is_err())
        {
            owner.close();
            return owner;
        }
        retained
            .background_tasks
            .push(Arc::new(OwnedBackgroundResources {
                session_id: self.session_id.clone(),
                run_id: self.run_id.clone(),
                worker: self.id.clone(),
                kind,
                owner: owner.clone(),
            }));
        owner
    }
    pub(crate) fn spawn_background(
        &self,
        kind: WorkerResourceKind,
        task: impl Future<Output = Result<(), crabber_extension::ResourceFailure>> + Send + 'static,
    ) -> Result<(), crabber_extension::ResourceRegistrationError> {
        let owner = crabber_extension::ToolResourceOwner::new();
        self.tasks.refresh_resources();
        let mut owners = self.tasks.owners.lock().unwrap();
        if self
            .scope
            .as_ref()
            .is_some_and(|scope| !scope.belongs_to(&self.tasks) || scope.verify(&owners).is_err())
        {
            return Err(crabber_extension::ResourceRegistrationError::Sealed);
        }
        owners
            .background_tasks
            .push(Arc::new(OwnedBackgroundResources {
                session_id: self.session_id.clone(),
                run_id: self.run_id.clone(),
                worker: self.id.clone(),
                kind,
                owner: owner.clone(),
            }));
        // Keep snapshot/close outside this registration handoff. No delivery
        // can be sealed between publication and actual task registration.
        let result = owner.resources().spawn(task);
        owner.close();
        result
    }
    /// Reap only actual observed successful model-callback joins. Active and
    /// failed scopes remain retained; worker identity only selects local owners.
    pub(crate) fn reap_model_callbacks(&self) {
        self.tasks
            .owners
            .lock()
            .unwrap()
            .background_tasks
            .retain(|scope| {
                if !Arc::ptr_eq(&scope.worker.0, &self.id.0)
                    || scope.kind != WorkerResourceKind::ModelCallbackInvocations
                {
                    return true;
                }
                let report = scope.owner.snapshot();
                !report.sealed || report.pending != 0 || !report.failures.is_empty()
            });
    }
    fn background_scopes(&self) -> Vec<Arc<OwnedBackgroundResources>> {
        self.tasks
            .owners
            .lock()
            .unwrap()
            .background_tasks
            .iter()
            .filter(|scope| Arc::ptr_eq(&scope.worker.0, &self.id.0))
            .cloned()
            .collect()
    }
    pub(crate) async fn finish_background(&self) -> Result<(), ()> {
        loop {
            let scopes = self.background_scopes();
            for scope in &scopes {
                scope.owner.close();
            }
            let mut failed = false;
            let mut pending = false;
            for scope in scopes {
                let report = scope.owner.drain(Duration::from_secs(5)).await;
                failed |= !report.failures.is_empty();
                pending |= report.pending != 0;
            }
            if failed {
                return Err(());
            }
            if !pending
                && self.background_scopes().iter().all(|scope| {
                    let report = scope.owner.snapshot();
                    report.sealed && report.pending == 0 && report.failures.is_empty()
                })
            {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }
}
impl ExecutionTasks {
    pub(super) async fn drain_background(
        &self,
        session: Option<&SessionId>,
        deadline: tokio::time::Instant,
    ) {
        let scopes = self
            .owners
            .lock()
            .unwrap()
            .background_tasks
            .iter()
            .filter(|scope| session.is_none_or(|id| scope.session_id == *id))
            .cloned()
            .collect::<Vec<_>>();
        let mut joins = FuturesUnordered::new();
        for scope in scopes {
            joins.push(async move {
                if scope.owner.snapshot().sealed {
                    scope
                        .owner
                        .drain(deadline.saturating_duration_since(tokio::time::Instant::now()))
                        .await;
                }
            });
        }
        while joins.next().await.is_some() {}
    }
}
pub(super) fn snapshots(
    owners: &mut ExecutionOwners,
    session: Option<&SessionId>,
) -> Vec<WorkerResourceDrain> {
    let reports = owners
        .background_tasks
        .iter()
        .filter(|scope| session.is_none_or(|id| scope.session_id == *id))
        .map(|scope| WorkerResourceDrain {
            session_id: scope.session_id.clone(),
            run_id: scope.run_id.clone(),
            kind: scope.kind,
            resources: scope.owner.snapshot(),
        })
        .collect();
    owners.background_tasks.retain(|scope| {
        let report = scope.owner.snapshot();
        !report.sealed || report.pending != 0 || !report.failures.is_empty()
    });
    reports
}

// Exact retained worker identity; no native or routing equality is used.
pub(super) fn worker_done(owners: &ExecutionOwners, worker: &WorkerResourceId) -> bool {
    owners
        .background_tasks
        .iter()
        .filter(|scope| Arc::ptr_eq(&scope.worker.0, &worker.0))
        .all(|scope| {
            let r = scope.owner.snapshot();
            r.sealed && r.pending == 0
        })
}

pub(super) fn callback_owners(
    owners: &ExecutionOwners,
) -> impl Iterator<Item = crabber_extension::ToolResourceOwner> + '_ {
    owners
        .background_tasks
        .iter()
        .map(|scope| scope.owner.clone())
}
