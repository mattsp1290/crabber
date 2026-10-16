//! Worker background-task ownership is separate from in-main tool resource joins.
use super::{ExecutionOwners, ExecutionTasks, WorkerResourceId, WorkerResources};
use crabber_core::{RunId, SessionId};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{future::Future, sync::Arc, time::Duration};

/// A fixed library-owned background task class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerResourceKind {
    LeaseHeartbeat,
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
        self.tasks
            .owners
            .lock()
            .unwrap()
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
        let mut owners = self.tasks.owners.lock().unwrap();
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
