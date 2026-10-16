//! Heartbeat task ownership is separate from in-main tool resource joins.
use super::{ExecutionOwners, ExecutionTasks, WorkerResourceId, WorkerResources};
use crabber_core::{RunId, SessionId};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{sync::Arc, time::Duration};

/// Actual retained heartbeat tasks, independent of the worker result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResourceDrain {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub resources: crabber_extension::ResourceDrain,
}
pub(super) struct OwnedHeartbeatResources {
    session_id: SessionId,
    run_id: RunId,
    worker: WorkerResourceId,
    owner: crabber_extension::ToolResourceOwner,
}
impl WorkerResources {
    pub(crate) fn heartbeat_resources(&self) -> crabber_extension::ToolResourceOwner {
        let owner = crabber_extension::ToolResourceOwner::new();
        self.tasks
            .owners
            .lock()
            .unwrap()
            .heartbeats
            .push(Arc::new(OwnedHeartbeatResources {
                session_id: self.session_id.clone(),
                run_id: self.run_id.clone(),
                worker: self.id.clone(),
                owner: owner.clone(),
            }));
        owner
    }
    fn heartbeat_scopes(&self) -> Vec<Arc<OwnedHeartbeatResources>> {
        self.tasks
            .owners
            .lock()
            .unwrap()
            .heartbeats
            .iter()
            .filter(|scope| Arc::ptr_eq(&scope.worker.0, &self.id.0))
            .cloned()
            .collect()
    }
    pub(super) async fn finish_heartbeats(&self) -> Result<(), ()> {
        loop {
            let scopes = self.heartbeat_scopes();
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
                && self.heartbeat_scopes().iter().all(|scope| {
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
    pub(super) async fn drain_heartbeats(
        &self,
        session: Option<&SessionId>,
        deadline: tokio::time::Instant,
    ) {
        let scopes = self
            .owners
            .lock()
            .unwrap()
            .heartbeats
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
) -> Vec<HeartbeatResourceDrain> {
    let reports = owners
        .heartbeats
        .iter()
        .filter(|scope| session.is_none_or(|id| scope.session_id == *id))
        .map(|scope| HeartbeatResourceDrain {
            session_id: scope.session_id.clone(),
            run_id: scope.run_id.clone(),
            resources: scope.owner.snapshot(),
        })
        .collect();
    owners.heartbeats.retain(|scope| {
        let report = scope.owner.snapshot();
        !report.sealed || report.pending != 0 || !report.failures.is_empty()
    });
    reports
}
