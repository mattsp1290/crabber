//! Local registration lineage. A retained reference is never a join witness.
use super::{ExecutionOwners, ExecutionTasks, RunId, TaskTermination, WorkerResourceId};
use crate::orchestrator::RuntimeError;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct RootScope {
    registry: Weak<Mutex<ExecutionOwners>>,
    root: Arc<RootIdentity>,
    member: RunId,
}
pub(super) struct RootIdentity {
    stop: CancellationToken,
    reconciled: Mutex<Vec<crabber_session::session_owner::preparation::PreparationReconciliation>>,
}
pub(super) enum Member {
    Task,
    Preparation,
    Resources(WorkerResourceId),
}
pub(super) struct Enrollment {
    identity: Arc<RootIdentity>,
    members: BTreeMap<RunId, Member>,
}
tokio::task_local! { static CURRENT: RootScope; }

impl RootScope {
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.root.stop.clone()
    }
    pub(crate) fn stop(&self) {
        self.root.stop.cancel();
    }
    pub(crate) fn retain_preparation(&self) -> Result<Self, RuntimeError> {
        self.retain(Member::Preparation)
    }
    pub(super) fn retain_resources(&self, worker: WorkerResourceId) -> Result<Self, RuntimeError> {
        self.retain(Member::Resources(worker))
    }
    fn retain(&self, kind: Member) -> Result<Self, RuntimeError> {
        let registry = self.registry.upgrade().ok_or(RuntimeError::HostClosed)?;
        let mut owners = registry.lock().unwrap();
        refresh_resources(&mut owners);
        self.verify(&owners)?;
        Ok(self.insert(&mut owners, kind))
    }
    pub(super) fn belongs_to(&self, tasks: &ExecutionTasks) -> bool {
        self.registry.ptr_eq(&Arc::downgrade(&tasks.owners))
    }
    pub(super) fn verify(&self, owners: &ExecutionOwners) -> Result<(), RuntimeError> {
        if owners
            .enrollments
            .get(&self.root_id())
            .is_some_and(|entry| {
                Arc::ptr_eq(&entry.identity, &self.root) && entry.members.contains_key(&self.member)
            })
        {
            Ok(())
        } else {
            Err(RuntimeError::HostClosed)
        }
    }
    fn root_id(&self) -> usize {
        Arc::as_ptr(&self.root) as usize
    }
    pub(super) fn insert(&self, owners: &mut ExecutionOwners, kind: Member) -> Self {
        let member = RunId::new();
        owners
            .enrollments
            .get_mut(&self.root_id())
            .expect("verified enrollment")
            .members
            .insert(member.clone(), kind);
        Self {
            registry: self.registry.clone(),
            root: self.root.clone(),
            member,
        }
    }
    pub(super) fn record_issue(&self, issue: super::RunCloseIssue) {
        if let Some(registry) = self.registry.upgrade() {
            let mut owners = registry.lock().unwrap();
            if !owners.run_issues.contains(&issue) {
                owners.run_issues.push(issue);
            }
        }
    }
    pub(super) fn record_result<T>(&self, result: &Result<T, RuntimeError>) {
        let Err(RuntimeError::PreparationIncomplete { preparation, .. }) = result else {
            return;
        };
        let reconciled = self.root.reconciled.lock().unwrap();
        if reconciled.contains(preparation.as_ref()) {
            return;
        }
        if let Some(registry) = self.registry.upgrade() {
            let mut owners = registry.lock().unwrap();
            if !owners
                .uncertain_preparations
                .iter()
                .any(|saved| saved == preparation.as_ref())
            {
                owners.uncertain_preparations.push((**preparation).clone());
            }
        }
    }
    pub(crate) fn resolve_preparation(
        &self,
        preparation: &crabber_session::session_owner::preparation::PreparationReconciliation,
    ) {
        let mut reconciled = self.root.reconciled.lock().unwrap();
        if !reconciled.contains(preparation) {
            reconciled.push(preparation.clone());
        }
        if let Some(registry) = self.registry.upgrade() {
            registry
                .lock()
                .unwrap()
                .uncertain_preparations
                .retain(|saved| saved != preparation);
        }
    }
    pub(crate) fn complete(&self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut owners = registry.lock().unwrap();
            if let Some(entry) = owners.enrollments.get_mut(&self.root_id()) {
                entry.members.remove(&self.member);
            }
            owners
                .enrollments
                .retain(|_, entry| !entry.members.is_empty());
        }
    }
    pub(crate) async fn scope<T>(&self, future: impl Future<Output = T>) -> T {
        CURRENT.scope(self.clone(), future).await
    }
    pub(super) fn member(&self) -> &RunId {
        &self.member
    }
}
impl ExecutionTasks {
    pub(super) fn new_root(&self, owners: &mut ExecutionOwners) -> Result<RootScope, RuntimeError> {
        if owners.closed {
            return Err(RuntimeError::HostClosed);
        }
        let root = Arc::new(RootIdentity {
            stop: CancellationToken::new(),
            reconciled: Mutex::new(Vec::new()),
        });
        let member = RunId::new();
        owners.enrollments.insert(
            Arc::as_ptr(&root) as usize,
            Enrollment {
                identity: root.clone(),
                members: BTreeMap::from([(member.clone(), Member::Task)]),
            },
        );
        Ok(RootScope {
            registry: Arc::downgrade(&self.owners),
            root,
            member,
        })
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.owners.lock().unwrap().closed
    }
    pub(crate) fn seal_ingress(&self) {
        self.owners.lock().unwrap().closed = true;
    }
    pub(crate) fn begin_close(&self) -> bool {
        let (signals, own) = {
            let mut owners = self.owners.lock().unwrap();
            owners.closed = true;
            let own = CURRENT
                .try_with(|scope| {
                    scope.registry.ptr_eq(&Arc::downgrade(&self.owners))
                        && scope.verify(&owners).is_ok()
                })
                .unwrap_or(false);
            (
                owners
                    .enrollments
                    .values()
                    .map(|entry| entry.identity.stop.clone())
                    .collect::<Vec<_>>(),
                own,
            )
        };
        for signal in signals {
            signal.cancel();
        }
        own
    }
    pub(crate) fn pending_enrollments(&self) -> Vec<super::PreparationTaskId> {
        self.refresh_resources();
        self.owners
            .lock()
            .unwrap()
            .enrollments
            .values()
            .flat_map(|entry| {
                entry
                    .members
                    .keys()
                    .map(|id| super::PreparationTaskId(id.0.clone()))
            })
            .collect()
    }
    pub(crate) fn uncertain_preparations(
        &self,
    ) -> Vec<crabber_session::session_owner::preparation::PreparationReconciliation> {
        self.owners.lock().unwrap().uncertain_preparations.clone()
    }
    pub(crate) fn failures(&self) -> Vec<super::ExecutionTaskFailure> {
        self.owners.lock().unwrap().failures.clone()
    }
    pub(super) fn record_termination(
        &self,
        task: &super::ExecutionTask,
        termination: TaskTermination,
    ) {
        if termination == TaskTermination::Returned {
            return;
        }
        let mut owners = self.owners.lock().unwrap();
        let id = super::PreparationTaskId(task.local_id.0.clone());
        if !owners.failures.iter().any(|failure| failure.task_id == id) {
            owners.failures.push(super::ExecutionTaskFailure {
                session_id: task.session_id.clone(),
                task_id: id,
                termination,
            });
        }
    }
    pub(super) fn refresh_resources(&self) {
        let mut owners = self.owners.lock().unwrap();
        refresh_resources(&mut owners);
    }
}
fn refresh_resources(owners: &mut ExecutionOwners) {
    let ready = owners
        .enrollments
        .values()
        .flat_map(|entry| {
            let active = entry
                .members
                .values()
                .any(|member| matches!(member, Member::Task));
            entry
                .members
                .iter()
                .filter_map(|(id, member)| match member {
                    Member::Resources(worker) if !active && resources_done(owners, worker) => {
                        Some((Arc::as_ptr(&entry.identity) as usize, id.clone()))
                    }
                    Member::Task | Member::Preparation | Member::Resources(_) => None,
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for (root, member) in ready {
        if let Some(entry) = owners.enrollments.get_mut(&root) {
            entry.members.remove(&member);
        }
    }
    owners
        .enrollments
        .retain(|_, entry| !entry.members.is_empty());
}

fn resources_done(owners: &ExecutionOwners, worker: &WorkerResourceId) -> bool {
    owners
        .resources
        .iter()
        .filter(|scope| {
            scope
                .worker
                .as_ref()
                .is_some_and(|id| Arc::ptr_eq(&id.0, &worker.0))
        })
        .all(|scope| {
            let r = scope.owner.snapshot();
            r.sealed && r.pending == 0
        })
        && super::background::worker_done(owners, worker)
}
