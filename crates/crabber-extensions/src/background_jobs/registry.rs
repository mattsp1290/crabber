//! Owner identity, admission reservations and bounded terminal retention.
use super::{
    failure,
    job::{Cause, Job},
    manager::Policy,
    runtime_error,
};
use crabber::{
    core::SessionId,
    extension::{ExtensionError, ToolContext},
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, PartialEq, Eq)]
pub(super) struct Owner {
    pub(super) session: SessionId,
    pub(super) workspace: String,
}
impl Owner {
    pub(super) fn from_context(context: &ToolContext) -> Result<Self, ExtensionError> {
        if context.cancel.is_cancelled() {
            return Err(failure("cancelled"));
        }
        Ok(Self {
            session: context.session_id.clone(),
            workspace: context
                .workspace()
                .workspace_id()
                .ok_or_else(|| runtime_error("owner"))?
                .into(),
        })
    }
}
pub(super) struct Registry {
    pub(super) jobs: BTreeMap<String, Arc<Job>>,
    pub(super) hidden: Vec<Arc<Job>>,
    pub(super) running: usize,
    pub(super) starting: usize,
    pub(super) counter: u64,
    pub(super) epoch: String,
    pub(super) closing: bool,
    pub(super) closed: bool,
}
pub(super) struct Reservation {
    policy: Arc<Policy>,
    pub(super) id: String,
    committed: bool,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut registry = self.policy.registry.lock().unwrap();
            registry.starting -= 1;
            self.policy.starting.send_replace(registry.starting);
        }
    }
}
impl Reservation {
    pub(super) fn commit(mut self, job: Arc<Job>, cancel: bool) -> bool {
        let mut registry = self.policy.registry.lock().unwrap();
        let publish = !registry.closing && !cancel;
        if publish {
            registry.jobs.insert(job.id.clone(), job);
        } else {
            job.set_cause_once(if registry.closing {
                Cause::Close
            } else {
                Cause::Cancelled
            });
            registry.hidden.push(job);
        }
        registry.starting -= 1;
        registry.running += 1;
        self.policy.starting.send_replace(registry.starting);
        self.committed = true;
        publish
    }
}
impl Policy {
    pub(super) fn reserve_start(self: &Arc<Self>) -> Result<Reservation, ExtensionError> {
        let mut registry = self.registry.lock().unwrap();
        if registry.closing {
            return Err(failure("manager-closing"));
        }
        while registry.jobs.len() + registry.hidden.len() + registry.starting
            >= self.configuration.limits.max_tracked
        {
            let oldest = registry
                .jobs
                .values()
                .filter_map(|job| {
                    job.terminal
                        .lock()
                        .unwrap()
                        .as_ref()
                        .map(|t| (t.completed_at, job.id.clone()))
                })
                .min();
            match oldest {
                Some((_, id)) => {
                    registry.jobs.remove(&id);
                }
                None => break,
            }
        }
        if registry.running + registry.starting >= self.configuration.limits.max_running
            || registry.jobs.len() + registry.hidden.len() + registry.starting
                >= self.configuration.limits.max_tracked
        {
            return Err(failure("capacity-exhausted"));
        }
        registry.counter = registry
            .counter
            .checked_add(1)
            .ok_or_else(|| failure("identity-exhausted"))?;
        let id = format!("job_{}_{:016x}", registry.epoch, registry.counter);
        registry.starting += 1;
        self.starting.send_replace(registry.starting);
        Ok(Reservation {
            policy: self.clone(),
            id,
            committed: false,
        })
    }
}
