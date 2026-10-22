pub(super) use super::registry::{Owner, Registry};
use super::{
    config::Configuration,
    failure, input,
    job::{self, Cause, Job, State},
    runtime_error,
};
use crate::process::{self, Launch, Tail, Tails};
use crabber::extension::{CleanupOwner, ExtensionError, ToolContext};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, hash_map::RandomState},
    hash::{BuildHasher, Hasher},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch},
    time::{Instant, timeout_at},
};

pub(super) struct Policy {
    pub(super) binding: Option<crate::contextual::Binding>,
    pub(super) shared: Option<super::SharedCapacity>,
    pub(super) configuration: Configuration,
    pub(super) cleanup: CleanupOwner,
    pub(super) registry: Mutex<Registry>,
    pub(super) starting: watch::Sender<usize>,
    #[cfg(test)]
    pub(super) test_hooks: Mutex<TestHooks>,
}
#[cfg(test)]
type AfterSpawn = Arc<
    dyn Fn(&str) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync,
>;
#[cfg(test)]
#[derive(Default)]
pub(super) struct TestHooks {
    pub(super) after_spawn: Option<AfterSpawn>,
    #[cfg(unix)]
    pub(super) pgids: Vec<i32>,
    #[cfg(unix)]
    pub(super) fail_kill: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(unix)]
    pub(super) signals: Arc<Mutex<Vec<process::GroupSignal>>>,
}

impl Policy {
    pub(super) fn new(configuration: Configuration) -> Self {
        let epoch: String = (0..2)
            .map(|_| {
                let mut hash = RandomState::new().build_hasher();
                hash.write(b"background-jobs-epoch");
                format!("{:016x}", hash.finish())
            })
            .collect();
        Self {
            binding: None,
            shared: None,
            configuration,
            cleanup: CleanupOwner::new(),
            registry: Mutex::new(Registry {
                jobs: BTreeMap::new(),
                hidden: Vec::new(),
                running: 0,
                starting: 0,
                counter: 0,
                epoch,
                closing: false,
                closed: false,
            }),
            starting: watch::channel(0).0,
            #[cfg(test)]
            test_hooks: Mutex::new(TestHooks::default()),
        }
    }
    pub(super) fn owner_report(&self) -> super::OwnerReport {
        let registry = self.registry.lock().unwrap();
        super::OwnerReport {
            live: registry.running + registry.starting,
            tracked: registry.jobs.len() + registry.hidden.len(),
            closing: registry.closing,
            closed: registry.closed && self.cleanup.tracker().pending() == 0,
            ..Default::default()
        }
    }
    pub(super) fn live_jobs(&self) -> usize {
        let registry = self.registry.lock().unwrap();
        registry.running + registry.starting
    }

    pub(super) async fn start(
        self: &Arc<Self>,
        context: ToolContext,
        value: Value,
    ) -> Result<Value, ExtensionError> {
        let owner = Owner::from_context(&context)?;
        let root = context
            .workspace()
            .directory()
            .ok_or_else(|| runtime_error("workspace-root"))?;
        let input = input::start(&value, &self.configuration.limits)?;
        let shared_permit = self
            .shared
            .as_ref()
            .map(|pool| pool.acquire())
            .transpose()
            .map_err(|_| failure("capacity-exhausted"))?;
        let reservation = self.reserve_start()?;
        let root = std::fs::canonicalize(root).map_err(|_| runtime_error("workspace-root"))?;
        let directory = std::fs::canonicalize(root.join(input.directory))
            .map_err(|_| runtime_error("working-directory"))?;
        if !directory.starts_with(&root) || !Path::new(&directory).is_dir() {
            return Err(runtime_error("working-directory"));
        }
        let tails = Tails {
            stdout: Arc::new(Mutex::new(Tail::new(
                self.configuration.limits.max_output_bytes_per_stream,
            ))),
            stderr: Arc::new(Mutex::new(Tail::new(
                self.configuration.limits.max_output_bytes_per_stream,
            ))),
        };
        let spawned = process::spawn(
            Launch {
                shell: &self.configuration.shell,
                command: &input.command,
                directory: &directory,
                environment: &self.configuration.environment,
            },
            tails.clone(),
            &self.cleanup.tracker(),
        )
        .map_err(|_| failure("spawn-failed"))?;
        #[cfg(all(test, unix))]
        let spawned = {
            let mut spawned = spawned;
            let hooks = self.test_hooks.lock().unwrap();
            spawned.set_signal_hooks(hooks.fail_kill.clone(), hooks.signals.clone());
            spawned
        };
        #[cfg(all(test, unix))]
        self.test_hooks
            .lock()
            .unwrap()
            .pgids
            .push(spawned.pgid().as_raw_nonzero().get());
        let job = Arc::new(Job::new(
            reservation.id.clone(),
            owner,
            input.timeout,
            tails,
        ));
        *job.shared_permit.lock().unwrap() = shared_permit;
        let policy = self.clone();
        let cancel = context.cancel.clone();
        let (sender, receiver) = oneshot::channel();
        self.cleanup.tracker().spawn(async move {
            #[cfg(test)]
            {
                let hook = policy.test_hooks.lock().unwrap().after_spawn.clone();
                if let Some(hook) = hook {
                    hook(&job.id).await;
                }
            }
            let published = reservation.commit(job.clone(), cancel.is_cancelled());
            let (group, gate_failed) = if published {
                match spawned.release_gate().await {
                    Ok(group) => (group, false),
                    Err(error) => {
                        let mut registry = policy.registry.lock().unwrap();
                        registry.jobs.remove(&job.id);
                        registry.hidden.push(job.clone());
                        job.set_cause_once(Cause::Close);
                        (*error.group, true)
                    }
                }
            } else {
                (spawned.withhold_gate(), false)
            };
            let result = if gate_failed {
                Err(failure("gate"))
            } else if published {
                Ok(job.start_result())
            } else {
                Err(failure("cancelled"))
            };
            if published && !gate_failed && job.timeout_seconds > 0 {
                let timer_job = job.clone();
                let mut done = job.done.subscribe();
                policy.cleanup.tracker().spawn(async move {
                    tokio::select! {
                        biased;
                        _ = done.wait_for(|done| *done) => {},
                        () = tokio::time::sleep(Duration::from_secs(timer_job.timeout_seconds)) => {
                            timer_job.set_cause_once(Cause::Timeout);
                            timer_job.request_attempt();
                        }
                    }
                });
            }
            policy.cleanup.tracker().spawn(job::coordinate(
                policy.clone(),
                job,
                group,
                gate_failed,
            ));
            let _ = sender.send(result);
        });
        tokio::select! {
            biased;
            () = context.cancel.cancelled() => Err(failure("cancelled")),
            result = receiver => result.unwrap_or_else(|_| Err(failure("cancelled"))),
        }
    }
    pub(super) fn job_completed(&self, job: &Job) {
        let mut registry = self.registry.lock().unwrap();
        registry.running -= 1;
        job.shared_permit.lock().unwrap().take();
        registry.hidden.retain(|hidden| hidden.id != job.id);
    }
    fn lookup(&self, owner: &Owner, id: &str) -> Result<Arc<Job>, ExtensionError> {
        self.registry
            .lock()
            .unwrap()
            .jobs
            .get(id)
            .filter(|job| job.owner == *owner)
            .cloned()
            .ok_or_else(|| failure("job-not-found"))
    }
    pub(super) fn status(
        &self,
        context: ToolContext,
        value: Value,
    ) -> Result<Value, ExtensionError> {
        let owner = Owner::from_context(&context)?;
        Ok(self.lookup(&owner, input::id(&value)?)?.status())
    }
    pub(super) fn list(&self, context: ToolContext, value: Value) -> Result<Value, ExtensionError> {
        let owner = Owner::from_context(&context)?;
        input::list(&value)?;
        let mut jobs: Vec<_> = self
            .registry
            .lock()
            .unwrap()
            .jobs
            .values()
            .filter(|job| job.owner == owner)
            .cloned()
            .collect();
        jobs.sort_by(|a, b| (a.started_at, &a.id).cmp(&(b.started_at, &b.id)));
        Ok(json!({"jobs":jobs.iter().map(|job|job.summary()).collect::<Vec<_>>()}))
    }
    pub(super) async fn kill(
        &self,
        context: ToolContext,
        value: Value,
    ) -> Result<Value, ExtensionError> {
        let owner = Owner::from_context(&context)?;
        let job = self.lookup(&owner, input::id(&value)?)?;
        if job.state() != State::Running {
            return Ok(job.kill_result(false));
        }
        let mut done = job.done.subscribe();
        let mut attempt = job.attempt.subscribe();
        let newly = job.set_cause_once(Cause::Kill);
        let requested = job.request_attempt();
        if job.state() != State::Running {
            return Ok(job.kill_result(newly));
        }
        tokio::select! {
            biased;
            () = context.cancel.cancelled() => return Err(failure("cancelled")),
            _ = done.wait_for(|done| *done) => {},
            _ = attempt.wait_for(|attempt| attempt.completed >= requested) => {},
        }
        if job.state() == State::Running {
            Err(failure("termination-incomplete"))
        } else {
            Ok(job.kill_result(newly))
        }
    }
    pub(super) async fn close(&self, bound: Duration) -> Result<(), ExtensionError> {
        let deadline = Instant::now() + bound;
        {
            let mut registry = self.registry.lock().unwrap();
            if registry.closed {
                return Ok(());
            }
            registry.closing = true;
        }
        let mut starting = self.starting.subscribe();
        timeout_at(deadline, starting.wait_for(|starting| *starting == 0))
            .await
            .map_err(|_| failure("termination-incomplete"))?
            .map_err(|_| failure("termination-incomplete"))?;
        let jobs: Vec<_> = {
            let registry = self.registry.lock().unwrap();
            registry
                .jobs
                .values()
                .chain(registry.hidden.iter())
                .cloned()
                .collect()
        };
        for job in &jobs {
            if job.state() == State::Running {
                job.set_cause_once(Cause::Close);
                job.request_attempt();
            }
        }
        for job in jobs {
            let mut done = job.done.subscribe();
            timeout_at(deadline, done.wait_for(|done| *done))
                .await
                .map_err(|_| failure("termination-incomplete"))?
                .map_err(|_| failure("termination-incomplete"))?;
        }
        let mut registry = self.registry.lock().unwrap();
        registry.jobs.clear();
        registry.hidden.clear();
        registry.closed = true;
        Ok(())
    }
}
