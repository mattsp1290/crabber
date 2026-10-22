use super::{
    ClearResult, ExecuteResult,
    config::Configuration,
    failure,
    runner::{self, ExecuteOutcome, Runner, StartFault},
    runtime_error,
};
use crabber::extension::{CleanupOwner, ExtensionError, ToolContext};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
#[cfg(unix)]
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hasher},
};
use tokio::{
    sync::{Mutex as AsyncMutex, oneshot},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

pub(super) use super::session::OwnerKey;
use super::session::{OwnerSession, ResetReason, Slot};

pub(super) struct Manager {
    pub(super) binding: Option<crate::contextual::Binding>,
    pub(super) shared: Option<super::SharedCapacity>,
    pub(super) configuration: Configuration,
    pub(super) cleanup: CleanupOwner,
    pub(super) state: Mutex<State>,
    #[cfg(test)]
    pub(super) hooks: Mutex<TestHooks>,
}
pub(super) struct State {
    pub(super) owners: BTreeMap<OwnerKey, Arc<OwnerSession>>,
    #[cfg(unix)]
    counter: u64,
    #[cfg(unix)]
    epoch: String,
    pub(super) closing: bool,
    pub(super) closed: bool,
}
#[cfg(test)]
pub(super) type Hook =
    Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;
#[cfg(test)]
#[derive(Default)]
pub(super) struct TestHooks {
    pub(super) before_request_write: Option<Hook>,
    pub(super) after_request_write: Option<Hook>,
    pub(super) before_commit: Option<Hook>,
    pub(super) before_send: Option<Hook>,
    pub(super) after_send: Option<Hook>,
    pub(super) before_directory_remove: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(super) before_reset: Option<Hook>,
    #[cfg(unix)]
    pub(super) fail_kill: Arc<AtomicBool>,
    #[cfg(unix)]
    pub(super) reap_timeout_once: bool,
    #[cfg(unix)]
    pub(super) pgids: Vec<i32>,
}

impl Manager {
    pub(super) fn new(configuration: Configuration) -> Self {
        #[cfg(unix)]
        let epoch = (0..2)
            .map(|_| {
                let mut h = RandomState::new().build_hasher();
                h.write(b"python-repl-epoch");
                format!("{:016x}", h.finish())
            })
            .collect();
        Self {
            binding: None,
            shared: None,
            configuration,
            cleanup: CleanupOwner::new(),
            state: Mutex::new(State {
                owners: BTreeMap::new(),
                #[cfg(unix)]
                counter: 0,
                #[cfg(unix)]
                epoch,
                closing: false,
                closed: false,
            }),
            #[cfg(test)]
            hooks: Mutex::new(TestHooks::default()),
        }
    }
    pub(super) fn owner_report(&self) -> super::OwnerReport {
        let state = self.state.lock().unwrap();
        super::OwnerReport {
            live: state
                .owners
                .values()
                .filter(|owner| owner.live.load(Ordering::SeqCst))
                .count(),
            tracked: state.owners.len(),
            closing: state.closing,
            closed: state.closed && self.cleanup.tracker().pending() == 0,
            ..Default::default()
        }
    }
    pub(super) fn live_runners(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .owners
            .values()
            .filter(|o| o.live.load(Ordering::SeqCst))
            .count()
    }
    fn check_open(&self, cancel: &CancellationToken) -> Result<(), ExtensionError> {
        let state = self.state.lock().unwrap();
        if state.closing || state.closed {
            return Err(failure("manager-closing"));
        }
        if cancel.is_cancelled() {
            return Err(failure("cancelled"));
        }
        Ok(())
    }
    pub(super) fn context(context: &ToolContext) -> Result<(OwnerKey, PathBuf), ExtensionError> {
        let key = OwnerKey {
            session: context.session_id.clone(),
            workspace: context
                .workspace()
                .workspace_id()
                .ok_or_else(|| runtime_error("owner"))?
                .into(),
        };
        let root = std::path::Path::new(
            context
                .workspace()
                .directory()
                .ok_or_else(|| runtime_error("workspace-root"))?,
        )
        .canonicalize()
        .map_err(|_| runtime_error("workspace-root"))?;
        if !root.is_dir() {
            return Err(runtime_error("workspace-root"));
        }
        Ok((key, root))
    }
    fn owner(
        &self,
        key: OwnerKey,
        root: PathBuf,
        cancel: &CancellationToken,
        admit: bool,
    ) -> Result<Option<Arc<OwnerSession>>, ExtensionError> {
        let mut state = self.state.lock().unwrap();
        if state.closing || state.closed {
            return Err(failure("manager-closing"));
        }
        if cancel.is_cancelled() {
            return Err(failure("cancelled"));
        }
        if let Some(owner) = state.owners.get(&key) {
            if owner.root != root {
                return Err(runtime_error("workspace-root-mismatch"));
            }
            return Ok(Some(owner.clone()));
        }
        if !admit {
            return Ok(None);
        }
        if state.owners.len() >= self.configuration.limits.max_sessions {
            return Err(failure("capacity-exhausted"));
        }
        let owner = Arc::new(OwnerSession {
            root,
            gate: AsyncMutex::new(Slot::default()),
            queue: Mutex::new(0),
            lifecycle: CancellationToken::new(),
            live: AtomicBool::new(false),
        });
        state.owners.insert(key, owner.clone());
        Ok(Some(owner))
    }
    pub(super) async fn execute(
        self: &Arc<Self>,
        context: ToolContext,
        code: String,
        timeout: Duration,
    ) -> Result<ExecuteResult, ExtensionError> {
        self.check_open(&context.cancel)?;
        if self
            .binding
            .as_ref()
            .is_some_and(|binding| !binding.matches(&context))
        {
            return Err(runtime_error("owner"));
        }
        let (key, root) = Self::context(&context)?;
        self.execute_owner(key, root, context.cancel, code, timeout)
            .await
    }
    pub(super) async fn execute_owner(
        self: &Arc<Self>,
        key: OwnerKey,
        root: PathBuf,
        cancel: CancellationToken,
        code: String,
        timeout: Duration,
    ) -> Result<ExecuteResult, ExtensionError> {
        let owner = self
            .owner(key, root, &cancel, true)?
            .expect("admitted owner");
        let (mut sender, receiver) = oneshot::channel();
        let (ack, acked) = oneshot::channel();
        let manager = self.clone();
        let task_cancel = cancel.clone();
        self.cleanup.tracker().spawn(async move {
            let acquired = tokio::select! { biased;
                () = sender.closed() => return,
                result = owner.acquire(
                    &task_cancel,
                    manager.configuration.limits.max_queued_per_session,
                ) => result,
            };
            let mut slot = match acquired {
                Ok(slot) => slot,
                Err(error) => {
                    let _ = sender.send(Err(error));
                    return;
                }
            };
            let result = manager
                .run(&owner, &mut slot, &task_cancel, &code, timeout)
                .await;
            #[cfg(test)]
            manager.hook(|h| h.before_send.clone()).await;
            let committed = result.is_ok();
            let sent = sender.send(result).is_ok();
            #[cfg(test)]
            manager.hook(|h| h.after_send.clone()).await;
            let delivered = sent
                && (!committed
                    || tokio::select! { biased;
                        result = acked => result.is_ok(),
                        // Close owns the reset if the caller remains unpolled.
                        () = owner.lifecycle.cancelled() => true,
                    });
            if !delivered && committed {
                let _ = manager
                    .reset(&owner, &mut slot, Some(ResetReason::Canceled))
                    .await;
            }
        });
        tokio::select! { biased;
            () = cancel.cancelled() => Err(failure("cancelled")),
            result = receiver => {
                let result = result.map_err(|_| failure("runner-failed"))?;
                let _ = ack.send(());
                result
            },
        }
    }
    async fn run(
        &self,
        owner: &OwnerSession,
        slot: &mut Slot,
        cancel: &CancellationToken,
        code: &str,
        timeout: Duration,
    ) -> Result<ExecuteResult, ExtensionError> {
        if slot.quarantined {
            return Err(failure("owner-quarantined"));
        }
        if slot.closed || owner.lifecycle.is_cancelled() {
            return Err(failure("manager-closing"));
        }
        if cancel.is_cancelled() {
            return Err(failure("cancelled"));
        }
        let started_now = slot.runner.is_none();
        let shared_permit = if started_now {
            self.shared
                .as_ref()
                .map(|pool| pool.acquire())
                .transpose()
                .map_err(|_| failure("capacity-exhausted"))?
        } else {
            None
        };
        if slot.dirs.is_none() {
            self.private_dirs(slot)?;
        }
        if started_now {
            super::private_dirs::prepare(slot.dirs.as_ref().unwrap())?;
            let environment = self.environment(slot.dirs.as_ref().unwrap())?;
            let b = self.configuration.bounds;
            let args = runner::arguments(runner::RUNNER_SOURCE, b);
            slot.shared_permit = shared_permit;
            owner.live.store(true, Ordering::SeqCst);
            let l = &self.configuration.limits;
            let result = Runner::start(
                crate::process::RunnerLaunch {
                    program: &self.configuration.python,
                    args: &args,
                    directory: &owner.root,
                    environment: &environment,
                },
                b,
                cancel,
                &owner.lifecycle,
                Instant::now() + l.runner_start_timeout,
                l.terminate_grace,
                l.kill_wait,
            )
            .await;
            match result {
                Ok(runner) => {
                    #[cfg(all(test, unix))]
                    {
                        let mut runner = runner;
                        let mut hooks = self.hooks.lock().unwrap();
                        runner.child.hooks.fail_kill = hooks.fail_kill.clone();
                        runner
                            .child
                            .hooks
                            .reap_timeout_once
                            .store(hooks.reap_timeout_once, Ordering::SeqCst);
                        hooks.reap_timeout_once = false;
                        hooks.pgids.push(runner.child.pgid().as_raw_nonzero().get());
                        slot.runner = Some(runner);
                    }
                    #[cfg(not(all(test, unix)))]
                    {
                        slot.runner = Some(runner);
                    }
                }
                Err(error) => {
                    slot.runner = error.runner.map(|r| *r);
                    slot.quarantined = slot.runner.is_some();
                    owner.live.store(slot.quarantined, Ordering::SeqCst);
                    if slot.quarantined {
                        return Err(failure("cleanup-incomplete"));
                    }
                    slot.shared_permit.take();
                    return Err(failure(match error.fault {
                        StartFault::Spawn => "runner-start",
                        StartFault::Timeout => "runner-start-timeout",
                        StartFault::Readiness => "runner-readiness",
                        StartFault::Bootstrap => "bootstrap-integrity",
                        StartFault::Cancelled => "cancelled",
                        StartFault::Closing => "manager-closing",
                    }));
                }
            }
        }
        #[cfg(test)]
        self.hook(|h| h.before_request_write.clone()).await;
        #[cfg(test)]
        {
            let hooks = self.hooks.lock().unwrap();
            let runner = slot.runner.as_mut().unwrap();
            runner.after_request_write = hooks.after_request_write.clone();
            runner.before_commit = hooks.before_commit.clone();
        }
        let outcome = slot
            .runner
            .as_mut()
            .unwrap()
            .execute(code, cancel, &owner.lifecycle, Instant::now() + timeout)
            .await;

        match outcome {
            ExecuteOutcome::Completed(response) => {
                let reason = slot.pending_reason.take();
                Ok(ExecuteResult {
                    status: response.status,
                    stdout: response.stdout,
                    stderr: response.stderr,
                    result: response.result,
                    exception: response.exception,
                    generation: slot.generation,
                    state_reset: reason.is_some(),
                    state_reset_reason: reason.map_or("", ResetReason::code).into(),
                })
            }
            other => {
                // Generation/reset policy intentionally ignores whether the request ran:
                // losing a pre-existing interpreter always reports its lost state.
                let (reason, code, _may_have_executed) = match other {
                    ExecuteOutcome::TimedOut { may_have_executed } => {
                        (Some(ResetReason::TimedOut), "timed-out", may_have_executed)
                    }
                    ExecuteOutcome::Interrupted { may_have_executed } => {
                        (Some(ResetReason::Canceled), "cancelled", may_have_executed)
                    }
                    ExecuteOutcome::Failed { may_have_executed } => (
                        Some(ResetReason::RunnerFailed),
                        "runner-failed",
                        may_have_executed,
                    ),
                    ExecuteOutcome::Closing { may_have_executed } => {
                        (None, "manager-closing", may_have_executed)
                    }
                    ExecuteOutcome::Completed(_) => unreachable!(),
                };
                self.reset(owner, slot, if started_now { None } else { reason })
                    .await?;
                Err(failure(code))
            }
        }
    }
    pub(super) async fn reset(
        &self,
        owner: &OwnerSession,
        slot: &mut Slot,
        reason: Option<ResetReason>,
    ) -> Result<(), ExtensionError> {
        #[cfg(test)]
        self.hook(|h| h.before_reset.clone()).await;
        let l = &self.configuration.limits;
        if let Some(runner) = slot.runner.as_mut() {
            if !runner
                .terminate(l.terminate_grace, l.kill_wait)
                .await
                .reaped
            {
                slot.quarantined = true;
                return Err(failure("cleanup-incomplete"));
            }
            slot.runner = None;
            slot.shared_permit.take();
            owner.live.store(false, Ordering::SeqCst);
            if let Some(reason) = reason {
                slot.generation = slot.generation.saturating_add(1);
                slot.pending_reason = Some(reason);
            }
        }
        Ok(())
    }
    pub(super) async fn clear(
        self: &Arc<Self>,
        context: ToolContext,
    ) -> Result<ClearResult, ExtensionError> {
        self.check_open(&context.cancel)?;
        if self
            .binding
            .as_ref()
            .is_some_and(|binding| !binding.matches(&context))
        {
            return Err(runtime_error("owner"));
        }
        let (key, root) = Self::context(&context)?;
        self.clear_owner(key, root, context.cancel).await
    }
    pub(super) async fn clear_owner(
        self: &Arc<Self>,
        key: OwnerKey,
        root: PathBuf,
        cancel: CancellationToken,
    ) -> Result<ClearResult, ExtensionError> {
        let Some(owner) = self.owner(key, root, &cancel, false)? else {
            return Ok(ClearResult {
                had_state: false,
                generation: 0,
            });
        };
        let (sender, receiver) = oneshot::channel();
        let manager = self.clone();
        let task_cancel = cancel.clone();
        self.cleanup.tracker().spawn(async move {
            let result = async {
                let mut slot = owner
                    .acquire(
                        &task_cancel,
                        manager.configuration.limits.max_queued_per_session,
                    )
                    .await?;
                if slot.quarantined {
                    return Err(failure("owner-quarantined"));
                }
                if slot.closed || owner.lifecycle.is_cancelled() {
                    return Err(failure("manager-closing"));
                }
                slot.pending_reason = None;
                let had_state = slot.runner.is_some();
                manager
                    .reset(&owner, &mut slot, Some(ResetReason::Cleared))
                    .await?;
                Ok(ClearResult {
                    had_state,
                    generation: slot.generation,
                })
            }
            .await;
            let _ = sender.send(result);
        });
        tokio::select! { biased; () = cancel.cancelled() => Err(failure("cancelled")), result = receiver => result.map_err(|_| failure("runner-failed"))? }
    }
    fn environment(&self, dirs: &std::path::Path) -> Result<Vec<(String, String)>, ExtensionError> {
        super::private_dirs::environment(&self.configuration.environment, dirs)
    }
    #[cfg(unix)]
    fn private_dirs(&self, slot: &mut Slot) -> Result<(), ExtensionError> {
        for _ in 0..8 {
            let root = {
                let mut state = self.state.lock().unwrap();
                state.counter = state
                    .counter
                    .checked_add(1)
                    .ok_or_else(|| failure("private-dirs"))?;
                self.configuration
                    .temp_root
                    .join(format!(".python-repl-{}-{}", state.epoch, state.counter))
            };
            match super::private_dirs::create_root(&root) {
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err(failure("private-dirs")),
                Ok(()) => {
                    slot.dirs = Some(root);
                    return Ok(());
                }
            }
        }
        Err(failure("private-dirs"))
    }
    #[cfg(not(unix))]
    fn private_dirs(&self, _: &mut Slot) -> Result<(), ExtensionError> {
        Err(failure("private-dirs"))
    }
    #[cfg(test)]
    async fn hook(&self, choose: impl FnOnce(&TestHooks) -> Option<Hook>) {
        let hook = choose(&self.hooks.lock().unwrap());
        if let Some(hook) = hook {
            hook().await;
        }
    }
}

#[cfg(all(test, unix))]
impl Manager {
    pub(super) fn queued(&self, key: &OwnerKey) -> usize {
        self.state
            .lock()
            .unwrap()
            .owners
            .get(key)
            .map_or(0, |o| *o.queue.lock().unwrap())
    }
}
