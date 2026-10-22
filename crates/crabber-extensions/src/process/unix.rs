use std::{
    path::Path,
    process::{ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use crabber::extension::CleanupTracker;
use rustix::process::{Pid, Signal, kill_process_group};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::watch,
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;

use super::{Tail, script::SUPERVISOR_SCRIPT};

pub(crate) struct Launch<'a> {
    pub(crate) shell: &'a Path,
    pub(crate) command: &'a str,
    pub(crate) directory: &'a Path,
    pub(crate) environment: &'a [(String, String)],
}

pub(crate) struct RunnerLaunch<'a> {
    pub(crate) program: &'a Path,
    pub(crate) args: &'a [String],
    pub(crate) directory: &'a Path,
    pub(crate) environment: &'a [(String, String)],
}

/// Keeps the leader unreaped until the final group signal anchors the PGID.
pub(crate) struct RunnerChild {
    child: Child,
    pgid: Pid,
    pipes: Option<(ChildStdin, ChildStdout)>,
    phase: Phase,
    term_deadline: Option<Instant>,
    swept: bool,
    #[cfg(target_os = "macos")]
    sweep_unconfirmed: bool,
    #[cfg(target_os = "macos")]
    sweep_deadline: Option<Instant>,
    #[cfg(test)]
    pub(crate) hooks: TestHooks,
}

pub(crate) fn spawn_runner(launch: RunnerLaunch<'_>) -> Result<RunnerChild, Fault> {
    let mut child = Command::new(launch.program)
        .args(launch.args)
        .current_dir(launch.directory)
        .env_clear()
        .envs(launch.environment.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| Fault::Spawn)?;
    let pgid = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(Pid::from_raw)
        .ok_or(Fault::Spawn)?;
    let pipes = Some((
        child.stdin.take().ok_or(Fault::Spawn)?,
        child.stdout.take().ok_or(Fault::Spawn)?,
    ));
    Ok(RunnerChild {
        child,
        pgid,
        pipes,
        phase: Phase::Initial,
        term_deadline: None,
        swept: false,
        #[cfg(target_os = "macos")]
        sweep_unconfirmed: false,
        #[cfg(target_os = "macos")]
        sweep_deadline: None,
        #[cfg(test)]
        hooks: TestHooks::default(),
    })
}

impl RunnerChild {
    pub(crate) fn take_pipes(&mut self) -> Option<(ChildStdin, ChildStdout)> {
        self.pipes.take()
    }

    #[cfg(test)]
    pub(crate) fn pgid(&self) -> Pid {
        self.pgid
    }

    pub(crate) fn leader_exited(&self) -> bool {
        use rustix::process::{WaitId, WaitIdOptions, waitid};
        match waitid(
            WaitId::Pid(self.pgid),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        ) {
            Ok(status) => status.is_some(),
            Err(error) => error == rustix::io::Errno::CHILD,
        }
    }

    fn signal(&mut self, signal: GroupSignal) -> Result<(), SignalFault> {
        #[cfg(target_os = "macos")]
        if self.sweep_unconfirmed {
            return self.probe_group_absence();
        }
        if self.swept {
            return Ok(());
        }
        #[cfg(test)]
        self.hooks.before_signal(signal)?;
        let result = signal_group(self.pgid, signal);
        // macOS only: Linux zombie-only groups do not reject signals. An early
        // reap there would release the PGID anchor before the final group KILL.
        #[cfg(target_os = "macos")]
        if result == Err(SignalFault::Failed) && self.child.try_wait().is_ok_and(|s| s.is_some()) {
            // Darwin excludes zombies from group signals. Reaping the leader
            // releases our PGID anchor, so only read-only absence probes are
            // safe now. Zombie descendants may keep the group present briefly.
            self.swept = true;
            self.sweep_unconfirmed = true;
            return self.probe_group_absence();
        }
        result
    }

    #[cfg(target_os = "macos")]
    fn probe_group_absence(&mut self) -> Result<(), SignalFault> {
        match rustix::process::test_kill_process_group(self.pgid) {
            Err(rustix::io::Errno::SRCH) => {
                self.sweep_unconfirmed = false;
                Err(SignalFault::Gone)
            }
            // Success means present; EPERM does not establish absence.
            _ => Err(SignalFault::Failed),
        }
    }

    async fn signal_for_cleanup(
        &mut self,
        signal: GroupSignal,
        bound: Duration,
    ) -> Result<(), SignalFault> {
        let result = self.signal(signal);
        #[cfg(target_os = "macos")]
        if self.sweep_unconfirmed {
            let deadline = *self.sweep_deadline.get_or_insert(Instant::now() + bound);
            loop {
                let observed = self.probe_group_absence();
                if !self.sweep_unconfirmed || Instant::now() >= deadline {
                    return observed;
                }
                tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(10)))
                    .await;
            }
        }
        #[cfg(not(target_os = "macos"))]
        let _ = bound;
        result
    }

    pub(crate) async fn terminate(&mut self, grace: Duration, kill_wait: Duration) -> Reap {
        self.pipes.take();
        if self.phase == Phase::Initial {
            match self
                .signal_for_cleanup(GroupSignal::Terminate, kill_wait)
                .await
            {
                Ok(()) => {
                    self.phase = Phase::Terminated;
                    self.term_deadline = Some(Instant::now() + grace);
                }
                Err(SignalFault::Gone) => {
                    self.phase = Phase::Killed;
                    self.swept = true;
                }
                Err(SignalFault::Failed) => return Reap::pending(),
            }
        }
        if self.phase == Phase::Terminated {
            let deadline = self.term_deadline.expect("TERM deadline");
            while Instant::now() < deadline && !self.leader_exited() {
                tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(10)))
                    .await;
            }
        }
        if self.phase != Phase::Killed {
            if self
                .signal_for_cleanup(GroupSignal::Kill, kill_wait)
                .await
                .is_err_and(|e| e != SignalFault::Gone)
            {
                return Reap::pending();
            }
            self.phase = Phase::Killed;
            self.swept = true;
        }
        // The leader may have moved to another group. Its unreaped PID is
        // still owned even when the original group is gone.
        let _ = self.child.start_kill();
        #[cfg(test)]
        if self
            .hooks
            .reap_timeout_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Reap::pending();
        }
        match timeout_at(Instant::now() + kill_wait, self.child.wait()).await {
            Ok(Ok(status)) => Reap {
                reaped: true,
                status: Some(status),
                output_forced: false,
            },
            _ => Reap::pending(),
        }
    }
}

impl Drop for RunnerChild {
    fn drop(&mut self) {
        if !self.swept {
            let _ = signal_group(self.pgid, GroupSignal::Kill);
        }
    }
}

#[derive(Clone)]
pub(crate) struct Tails {
    pub(crate) stdout: Arc<Mutex<Tail>>,
    pub(crate) stderr: Arc<Mutex<Tail>>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Fault {
    Spawn,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GroupSignal {
    Terminate,
    Kill,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SignalFault {
    Gone,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Initial,
    Terminated,
    Killed,
}

pub(crate) struct Reap {
    pub(crate) reaped: bool,
    pub(crate) status: Option<ExitStatus>,
    pub(crate) output_forced: bool,
}

pub(crate) struct Spawned {
    group: Group,
    stdin: ChildStdin,
}

pub(crate) struct GateFailure {
    pub(crate) group: Box<Group>,
}
impl std::fmt::Debug for GateFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Gate")
    }
}

impl Spawned {
    #[cfg(test)]
    pub(crate) fn fail_gate_once(&mut self) {
        self.group.hooks.fail_gate_once = true;
    }

    #[cfg(test)]
    pub(crate) async fn exited(&mut self) -> std::io::Result<ExitStatus> {
        self.group.exited().await
    }

    #[cfg(test)]
    pub(crate) fn set_signal_hooks(
        &mut self,
        failure: Arc<std::sync::atomic::AtomicBool>,
        signals: Arc<Mutex<Vec<GroupSignal>>>,
    ) {
        self.group.hooks.fail_kill = failure;
        self.group.hooks.signals = signals;
    }

    #[cfg(test)]
    pub(crate) fn pgid(&self) -> Pid {
        self.group.pgid()
    }

    pub(crate) async fn release_gate(self) -> Result<Group, GateFailure> {
        let Self { group, mut stdin } = self;
        #[cfg(test)]
        let result = if group.hooks.fail_gate_once {
            Err(std::io::ErrorKind::BrokenPipe.into())
        } else {
            stdin.write_all(b"G\n").await
        };
        #[cfg(not(test))]
        let result = stdin.write_all(b"G\n").await;
        drop(stdin);
        match result {
            Ok(()) => Ok(group),
            Err(_) => Err(GateFailure {
                group: Box::new(group),
            }),
        }
    }

    pub(crate) fn withhold_gate(self) -> Group {
        drop(self.stdin);
        self.group
    }
}

pub(crate) fn spawn(
    launch: Launch<'_>,
    tails: Tails,
    tracker: &CleanupTracker,
) -> Result<Spawned, Fault> {
    let mut child = Command::new(launch.shell)
        .args(["-c", SUPERVISOR_SCRIPT, "background-job-supervisor"])
        .arg(launch.shell)
        .arg(launch.command)
        .current_dir(launch.directory)
        .env_clear()
        .envs(launch.environment.iter().map(|(key, value)| (key, value)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| Fault::Spawn)?;
    let pgid = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(Pid::from_raw)
        .ok_or(Fault::Spawn)?;
    let stdin = child.stdin.take().ok_or(Fault::Spawn)?;
    let stdout = child.stdout.take().ok_or(Fault::Spawn)?;
    let stderr = child.stderr.take().ok_or(Fault::Spawn)?;
    let pumps = Pumps {
        stdout: start_pump(stdout, tails.stdout, tracker),
        stderr: start_pump(stderr, tails.stderr, tracker),
    };
    Ok(Spawned {
        group: Group {
            child,
            pgid,
            pumps,
            phase: Phase::Initial,
            term_deadline: None,
            swept: false,
            #[cfg(test)]
            hooks: TestHooks::default(),
        },
        stdin,
    })
}

pub(crate) struct Group {
    child: Child,
    pgid: Pid,
    pumps: Pumps,
    phase: Phase,
    term_deadline: Option<Instant>,
    swept: bool,
    #[cfg(test)]
    pub(super) hooks: TestHooks,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestHooks {
    pub(crate) signals: Arc<Mutex<Vec<GroupSignal>>>,
    pub(crate) fail_kill: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) fail_kill_once: std::sync::atomic::AtomicBool,
    pub(super) fail_gate_once: bool,
    pub(crate) reap_timeout_once: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl TestHooks {
    fn before_signal(&self, signal: GroupSignal) -> Result<(), SignalFault> {
        use std::sync::atomic::Ordering;
        self.signals.lock().unwrap().push(signal);
        if signal == GroupSignal::Kill
            && (self.fail_kill.load(Ordering::SeqCst)
                || self.fail_kill_once.swap(false, Ordering::SeqCst))
        {
            return Err(SignalFault::Failed);
        }
        Ok(())
    }
}

pub(crate) fn signal_group(pgid: Pid, signal: GroupSignal) -> Result<(), SignalFault> {
    let signal = match signal {
        GroupSignal::Terminate => Signal::TERM,
        GroupSignal::Kill => Signal::KILL,
    };
    kill_process_group(pgid, signal).map_err(|error| {
        if error == rustix::io::Errno::SRCH {
            SignalFault::Gone
        } else {
            SignalFault::Failed
        }
    })
}

impl Group {
    #[cfg(test)]
    pub(crate) fn pgid(&self) -> Pid {
        self.pgid
    }

    pub(crate) async fn exited(&mut self) -> std::io::Result<ExitStatus> {
        self.child.wait().await
    }

    pub(crate) fn signal(&mut self, signal: GroupSignal) -> Result<(), SignalFault> {
        #[cfg(test)]
        self.hooks.before_signal(signal)?;
        let result = signal_group(self.pgid, signal);
        if result == Err(SignalFault::Failed)
            && self.child.try_wait().is_ok_and(|status| status.is_some())
        {
            // Darwin can return EPERM for a group containing only a zombie.
            // Reap our owned leader, then retry: ESRCH is authoritative absence;
            // a real permission failure remains Failed. Tokio caches the status.
            return signal_group(self.pgid, signal);
        }
        result
    }

    pub(crate) async fn terminate(
        &mut self,
        first: GroupSignal,
        grace: Duration,
        kill_wait: Duration,
    ) -> Reap {
        let mut status = None;
        if self.phase == Phase::Initial && first == GroupSignal::Terminate {
            match self.signal(GroupSignal::Terminate) {
                Ok(()) => {
                    self.phase = Phase::Terminated;
                    self.term_deadline = Some(Instant::now() + grace);
                }
                Err(SignalFault::Gone) => {
                    self.phase = Phase::Killed;
                    self.swept = true;
                }
                Err(SignalFault::Failed) => return Reap::pending(),
            }
        }
        if self.phase == Phase::Terminated {
            // Persist the deadline across dropped futures and failed signals.
            let deadline = self.term_deadline.expect("TERM phase has a deadline");
            if let Ok(Ok(exit)) = timeout_at(deadline, self.child.wait()).await {
                status = Some(exit);
            }
        }
        if self.phase != Phase::Killed {
            if self
                .signal(GroupSignal::Kill)
                .is_err_and(|fault| fault != SignalFault::Gone)
            {
                return Reap::pending();
            }
            self.phase = Phase::Killed;
            self.swept = true;
        }
        #[cfg(test)]
        if self
            .hooks
            .reap_timeout_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Reap {
                reaped: false,
                status,
                output_forced: false,
            };
        }
        let deadline = Instant::now() + kill_wait;
        match timeout_at(deadline, self.child.wait()).await {
            Ok(Ok(exit)) => {
                status = Some(exit);
                let output_forced = self.pumps.settle(deadline).await;
                Reap {
                    reaped: true,
                    status,
                    output_forced,
                }
            }
            _ => Reap {
                reaped: false,
                status,
                output_forced: false,
            },
        }
    }

    pub(crate) async fn sweep_and_reap(&mut self, status: ExitStatus, kill_wait: Duration) -> Reap {
        // Keep ownership and the final-signal phase armed after a failed sweep.
        if self
            .signal(GroupSignal::Kill)
            .is_err_and(|fault| fault != SignalFault::Gone)
        {
            return Reap {
                reaped: false,
                status: Some(status),
                output_forced: false,
            };
        }
        self.swept = true;
        self.phase = Phase::Killed;
        let output_forced = self.pumps.settle(Instant::now() + kill_wait).await;
        Reap {
            reaped: true,
            status: Some(status),
            output_forced,
        }
    }
}

impl Reap {
    fn pending() -> Self {
        Self {
            reaped: false,
            status: None,
            output_forced: false,
        }
    }
}

impl Drop for Group {
    fn drop(&mut self) {
        if !self.swept {
            let _ = signal_group(self.pgid, GroupSignal::Kill);
        }
        self.pumps.stdout.abandon.cancel();
        self.pumps.stderr.abandon.cancel();
    }
}

struct Pumps {
    stdout: PumpHandle,
    stderr: PumpHandle,
}
struct PumpHandle {
    abandon: CancellationToken,
    done: watch::Receiver<bool>,
    tail: Arc<Mutex<Tail>>,
}

impl Pumps {
    async fn settle(&mut self, deadline: Instant) -> bool {
        let completed = timeout_at(deadline, async {
            let _ = self.stdout.done.wait_for(|done| *done).await;
            let _ = self.stderr.done.wait_for(|done| *done).await;
        })
        .await
        .is_ok();
        if !completed {
            // Publish truncation before returning, without waiting for the
            // abandoned readers to be scheduled.
            self.stdout.tail.lock().unwrap().mark_truncated();
            self.stderr.tail.lock().unwrap().mark_truncated();
            self.stdout.abandon.cancel();
            self.stderr.abandon.cancel();
        }
        !completed
    }
}

fn start_pump(
    reader: impl AsyncRead + Unpin + Send + 'static,
    tail: Arc<Mutex<Tail>>,
    tracker: &CleanupTracker,
) -> PumpHandle {
    let abandon = CancellationToken::new();
    let (done_tx, done) = watch::channel(false);
    let handle = PumpHandle {
        abandon: abandon.clone(),
        done,
        tail: tail.clone(),
    };
    tracker.spawn(async move {
        pump(reader, tail, abandon).await;
        let _ = done_tx.send(true);
    });
    handle
}

pub(super) async fn pump(
    mut reader: impl AsyncRead + Unpin,
    tail: Arc<Mutex<Tail>>,
    abandon: CancellationToken,
) {
    let mut bytes = [0; 8192];
    loop {
        tokio::select! {
            biased;
            () = abandon.cancelled() => { tail.lock().unwrap().mark_truncated(); return; }
            result = reader.read(&mut bytes) => match result {
                Ok(0) => return,
                Ok(count) => tail.lock().unwrap().write(&bytes[..count]),
                Err(_) => { tail.lock().unwrap().mark_truncated(); return; }
            }
        }
    }
}
