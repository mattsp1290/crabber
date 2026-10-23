//! Explicit process policies and nonblocking process-group cleanup on future drop.
use crabber::ExtensionError;
use rustix::process::{Pid, Signal, kill_process_group};
use serde::Serialize;
use std::{
    io,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};
use tokio::{
    process::{Child, ChildStderr, ChildStdout, Command},
    time::{Instant, timeout_at},
};

/// Maximum reap and pipe-EOF grace after killing or leader exit.
pub const WAIT_AFTER_KILL: Duration = Duration::from_secs(5);
/// Host-owned child environment. No implicit inheritance default.
#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub enum EnvPolicy {
    /// Copy the host environment verbatim.
    Inherit,
    /// Clear environment and set only these entries.
    Replace(Vec<(String, String)>),
    /// Clear environment, retain named host variables, then apply overrides.
    Allowlist {
        /// Host variable names to retain.
        keep: Vec<String>,
        /// Explicit overrides.
        set: Vec<(String, String)>,
    },
}
impl EnvPolicy {
    /// Retain PATH, HOME, LANG and TERM; secrets remain excluded.
    pub fn minimal_allowlist() -> Self {
        Self::Allowlist {
            keep: ["PATH", "HOME", "LANG", "TERM"].map(String::from).into(),
            set: vec![],
        }
    }
    /// Reject NULs, empty names, or equals signs in variable names before spawn.
    ///
    /// # Errors
    /// Returns a plan error for invalid environment variable names or NUL-bearing values.
    pub fn validate(&self) -> Result<(), ExtensionError> {
        let key_ok = |k: &str| !k.is_empty() && !k.contains(['\0', '=']);
        let valid = match self {
            Self::Inherit => true,
            Self::Replace(set) => set.iter().all(|(k, v)| key_ok(k) && !v.contains('\0')),
            Self::Allowlist { keep, set } => {
                keep.iter().all(|k| key_ok(k))
                    && set.iter().all(|(k, v)| key_ok(k) && !v.contains('\0'))
            }
        };
        if valid {
            Ok(())
        } else {
            Err(ExtensionError::Plan("environment".into()))
        }
    }
}
/// Optional child uid/gid; the host must have permission to change identity.
#[derive(Clone, Copy, Serialize, Debug, PartialEq, Eq)]
pub struct RunAs {
    /// Effective child user id.
    pub uid: u32,
    /// Effective child group id.
    pub gid: u32,
}
/// Apply environment and identity; always create a group, null stdin and kill on drop.
pub fn apply_policy(cmd: &mut Command, env: &EnvPolicy, run_as: Option<RunAs>) {
    match env {
        EnvPolicy::Inherit => {}
        EnvPolicy::Replace(set) => {
            cmd.env_clear().envs(set.iter().cloned());
        }
        EnvPolicy::Allowlist { keep, set } => {
            cmd.env_clear();
            for key in keep {
                if let Some(value) = std::env::var_os(key) {
                    cmd.env(key, value);
                }
            }
            cmd.envs(set.iter().cloned());
        }
    }
    if let Some(identity) = run_as {
        cmd.uid(identity.uid).gid(identity.gid);
    }
    cmd.process_group(0).kill_on_drop(true).stdin(Stdio::null());
}
/// Resolve an executable on the host PATH once. Does not consult the child environment.
///
/// # Errors
/// Returns a plan error if PATH contains no valid executable or canonicalization fails.
pub fn resolve_binary(name: &str) -> Result<PathBuf, ExtensionError> {
    for dir in std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()) {
        let path = dir.join(name);
        if validate_binary(&path).is_ok() {
            return path
                .canonicalize()
                .map_err(|_| ExtensionError::Plan("binary".into()));
        }
    }
    Err(ExtensionError::Plan(format!("{name} executable required")))
}
/// Construction-time check for an absolute executable regular file.
///
/// # Errors
/// Returns a plan error unless the path is an absolute, executable regular file.
pub fn validate_binary(path: &Path) -> Result<(), ExtensionError> {
    use std::os::unix::fs::PermissionsExt;
    if path.is_absolute()
        && path
            .metadata()
            .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    {
        Ok(())
    } else {
        Err(ExtensionError::Plan(
            "binary must be an absolute executable file".into(),
        ))
    }
}
/// Result of a bounded child wait.
pub enum WaitOutcome {
    /// The leader was reaped.
    Exited(ExitStatus),
    /// The deadline expired; group was killed and bounded reaping attempted.
    TimedOut,
    /// Waiting for the leader failed.
    Failed(io::Error),
}
/// Owns a process group until explicitly finished. Drop signals even after the
/// leader exits, so cancellation during pipe draining still kills descendants.
pub struct ProcessGroupGuard {
    child: Child,
    pgid: Pid,
    armed: bool,
}
impl ProcessGroupGuard {
    /// Spawn a command prepared by `apply_policy`, owning cleanup immediately.
    ///
    /// # Errors
    /// Returns an I/O error if spawning fails or the child process ID is unavailable.
    #[allow(
        clippy::cast_possible_wrap,
        reason = "OS process IDs are positive pid_t values bounded by i32::MAX."
    )]
    pub fn spawn(cmd: &mut Command) -> io::Result<Self> {
        let child = cmd.spawn()?;
        let pgid = child
            .id()
            .and_then(|id| Pid::from_raw(id as i32))
            .ok_or_else(|| io::Error::other("child id unavailable"))?;
        Ok(Self {
            child,
            pgid,
            armed: true,
        })
    }
    pub(crate) fn needs_cleanup(&self) -> bool {
        self.armed
    }
    /// The group's leader id, useful for lifecycle verification.
    #[must_use]
    #[allow(
        clippy::cast_sign_loss,
        reason = "OS process IDs are positive pid_t values bounded by i32::MAX."
    )]
    pub fn pid(&self) -> u32 {
        self.pgid.as_raw_nonzero().get() as u32
    }
    /// Take ownership of piped stdout.
    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }
    /// Take ownership of piped stderr.
    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr.take()
    }
    /// Signal every member of the child's process group. Safe to repeat.
    pub fn kill_group(&mut self) {
        let _ = kill_process_group(self.pgid, Signal::KILL);
    }
    /// Disarm cleanup once the leader is reaped and both pipes reached EOF.
    /// Descendants that detach or close their pipes are a host concern.
    pub fn finish(&mut self) {
        self.armed = false;
    }
    /// Wait for the leader without changing pipe ownership.
    ///
    /// # Errors
    /// Returns the operating system error if waiting for the child fails.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child.wait().await
    }
    /// Bound the wait; timeout signals the group and attempts reaping for five seconds.
    pub async fn wait_with_deadline(&mut self, deadline: Instant) -> WaitOutcome {
        match timeout_at(deadline, self.child.wait()).await {
            Ok(Ok(status)) => WaitOutcome::Exited(status),
            Ok(Err(error)) => WaitOutcome::Failed(error),
            Err(_) => {
                self.kill_group();
                let _ = tokio::time::timeout(WAIT_AFTER_KILL, self.child.wait()).await;
                WaitOutcome::TimedOut
            }
        }
    }
}
impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        if self.armed {
            self.kill_group();
        }
    }
}
