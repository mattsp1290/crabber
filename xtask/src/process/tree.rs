//! Linux subreaper support for commands whose helpers may create new sessions.
//! PID start times prevent signalling a reused PID. Only this runner's children
//! and descendants are considered; no command-name or machine-wide killing.

use std::{
    collections::BTreeMap,
    fs, io,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use nix::{
    errno::Errno,
    sys::{
        signal::{Signal, kill},
        wait::{WaitPidFlag, waitpid},
    },
    unistd::Pid,
};

static ENABLED: AtomicBool = AtomicBool::new(false);

pub(crate) fn enable() -> io::Result<()> {
    nix::sys::prctl::set_child_subreaper(true).map_err(io::Error::other)?;
    ENABLED.store(true, Ordering::SeqCst);
    Ok(())
}

pub(super) struct Tree {
    owned: BTreeMap<i32, u64>,
}
impl Tree {
    pub(super) fn new() -> Option<Self> {
        ENABLED.load(Ordering::SeqCst).then(|| Self {
            owned: BTreeMap::new(),
        })
    }
    pub(super) fn scan(&mut self) -> io::Result<()> {
        // Orphaned grandchildren are now direct children of this subreaper,
        // including helpers which started a separate Unix process group.
        let root = i32::try_from(std::process::id()).map_err(io::Error::other)?;
        let mut pending = vec![root];
        let mut seen = std::collections::BTreeSet::new();
        while let Some(parent) = pending.pop() {
            if !seen.insert(parent) {
                continue;
            }
            let tasks = match fs::read_dir(format!("/proc/{parent}/task")) {
                Ok(tasks) => tasks,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            for task in tasks {
                let path = task?.path().join("children");
                let children = match fs::read_to_string(path) {
                    Ok(children) => children,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error),
                };
                for child in children.split_whitespace() {
                    let pid = child.parse::<i32>().map_err(io::Error::other)?;
                    if let Some((start, _)) = identity(pid)? {
                        self.owned.insert(pid, start);
                        pending.push(pid);
                    }
                }
            }
        }
        Ok(())
    }
    fn signal(&mut self, signal: Signal) -> io::Result<()> {
        self.scan()?;
        for (&pid, &start) in &self.owned {
            if identity(pid)?.is_some_and(|(current, _)| current == start) {
                match kill(Pid::from_raw(pid), signal) {
                    Ok(()) | Err(Errno::ESRCH) => {}
                    Err(error) => return Err(io::Error::other(error)),
                }
            }
        }
        Ok(())
    }
    pub(super) fn terminate(&mut self) -> io::Result<()> {
        self.signal(Signal::SIGTERM)?;
        std::thread::sleep(Duration::from_millis(200));
        self.signal(Signal::SIGKILL)
    }
    fn reap(&mut self) -> io::Result<bool> {
        self.scan()?;
        let mut active = false;
        for (&pid, &start) in &self.owned {
            if let Some((current, state)) = identity(pid)? {
                if current != start {
                    continue;
                }
                if state == "Z" {
                    match waitpid(Pid::from_raw(pid), Some(WaitPidFlag::WNOHANG)) {
                        Ok(_) | Err(Errno::ECHILD | Errno::ESRCH) => {}
                        Err(error) => return Err(io::Error::other(error)),
                    }
                } else {
                    active = true;
                }
            }
        }
        Ok(active)
    }
    /// Called after the main Child has been reaped. A surviving helper is a gate
    /// failure even if cleanup can successfully terminate it.
    pub(super) fn finish(&mut self) -> io::Result<()> {
        if !self.reap()? {
            return Ok(());
        }
        self.terminate()?;
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.reap()? {
            if Instant::now() >= deadline {
                return Err(io::Error::other(
                    "owned child processes did not stop after SIGKILL",
                ));
            }
            self.signal(Signal::SIGKILL)?;
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(io::Error::other(
            "command left a running descendant; owned processes were terminated and reaped",
        ))
    }
}

fn identity(pid: i32) -> io::Result<Option<(u64, String)>> {
    let record = match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let (_, fields) = record
        .rsplit_once(") ")
        .ok_or_else(|| io::Error::other("malformed process identity"))?;
    let fields: Vec<_> = fields.split_whitespace().collect();
    let start = fields
        .get(19)
        .ok_or_else(|| io::Error::other("missing process start time"))?
        .parse::<u64>()
        .map_err(io::Error::other)?;
    Ok(Some((start, fields[0].to_owned())))
}

#[cfg(feature = "postgres-check")]
pub(crate) fn verify_drained() -> io::Result<()> {
    let mut tree =
        Tree::new().ok_or_else(|| io::Error::other("process supervision is not enabled"))?;
    tree.finish()
}
