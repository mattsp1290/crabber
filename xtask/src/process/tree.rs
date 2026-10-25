//! Linux subreaper support for commands whose helpers may create new sessions.
//! PID start times prevent signalling a reused PID. Only this runner's children
//! and descendants are considered; no command-name or machine-wide killing.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    process::{Child, Command},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
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
static MANAGED: Mutex<BTreeMap<u32, usize>> = Mutex::new(BTreeMap::new());

pub(super) struct ManagedChild(u32);

impl ManagedChild {
    pub(super) fn spawn(command: &mut Command) -> io::Result<(Child, Self)> {
        // Registration and orphan reaping share the lock so a fast child cannot
        // be mistaken for an adopted zombie between spawn and registration.
        let mut managed = MANAGED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let child = command.spawn()?;
        // A reaped Child can outlive PID reuse while its output readers drain.
        *managed.entry(child.id()).or_default() += 1;
        let registration = Self(child.id());
        Ok((child, registration))
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        let mut managed = MANAGED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = managed.get_mut(&self.0).unwrap();
        *count -= 1;
        if *count == 0 {
            managed.remove(&self.0);
        }
    }
}

pub(crate) fn enable() -> io::Result<()> {
    nix::sys::prctl::set_child_subreaper(true).map_err(io::Error::other)?;
    ENABLED.store(true, Ordering::SeqCst);
    Ok(())
}

pub(super) struct Tree {
    owned: BTreeMap<i32, u64>,
    leader: Option<u32>,
}
impl Tree {
    pub(super) fn new(leader: Option<u32>) -> Option<Self> {
        ENABLED.load(Ordering::SeqCst).then(|| Self {
            owned: BTreeMap::new(),
            leader,
        })
    }
    pub(super) fn scan(&mut self) -> io::Result<()> {
        // Orphaned grandchildren are now direct children of this subreaper,
        // including helpers which started a separate Unix process group.
        let root = i32::try_from(std::process::id()).map_err(io::Error::other)?;
        let managed = MANAGED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut pending = vec![root];
        let mut seen = BTreeSet::new();
        while let Some(parent) = pending.pop() {
            if !seen.insert(parent) {
                continue;
            }
            let tasks = match fs::read_dir(format!("/proc/{parent}/task")) {
                Ok(tasks) => tasks,
                Err(error) if vanished(&error) => continue,
                Err(error) => return Err(error),
            };
            for task in tasks {
                let task = match task {
                    Ok(task) => task,
                    Err(error) if vanished(&error) => continue,
                    Err(error) => return Err(error),
                };
                let path = task.path().join("children");
                let children = match fs::read_to_string(path) {
                    Ok(children) => children,
                    Err(error) if vanished(&error) => continue,
                    Err(error) => return Err(error),
                };
                for child in children.split_whitespace() {
                    let child = child.parse::<u32>().map_err(io::Error::other)?;
                    if parent == root && managed.contains_key(&child) && self.leader != Some(child)
                    {
                        continue;
                    }
                    let pid = i32::try_from(child).map_err(io::Error::other)?;
                    if let Some((start, state)) = identity(pid)? {
                        self.owned.insert(pid, start);
                        if parent == root && state == "Z" && !managed.contains_key(&child) {
                            reap_zombie(pid, start)?;
                            self.owned.remove(&pid);
                            continue;
                        }
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
        let managed = MANAGED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut active = false;
        for (&pid, &start) in &self.owned {
            if managed.contains_key(&u32::try_from(pid).map_err(io::Error::other)?) {
                continue;
            }
            if let Some((current, state)) = identity(pid)? {
                if current != start {
                    continue;
                }
                if state == "Z" {
                    reap_zombie(pid, start)?;
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

fn reap_zombie(pid: i32, start: u64) -> io::Result<()> {
    if identity(pid)?.is_some_and(|(current, state)| current == start && state == "Z") {
        match waitpid(Pid::from_raw(pid), Some(WaitPidFlag::WNOHANG)) {
            Ok(_) | Err(Errno::ECHILD | Errno::ESRCH) => {}
            Err(error) => return Err(io::Error::other(error)),
        }
    }
    Ok(())
}

fn identity(pid: i32) -> io::Result<Option<(u64, String)>> {
    let record = match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if vanished(&error) => return Ok(None),
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

pub(crate) fn verify_drained() -> io::Result<()> {
    let mut tree =
        Tree::new(None).ok_or_else(|| io::Error::other("process supervision is not enabled"))?;
    tree.finish()
}

fn vanished(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(Errno::ESRCH as i32)
}
