//! Test-owned process cleanup, shared by primitive and Agent fixtures.
use rustix::process::{Pid, Signal, kill_process, kill_process_group};
use std::path::{Path, PathBuf};
use tokio::time::{Duration, sleep, timeout};

// The shell publishes $! before Python can detach. A guard installed before
// launch can therefore always recover a detached holder, including on panic.
pub const HOLDER_COMMAND: &str = r#""$PYTHON" -c 'import os,time,pathlib
p=pathlib.Path(os.environ["HOLDER"])
limit=time.monotonic()+5
while not p.exists() or p.read_text()!=str(os.getpid()):
 if time.monotonic()>limit: raise SystemExit(1)
 time.sleep(.01)
os.setsid()
pathlib.Path(str(p)+".ready").write_text(str(os.getpid()))
time.sleep(60)' & printf '%s' "$!" > "$HOLDER"; while [ ! -s "$HOLDER.ready" ]; do sleep .01; done"#;

#[path = "python.rs"]
mod interpreter;
pub use interpreter::python;

pub struct ProcessCleanup {
    directory: PathBuf,
    group: Option<Pid>,
    groups_active: bool,
}
impl ProcessCleanup {
    pub fn new(directory: &Path) -> Self {
        Self {
            directory: directory.into(),
            group: None,
            groups_active: true,
        }
    }
    pub fn disarm_groups(&mut self) {
        self.groups_active = false;
    }
    pub fn set_group(&mut self, raw: i32) {
        self.group = Pid::from_raw(raw);
    }
    fn holder(&self) -> Option<Pid> {
        std::fs::read_to_string(self.directory.join("holder"))
            .ok()
            .and_then(|text| text.parse::<i32>().ok())
            .filter(|pid| *pid > 1)
            .and_then(Pid::from_raw)
    }
    pub fn holder_alive(&self) -> bool {
        let Some(pid) = self.holder() else {
            return false;
        };
        let output = std::process::Command::new("ps")
            .args(["-p", &pid.as_raw_nonzero().get().to_string(), "-o", "stat="])
            .output()
            .unwrap();
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .any(|line| !line.trim().is_empty() && !line.trim().starts_with('Z'))
    }
    pub async fn kill_holder(&self) {
        if let Some(pid) = self.holder() {
            let _ = kill_process(pid, Signal::KILL);
        }
        timeout(Duration::from_secs(5), async {
            while self.holder_alive() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached fixture holder survived cleanup");
        // A later guard must never signal this now-stale PID.
        std::fs::remove_file(self.directory.join("holder")).unwrap();
    }
}
impl Drop for ProcessCleanup {
    fn drop(&mut self) {
        if self.groups_active {
            if let Some(group) = self.group {
                let _ = kill_process_group(group, Signal::KILL);
            }
            // Stop all launchers before recovering the holder's published PID.
            if let Ok(entries) = std::fs::read_dir(&self.directory) {
                for entry in entries.flatten() {
                    if let Some(group) = entry
                        .file_name()
                        .to_str()
                        .and_then(|name| name.strip_prefix("pgid."))
                        .and_then(|id| id.parse::<i32>().ok())
                        .and_then(Pid::from_raw)
                    {
                        let _ = kill_process_group(group, Signal::KILL);
                    }
                }
            }
        }
        if let Some(pid) = self.holder() {
            let _ = kill_process(pid, Signal::KILL);
        }
    }
}
