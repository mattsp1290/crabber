use super::*;
use crabber::extension::CleanupOwner;
use rustix::process::Pid;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

const WAIT: Duration = Duration::from_secs(3);

fn shell_available() -> bool {
    use std::os::unix::fs::PermissionsExt;
    let available = std::fs::metadata("/bin/sh").is_ok_and(|m| m.permissions().mode() & 0o111 != 0)
        && std::process::Command::new("ps")
            .args(["-A", "-o", "pid=,pgid=,stat="])
            .output()
            .is_ok_and(|o| o.status.success());
    if !available {
        assert_ne!(
            std::env::var("BACKGROUND_JOBS_REQUIRE_SHELL").as_deref(),
            Ok("1"),
            "required /bin/sh or ps absent"
        );
        eprintln!("background_jobs: /bin/sh or ps absent, skipping");
    }
    available
}

fn group_members(pgid: Pid) -> Vec<u32> {
    let output = std::process::Command::new("ps")
        .args(["-A", "-o", "pid=,pgid=,stat="])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse().ok()?;
            let group: i32 = fields.next()?.parse().ok()?;
            let state = fields.next()?;
            (group == pgid.as_raw_nonzero().get() && !state.starts_with('Z')).then_some(pid)
        })
        .collect()
}

async fn group_gone(pgid: Pid) {
    timeout(WAIT, async {
        while !group_members(pgid).is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("group still has live members");
}

struct Fixture {
    owner: CleanupOwner,
    directory: tempfile::TempDir,
    tails: Tails,
}
impl Fixture {
    fn new() -> Self {
        Self {
            owner: CleanupOwner::new(),
            directory: tempfile::tempdir().unwrap(),
            tails: Tails {
                stdout: Arc::new(Mutex::new(Tail::new(4096))),
                stderr: Arc::new(Mutex::new(Tail::new(4096))),
            },
        }
    }
    fn spawn(&self, command: &str, environment: &[(String, String)]) -> Spawned {
        spawn(
            Launch {
                shell: Path::new("/bin/sh"),
                command,
                directory: self.directory.path(),
                environment,
            },
            self.tails.clone(),
            &self.owner.tracker(),
        )
        .unwrap()
    }
    async fn finish(&self, pgid: Pid) {
        self.owner.join(WAIT).await.unwrap();
        group_gone(pgid).await;
    }
    async fn released(&self, command: &str) -> Group {
        self.spawn(command, &[]).release_gate().await.unwrap()
    }
}

#[path = "../../tests/support/owned_process.rs"]
mod owned_process;
pub(crate) use owned_process::python;

#[path = "tests/contract.rs"]
mod contract;

#[path = "tests/launch.rs"]
mod launch;

#[path = "tests/termination.rs"]
mod termination;

#[path = "tests/output.rs"]
mod output;
