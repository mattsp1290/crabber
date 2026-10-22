use super::*;
use super::{
    job::{Cause, Job, State},
    manager::{Owner, Policy},
};
use crate::process::{Reap, Tail, Tails};
use crabber::{
    core::{RunId, SessionId, ToolCallId},
    extension::{HostServices, ToolContext, WorkspaceContext},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::{Duration, UNIX_EPOCH},
};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

fn options() -> Options {
    Options {
        shell_path: "/bin/sh".into(),
        shell_identity: "test-sh-v1".into(),
        environment: Environment {
            mode: EnvironmentMode::ExplicitOnly,
            overrides: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
            identity: "test-env-v1".into(),
        },
        limits: Limits {
            max_running: 2,
            max_tracked: 4,
            max_command_bytes: 4096,
            max_working_directory_bytes: 1024,
            max_output_bytes_per_stream: 4096,
            max_environment_entries: 1024,
            max_environment_bytes: 256 * 1024,
            default_timeout: Duration::ZERO,
            max_timeout: Duration::from_secs(30),
            terminate_grace: Duration::from_millis(100),
            kill_wait: Duration::from_secs(2),
            shutdown_grace: Duration::from_secs(3),
        },
    }
}
fn policy(options: Options) -> Arc<Policy> {
    BackgroundJobs::new(options).unwrap().policy
}
fn owner() -> Owner {
    Owner {
        session: SessionId::from("session"),
        workspace: "workspace".into(),
    }
}
fn tails() -> Tails {
    Tails {
        stdout: Arc::new(Mutex::new(Tail::new(4096))),
        stderr: Arc::new(Mutex::new(Tail::new(4096))),
    }
}
fn context(dir: &std::path::Path, cancel: CancellationToken) -> ToolContext {
    ToolContext::new(
        SessionId::from("session"),
        RunId::from("run"),
        ToolCallId::from("call"),
        cancel,
        HostServices::default(),
        WorkspaceContext::from_persisted("workspace", dir.to_str().unwrap()),
        Arc::new(|_| {}),
    )
}
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
            "required shell/ps unavailable"
        );
        eprintln!("background_jobs: /bin/sh or ps absent, skipping");
    }
    available
}
fn group_members(pgid: i32) -> Vec<u32> {
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
            (group == pgid && !state.starts_with('Z')).then_some(pid)
        })
        .collect()
}
async fn finish(policy: &Policy) {
    policy.close(Duration::from_secs(5)).await.unwrap();
    policy.cleanup.join(Duration::from_secs(3)).await.unwrap();
    let pgids = policy.test_hooks.lock().unwrap().pgids.clone();
    for pgid in pgids {
        timeout(Duration::from_secs(3), async {
            while !group_members(pgid).is_empty() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    assert_eq!(policy.live_jobs(), 0);
}
async fn terminal(policy: &Policy, dir: &std::path::Path, id: &str) -> Value {
    timeout(Duration::from_secs(5), async {
        loop {
            let status = policy
                .status(context(dir, CancellationToken::new()), json!({"id":id}))
                .unwrap();
            if status["state"] != "running" {
                return status;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[path = "tests/contract.rs"]
mod contract;

#[path = "tests/configuration.rs"]
mod configuration;

#[path = "tests/lifecycle.rs"]
mod lifecycle;

#[path = "tests/retry.rs"]
mod retry;
