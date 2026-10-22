use std::process::Command;

pub(super) use crate::process::python;

pub(super) fn group_members(id: i32) -> Vec<(i32, String)> {
    let output = Command::new("ps")
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
            let pgid: i32 = fields.next()?.parse().ok()?;
            let state = fields.next()?.to_owned();
            (pgid == id).then_some((pid, state))
        })
        .collect()
}

pub(super) async fn group_gone(id: i32) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !group_members(id).is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("runner process group survived cleanup");
}

pub(super) fn options(directory: &std::path::Path) -> Option<crate::python_repl::Options> {
    use crate::python_repl::*;
    use std::{collections::BTreeMap, time::Duration};
    Some(Options {
        python_path: python()?,
        python_identity: "test-python-v1".into(),
        temp_root: directory.into(),
        environment: Environment {
            mode: EnvironmentMode::ExplicitOnly,
            overrides: BTreeMap::new(),
            identity: "test-env-v1".into(),
        },
        limits: Limits {
            max_sessions: 4,
            max_queued_per_session: 2,
            max_code_bytes: 4096,
            max_output_bytes_per_stream: 4096,
            max_result_bytes: 4096,
            max_exception_bytes: 8192,
            max_environment_entries: 1024,
            max_environment_bytes: 256 * 1024,
            default_timeout: Duration::from_secs(10),
            max_timeout: Duration::from_secs(30),
            runner_start_timeout: Duration::from_secs(10),
            terminate_grace: Duration::from_millis(300),
            kill_wait: Duration::from_secs(3),
            shutdown_grace: Duration::from_secs(5),
        },
    })
}
