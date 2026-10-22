use crate::fixture::*;
use crabber::extension::FactoryScope;
use crabber_extensions::{
    background_jobs as jobs, delegate_task as delegate, python_repl as python,
};
use serde_json::json;
use std::{path::Path, time::Duration};

pub(super) async fn pid_file(root: &Path, name: &str) -> rustix::process::Pid {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(root.join(name))
                && let Ok(raw) = text.parse::<i32>()
            {
                return rustix::process::Pid::from_raw(raw).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
pub(super) async fn gone(groups: &[rustix::process::Pid]) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while groups
            .iter()
            .any(|pid| rustix::process::test_kill_process_group(*pid).is_ok())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owner process groups survived close");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn targeted_close_joins_one_owners_processes_and_leaves_other_session_alive() {
    let Some(h) = Harness::new(FactoryScope::Session, runner()) else {
        return;
    };
    let a = context(&h.root.path().join("a"), "ws", "a");
    let b = context(&h.root.path().join("a"), "ws", "b");
    let mut ids = Vec::new();
    let mut groups = Vec::new();
    for (execution, label) in [(&a, "a"), (&b, "b")] {
        let records = h.first(execution, &[
            (jobs::START_TOOL, json!({"command":format!("printf '%s' \"$PPID\" > \"$FIXTURE_ROOT/job-{label}\"; printf '%s' \"$PPID\" > \"$FIXTURE_ROOT/pgid.$PPID\"; sleep 60")})),
            (python::EXECUTE_TOOL, json!({"code":record_code(&format!("pathlib.Path(os.environ['FIXTURE_ROOT'],'py-{label}').write_text(str(os.getpgrp())); x=41; x"))})),
            (delegate::START_TOOL, json!({"task":"retained", "profile":"test"})),
        ]).await;
        ids.push(value(&records[0])["id"].as_str().unwrap().to_owned());
        assert_eq!(value(&records[1])["result"]["text"], "41");
        groups.push([
            pid_file(h.root.path(), &format!("job-{label}")).await,
            pid_file(h.root.path(), &format!("py-{label}")).await,
        ]);
    }
    h.agent
        .close_session_resources(a.session(), Duration::from_secs(5))
        .await
        .unwrap();
    gone(&groups[0]).await;
    for pid in groups[1] {
        assert!(rustix::process::test_kill_process_group(pid).is_ok());
    }
    let records = h
        .next(
            &b,
            &[
                (jobs::STATUS_TOOL, json!({"id":ids[1]})),
                (python::EXECUTE_TOOL, json!({"code":"x+=1; x"})),
            ],
        )
        .await;
    assert_eq!(value(&records[0])["state"], "running");
    assert_eq!(value(&records[1])["result"]["text"], "42");
    let report = h
        .jobs
        .owner_report("ws", Some(b.session().session_id()))
        .unwrap();
    assert_eq!(report.live, 1);
    assert!(!report.closing);
    for execution in [&a, &b] {
        let reports = h
            .agent
            .drain_session_invocations(
                execution.session().session_id(),
                true,
                Duration::from_secs(5),
            )
            .await;
        assert_eq!(reports.len(), 1);
        assert!(reports[0].local_joined);
        assert_eq!(reports[0].status, crabber::SessionInvocationStatus::Joined);
    }
    h.close().await;
}
