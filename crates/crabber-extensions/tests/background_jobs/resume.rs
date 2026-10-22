use super::*;
use crabber::{
    RuntimeError,
    core::ToolInfo,
    extension::ToolDefinition,
    runtime::{InterruptPolicy, PermissionPolicy},
};

#[path = "resume/fixture.rs"]
mod fixture;
use fixture::*;

#[tokio::test(flavor = "multi_thread")]
async fn paused_pending_start_runs_once_on_resume() {
    if !shell_available() {
        return;
    }
    let p = pause(true).await;
    let ext = Arc::new(BackgroundJobs::new(p.options.clone()).unwrap());
    assert_eq!(p.ext.config_hash(), ext.config_hash());
    let a = resumed(&p, ext.clone()).await;
    assert_eq!(p.ext.live_jobs(), 0);
    a.close_extensions().await.unwrap();
    p.agent.close_extensions().await.unwrap();
    groups_gone(p.dir.path()).await;
    assert_eq!(ext.live_jobs(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_refuses_every_drift_before_mutation() {
    if !shell_available() {
        return;
    }
    let edits = mutations();
    assert_eq!(edits.len(), 15);
    let mut labels = edits.iter().map(|(label, _)| *label).collect::<Vec<_>>();
    labels.extend(["shell_path", "removed", "added"]);
    assert_eq!(labels.len(), 18);
    for (index, label) in labels.into_iter().enumerate() {
        eprintln!("resume drift: {label}");
        let p = pause(label != "added").await;
        let mut o = p.options.clone();
        if index < edits.len() {
            (edits[index].1)(&mut o);
        }
        if label == "shell_path" {
            let copy = p.dir.path().join("shell-copy");
            std::fs::copy(o.shell_path.canonicalize().unwrap(), &copy).unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&copy, std::fs::Permissions::from_mode(0o700)).unwrap();
            assert!(
                std::process::Command::new(&copy)
                    .args(["-c", "exit 0"])
                    .status()
                    .unwrap()
                    .success()
            );
            o.shell_path = copy;
        }
        let changed = Arc::new(BackgroundJobs::new(o).unwrap());
        let a = build(
            p.store.clone(),
            (label != "removed").then(|| changed.clone()),
            p.dir.path(),
            false,
            vec![done()],
        );
        let before = persistence(&p).await;
        assert!(
            matches!(a.resume(&p.run).await, Err(RuntimeError::PlanChanged)),
            "{label}"
        );
        assert_eq!(persistence(&p).await, before, "{label}");
        assert!(pgids(p.dir.path()).is_empty());
        assert_eq!(changed.live_jobs() + p.ext.live_jobs(), 0);
        let equal = Arc::new(BackgroundJobs::new(p.options.clone()).unwrap());
        let original = resumed(&p, equal.clone()).await;
        a.close_extensions().await.unwrap();
        original.close_extensions().await.unwrap();
        p.agent.close_extensions().await.unwrap();
        assert_eq!(equal.live_jobs(), 0);
        groups_gone(p.dir.path()).await;
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn environment_values_do_not_change_the_fingerprint() {
    if !shell_available() {
        return;
    }
    let p = pause(true).await;
    let mut o = p.options.clone();
    o.environment
        .overrides
        .insert("CHANGED_VALUE".into(), "synthetic-value".into());
    let ext = Arc::new(BackgroundJobs::new(o).unwrap());
    assert_eq!(p.ext.config_hash(), ext.config_hash());
    let a = resumed(&p, ext.clone()).await;
    a.close_extensions().await.unwrap();
    p.agent.close_extensions().await.unwrap();
    groups_gone(p.dir.path()).await;
    assert_eq!(ext.live_jobs(), 0);
}
#[tokio::test(flavor = "multi_thread")]
async fn status_after_restart_sees_no_jobs() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        call(START_TOOL, json!({"command":command("exit 0")})),
        done(),
    ]));
    let a = agent(
        h.ext.clone(),
        store.clone(),
        provider,
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = a.prompt(None, "start").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let job = result_value(&snapshot_tools(&store, &session).await[0]);
    super::observe::terminal_for(&h, &session, job["id"].as_str().unwrap()).await;
    a.close_extensions().await.unwrap();
    h.close().await;
    let fresh = Arc::new(BackgroundJobs::new(options(h.dir.path())).unwrap());
    let provider = Arc::new(FakeProvider::scripted(vec![
        calls(&[
            (STATUS_TOOL, json!({"id":job["id"]})),
            (LIST_TOOL, json!({})),
        ]),
        done(),
    ]));
    let a = agent(
        fresh.clone(),
        store.clone(),
        provider,
        PermissionDecision::Allow,
        h.dir.path(),
    );
    a.prompt(Some(session.clone()), "restart")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    let status = records.iter().find(|r| r.name == STATUS_TOOL).unwrap();
    assert_eq!(status.status, ToolCallStatus::Failed);
    assert!(
        result_value(status)
            .as_str()
            .unwrap()
            .ends_with("job-not-found")
    );
    assert_eq!(
        result_value(records.iter().find(|r| r.name == LIST_TOOL).unwrap()),
        json!({"jobs":[]})
    );
    a.close_extensions().await.unwrap();
    assert_eq!(fresh.live_jobs(), 0);
}
