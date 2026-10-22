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
async fn paused_pending_execute_runs_once_with_fresh_state() {
    if !python_available() {
        return;
    }
    let p = pause(true).await;
    let ext = Arc::new(PythonRepl::new(p.options.clone()).unwrap());
    assert_eq!(p.ext.config_hash(), ext.config_hash());
    let a = resumed(&p, ext.clone()).await;
    a.close_extensions().await.unwrap();
    p.agent.close_extensions().await.unwrap();
    groups_gone(p.dir.path()).await;
    assert_eq!(ext.live_runners(), 0);
    assert_eq!(std::fs::read_dir(&p.options.temp_root).unwrap().count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_refuses_every_drift_before_mutation() {
    if !python_available() {
        return;
    }
    let edits = mutations();
    assert_eq!(edits.len(), 14 + 3);
    let mut labels = edits.iter().map(|(label, _)| *label).collect::<Vec<_>>();
    labels.extend(["python_path", "removed", "added"]);
    assert_eq!(labels.len(), 20);
    for (index, label) in labels.into_iter().enumerate() {
        eprintln!("python resume drift: {label}");
        let p = pause(label != "added").await;
        let mut o = p.options.clone();
        if index < edits.len() {
            (edits[index].1)(&mut o);
        }
        if label == "python_path" {
            use std::os::unix::fs::PermissionsExt;
            let wrapper = p.dir.path().join("python-wrapper");
            std::fs::write(
                &wrapper,
                format!("#!/bin/sh\nexec '{}' \"$@\"\n", o.python_path.display()),
            )
            .unwrap();
            std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
            o.python_path = wrapper;
        }
        let changed = Arc::new(PythonRepl::new(o).unwrap());
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
        assert_eq!(changed.live_runners() + p.ext.live_runners(), 0);
        assert!(pgids(p.dir.path()).is_empty());
        assert_eq!(std::fs::read_dir(&p.options.temp_root).unwrap().count(), 0);
        assert!(!p.dir.path().join("executions").exists());
        a.close_extensions().await.unwrap();
        p.agent.close_extensions().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_values_and_temp_root_do_not_change_the_fingerprint() {
    if !python_available() {
        return;
    }
    for change_root in [false, true] {
        let p = pause(true).await;
        let mut o = p.options.clone();
        let root = tempfile::tempdir().unwrap();
        if change_root {
            o.temp_root = root.path().into();
        } else {
            o.environment
                .overrides
                .insert("FIXTURE_SECRET".into(), "changed-value".into());
            o.environment
                .overrides
                .insert("NEW_KEY".into(), "new-value".into());
        }
        let temp_root = o.temp_root.clone();
        let ext = Arc::new(PythonRepl::new(o).unwrap());
        assert_eq!(p.ext.config_hash(), ext.config_hash());
        let a = resumed(&p, ext.clone()).await;
        a.close_extensions().await.unwrap();
        p.agent.close_extensions().await.unwrap();
        groups_gone(p.dir.path()).await;
        assert_eq!(ext.live_runners(), 0);
        assert_eq!(std::fs::read_dir(temp_root).unwrap().count(), 0);
    }
}
