use super::*;
#[tokio::test(flavor = "multi_thread")]
async fn state_errors_clear_and_private_directories() {
    use std::os::unix::fs::PermissionsExt;
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    let first = f.execute(key("one"), "x = 41").await.unwrap();
    assert_eq!(first.generation, 0);
    assert_eq!(
        f.execute(key("one"), "x + 1").await.unwrap().result.text,
        "42"
    );
    assert_eq!(
        f.execute(key("one"), "1/0").await.unwrap().status,
        ExecuteStatus::PythonError
    );
    assert_eq!(f.execute(key("one"), "x").await.unwrap().result.text, "41");
    let dirs = std::fs::read_dir(&f.temp)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert_eq!(dirs.metadata().unwrap().permissions().mode() & 0o777, 0o700);
    for child in std::fs::read_dir(&dirs).unwrap() {
        assert_eq!(
            child.unwrap().metadata().unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
    let clear = f
        .manager
        .clear_owner(key("one"), f.root.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert!(clear.had_state);
    assert_eq!(clear.generation, 1);
    assert!(dirs.exists());
    let after_clear = f.execute(key("one"), "x").await.unwrap();
    assert_eq!(after_clear.status, ExecuteStatus::PythonError);
    assert_eq!(after_clear.state_reset_reason, "cleared");
    assert!(after_clear.state_reset);
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn owners_are_isolated_and_progress_concurrently() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.execute(key("one"), "x = 1").await.unwrap();
    assert_eq!(
        f.execute(key("two"), "x").await.unwrap().status,
        ExecuteStatus::PythonError
    );
    let (a, b) = tokio::join!(
        f.execute(key("one"), "import time; time.sleep(.2); x"),
        f.execute(key("two"), "import os; os.environ['HOME']")
    );
    assert_eq!(a.unwrap().result.text, "1");
    assert!(b.unwrap().result.text.contains(f.temp.to_str().unwrap()));
    let one=f.execute(key("one"),"import os; tuple(os.environ[k] for k in ['HOME','XDG_CACHE_HOME','XDG_CONFIG_HOME','XDG_DATA_HOME','XDG_STATE_HOME','XDG_RUNTIME_DIR','TMPDIR','TMP','TEMP'])").await.unwrap();
    let two=f.execute(key("two"),"tuple(os.environ[k] for k in ['HOME','XDG_CACHE_HOME','XDG_CONFIG_HOME','XDG_DATA_HOME','XDG_STATE_HOME','XDG_RUNTIME_DIR','TMPDIR','TMP','TEMP'])").await.unwrap();
    assert_ne!(one.result.text, two.result.text);
    assert!(one.result.text.contains(f.temp.to_str().unwrap()));
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn lifetime_budget_and_failed_setup_admissions() {
    let Some(f) = Fixture::new(|o| o.limits.max_sessions = 1) else {
        return;
    };
    assert!(
        !f.manager
            .clear_owner(key("unknown"), f.root.clone(), CancellationToken::new())
            .await
            .unwrap()
            .had_state
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    code(
        f.manager
            .execute_owner(
                key("canceled"),
                f.root.clone(),
                cancel,
                "1".into(),
                Duration::from_secs(1),
            )
            .await
            .unwrap_err(),
        "cancelled",
    );
    f.execute(key("one"), "1").await.unwrap();
    f.manager
        .clear_owner(key("one"), f.root.clone(), CancellationToken::new())
        .await
        .unwrap();
    code(
        f.execute(key("two"), "1").await.unwrap_err(),
        "capacity-exhausted",
    );
    f.close().await;
    let Some(f) = Fixture::new(|o| o.limits.max_sessions = 1) else {
        return;
    };
    std::fs::remove_dir(&f.temp).unwrap();
    code(
        f.execute(key("one"), "1").await.unwrap_err(),
        "private-dirs",
    );
    std::fs::create_dir(&f.temp).unwrap();
    code(
        f.execute(key("two"), "1").await.unwrap_err(),
        "capacity-exhausted",
    );
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn workspace_root_drift_preserves_state() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.execute(key("one"), "x=7").await.unwrap();
    let other = tempfile::tempdir().unwrap();
    code(
        f.manager
            .execute_owner(
                key("one"),
                other.path().into(),
                CancellationToken::new(),
                "x=8".into(),
                Duration::from_secs(1),
            )
            .await
            .unwrap_err(),
        "workspace-root-mismatch",
    );
    code(
        f.manager
            .clear_owner(key("one"), other.path().into(), CancellationToken::new())
            .await
            .unwrap_err(),
        "workspace-root-mismatch",
    );
    assert_eq!(f.execute(key("one"), "x").await.unwrap().result.text, "7");
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn timeout_reset_and_clear_consumes_pending_notice() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.execute(key("one"), "x=1").await.unwrap();
    code(
        f.manager
            .execute_owner(
                key("one"),
                f.root.clone(),
                CancellationToken::new(),
                "import time; time.sleep(60)".into(),
                Duration::from_millis(100),
            )
            .await
            .unwrap_err(),
        "timed-out",
    );
    let next = f.execute(key("one"), "x").await.unwrap();
    assert_eq!(next.generation, 1);
    assert!(next.state_reset);
    assert_eq!(next.state_reset_reason, "timed_out");
    assert_eq!(next.status, ExecuteStatus::PythonError);
    code(
        f.manager
            .execute_owner(
                key("one"),
                f.root.clone(),
                CancellationToken::new(),
                "import time; time.sleep(60)".into(),
                Duration::from_millis(100),
            )
            .await
            .unwrap_err(),
        "timed-out",
    );
    let clear = f
        .manager
        .clear_owner(key("one"), f.root.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert!(!clear.had_state);
    assert!(!f.execute(key("one"), "1").await.unwrap().state_reset);
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_and_mid_request_death_reset_existing_state() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.execute(key("one"), "x=1").await.unwrap();
    let id = f.manager.hooks.lock().unwrap().pgids[0];
    rustix::process::kill_process_group(
        rustix::process::Pid::from_raw(id).unwrap(),
        rustix::process::Signal::KILL,
    )
    .unwrap();
    sleep(Duration::from_millis(100)).await;
    code(
        f.execute(key("one"), "1").await.unwrap_err(),
        "runner-failed",
    );
    let next = f.execute(key("one"), "1").await.unwrap();
    assert_eq!(next.state_reset_reason, "runner_failed");
    assert_eq!(next.generation, 1);
    code(
        f.execute(key("one"), "import os; os._exit(3)")
            .await
            .unwrap_err(),
        "runner-failed",
    );
    let next = f.execute(key("one"), "1").await.unwrap();
    assert_eq!(next.state_reset_reason, "runner_failed");
    assert_eq!(next.generation, 2);
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn symlinked_interpreter_executes_unresolved() {
    let Some(mut f) = Fixture::new(|_| {}) else {
        return;
    };
    let path = f._directory.path().canonicalize().unwrap().join("python");
    std::os::unix::fs::symlink(&f.manager.configuration.python, &path).unwrap();
    assert_ne!(path, path.canonicalize().unwrap());
    let mut opts = options(&f.temp).unwrap();
    opts.python_path = path.clone();
    f.manager = Arc::new(Manager::new(config::validate(opts).unwrap().0));
    assert_eq!(f.manager.configuration.python, path);
    assert_eq!(
        f.execute(key("one"), "import sys; sys.executable")
            .await
            .unwrap()
            .result
            .text,
        format!("'{}'", path.display())
    );
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn eof_failure_keeps_leader_zombie_until_reset() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.execute(key("one"), "x=1").await.unwrap();
    let id = f.manager.hooks.lock().unwrap().pgids[0];
    let barrier = Barrier::new();
    f.manager.hooks.lock().unwrap().before_reset = Some(barrier.hook());
    let manager = f.manager.clone();
    let root = f.root.clone();
    let task = tokio::spawn(async move {
        manager
            .execute_owner(
                key("one"),
                root,
                CancellationToken::new(),
                "import os; os._exit(3)".into(),
                Duration::from_secs(10),
            )
            .await
    });
    barrier.entered().await;
    assert!(
        super::super::support::group_members(id)
            .iter()
            .any(|(pid, state)| *pid == id && state.starts_with('Z'))
    );
    f.manager.hooks.lock().unwrap().before_reset = None;
    barrier.release();
    code(task.await.unwrap().unwrap_err(), "runner-failed");
    f.close().await;
}
