use super::*;
#[tokio::test]
async fn missing_private_directory_is_already_cleaned() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.execute(key("one"), "1").await.unwrap();
    let dirs = std::fs::read_dir(&f.temp)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_dir_all(dirs).unwrap();
    f.close().await;
}

#[tokio::test]
async fn blocking_directory_removal_obeys_deadline_and_retry() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.execute(key("one"), "1").await.unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let (release, waiting) = std::sync::mpsc::channel();
    let waiting = std::sync::Mutex::new(waiting);
    let notified = entered.clone();
    f.manager.hooks.lock().unwrap().before_directory_remove = Some(Arc::new(move || {
        notified.add_permits(1);
        waiting
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
    }));
    let manager = f.manager.clone();
    let closing = tokio::spawn(async move { manager.close(Duration::from_millis(100)).await });
    timeout(Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    code(
        timeout(Duration::from_secs(2), closing)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err(),
        "cleanup-incomplete",
    );
    assert_eq!(f.manager.live_runners(), 0);
    assert_eq!(std::fs::read_dir(&f.temp).unwrap().count(), 1);
    f.manager.hooks.lock().unwrap().before_directory_remove = None;
    release.send(()).unwrap();
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn close_interrupts_inflight_and_all_ignoring_owners_concurrently() {
    let grace = Duration::from_secs(2);
    let Some(f) = Fixture::new(|o| {
        o.limits.terminate_grace = grace;
        o.limits.shutdown_grace = Duration::from_secs(8);
    }) else {
        return;
    };
    for n in 0..4 {
        f.execute(key(&n.to_string()), "x=1").await.unwrap();
    }
    let barrier = Barrier::new();
    f.manager.hooks.lock().unwrap().after_request_write = Some(barrier.hook());
    let mut tasks = Vec::new();
    for n in 0..4 {
        let manager = f.manager.clone();
        let root = f.root.clone();
        tasks.push(tokio::spawn(async move {manager.execute_owner(key(&n.to_string()),root,CancellationToken::new(),"import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(60)".into(),Duration::from_secs(30)).await}));
    }
    for _ in 0..4 {
        barrier.entered().await;
    }
    sleep(Duration::from_millis(100)).await;
    f.manager.hooks.lock().unwrap().after_request_write = None;
    for _ in 0..4 {
        barrier.release();
    }
    let now = Instant::now();
    f.close().await;
    assert!(now.elapsed() < grace + Duration::from_secs(2));
    for task in tasks {
        code(task.await.unwrap().unwrap_err(), "manager-closing");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn quarantine_blocks_reuse_and_close_reports_survivors() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.manager
        .hooks
        .lock()
        .unwrap()
        .fail_kill
        .store(true, Ordering::SeqCst);
    f.execute(key("one"), "x=1").await.unwrap();
    code(
        f.manager
            .clear_owner(key("one"), f.root.clone(), CancellationToken::new())
            .await
            .unwrap_err(),
        "cleanup-incomplete",
    );
    code(
        f.execute(key("one"), "1").await.unwrap_err(),
        "owner-quarantined",
    );
    assert_eq!(f.manager.live_runners(), 1);
    code(
        f.manager.close(Duration::from_secs(5)).await.unwrap_err(),
        "cleanup-incomplete",
    );
    // Quarantine is intentionally permanent; the test owns emergency cleanup.
    f.manager
        .hooks
        .lock()
        .unwrap()
        .fail_kill
        .store(false, Ordering::SeqCst);
}

#[tokio::test(flavor = "multi_thread")]
async fn close_deadline_is_retryable_and_concurrent_close_joins() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.execute(key("one"), "x=1").await.unwrap();
    let owner = f.manager.state.lock().unwrap().owners[&key("one")].clone();
    let held = owner.gate.lock().await;
    code(
        f.manager.close(Duration::ZERO).await.unwrap_err(),
        "cleanup-incomplete",
    );
    drop(held);
    let (a, b) = tokio::join!(
        f.manager.close(Duration::from_secs(5)),
        f.manager.close(Duration::from_secs(5))
    );
    a.unwrap();
    b.unwrap();
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn close_during_runner_start_interrupts_and_removes_directories() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let started = directory.path().join("started");
    let wrapper = directory.path().join("python");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nprintf ready > '{}'\nexec /bin/sleep 60\n",
            started.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let Some(f) = Fixture::new(|o| o.python_path = wrapper) else {
        return;
    };
    let manager = f.manager.clone();
    let root = f.root.clone();
    let call = tokio::spawn(async move {
        manager
            .execute_owner(
                key("one"),
                root,
                CancellationToken::new(),
                "1".into(),
                Duration::from_secs(10),
            )
            .await
    });
    timeout(Duration::from_secs(5), async {
        while !started.exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let now = Instant::now();
    f.close().await;
    assert!(now.elapsed() < f.manager.configuration.limits.runner_start_timeout / 2);
    code(call.await.unwrap().unwrap_err(), "manager-closing");
}

#[tokio::test(flavor = "multi_thread")]
async fn first_timeout_has_no_generation_and_reap_failure_quarantines() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
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
    let next = f.execute(key("one"), "1").await.unwrap();
    assert_eq!(next.generation, 0);
    assert!(!next.state_reset);
    f.close().await;
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.manager.hooks.lock().unwrap().reap_timeout_once = true;
    f.execute(key("one"), "1").await.unwrap();
    code(
        f.manager
            .clear_owner(key("one"), f.root.clone(), CancellationToken::new())
            .await
            .unwrap_err(),
        "cleanup-incomplete",
    );
    code(
        f.execute(key("one"), "1").await.unwrap_err(),
        "owner-quarantined",
    );
    code(
        f.manager.close(Duration::from_secs(5)).await.unwrap_err(),
        "cleanup-incomplete",
    );
    assert_eq!(f.manager.live_runners(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn private_directory_removal_failure_is_reported_and_retryable() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.execute(key("one"), "1").await.unwrap();
    let dirs = std::fs::read_dir(&f.temp)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let moved = f._directory.path().join("retained");
    std::fs::rename(&dirs, &moved).unwrap();
    std::fs::write(&dirs, "fixture obstructs removal").unwrap();
    code(
        f.manager.close(Duration::from_secs(5)).await.unwrap_err(),
        "cleanup-incomplete",
    );
    assert_eq!(f.manager.live_runners(), 0);
    std::fs::remove_file(&dirs).unwrap();
    std::fs::rename(&moved, &dirs).unwrap();
    f.close().await;
}
