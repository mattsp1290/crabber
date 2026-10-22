use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn natural_exit_code_and_output_are_exact() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut group = f.released("printf out; printf err >&2; exit 3").await;
    let pgid = group.pgid();
    let status = timeout(WAIT, group.exited()).await.unwrap().unwrap();
    assert_eq!(status.code(), Some(3));
    let reap = group.sweep_and_reap(status, WAIT).await;
    assert!(reap.reaped);
    assert_eq!(reap.status.unwrap().code(), Some(3));
    assert!(!reap.output_forced);
    assert_eq!(
        f.tails.stdout.lock().unwrap().snapshot(),
        ("out".into(), false)
    );
    assert_eq!(
        f.tails.stderr.lock().unwrap().snapshot(),
        ("err".into(), false)
    );
    f.finish(pgid).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn held_output_is_forced_closed_at_kill_wait() {
    if !shell_available() {
        return;
    }
    let Some(python) = owned_process::python() else {
        return;
    };
    let f = Fixture::new();
    let mut cleanup = owned_process::ProcessCleanup::new(f.directory.path());
    let environment = [
        ("PATH".into(), "/usr/bin:/bin".into()),
        ("PYTHON".into(), python.to_str().unwrap().into()),
        (
            "HOLDER".into(),
            f.directory.path().join("holder").to_str().unwrap().into(),
        ),
    ];
    let spawned = f.spawn(
        &format!("{}; exit 0", owned_process::HOLDER_COMMAND),
        &environment,
    );
    cleanup.set_group(spawned.pgid().as_raw_nonzero().get());
    let mut group = spawned.release_gate().await.unwrap();
    let pgid = group.pgid();
    let status = timeout(WAIT, group.exited()).await.unwrap().unwrap();
    assert!(cleanup.holder_alive());
    let start = Instant::now();
    let reap = group
        .sweep_and_reap(status, Duration::from_millis(500))
        .await;
    assert!(reap.reaped && reap.output_forced);
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(f.tails.stdout.lock().unwrap().snapshot().1);
    assert!(f.tails.stderr.lock().unwrap().snapshot().1);
    cleanup.kill_holder().await;
    f.finish(pgid).await;
    cleanup.disarm_groups();
}

#[tokio::test(flavor = "multi_thread")]
async fn pump_abandon_marks_truncated() {
    let (_writer, reader) = tokio::io::duplex(32);
    let tail = Arc::new(Mutex::new(Tail::new(32)));
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(unix::pump(reader, tail.clone(), cancel.clone()));
    cancel.cancel();
    timeout(Duration::from_millis(100), handle)
        .await
        .unwrap()
        .unwrap();
    assert!(tail.lock().unwrap().snapshot().1);
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_and_directory_are_applied_and_frozen() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut group = f
        .spawn(
            "printf '%s|%s|%s' \"$FROZEN\" \"$(pwd -P)\" \"$HOME\"",
            &[("FROZEN".into(), "synthetic".into())],
        )
        .release_gate()
        .await
        .unwrap();
    let pgid = group.pgid();
    let status = timeout(WAIT, group.exited()).await.unwrap().unwrap();
    group.sweep_and_reap(status, WAIT).await;
    assert_eq!(
        f.tails.stdout.lock().unwrap().snapshot().0,
        format!(
            "synthetic|{}|",
            f.directory.path().canonicalize().unwrap().display()
        )
    );
    f.finish(pgid).await;
}
