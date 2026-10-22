use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn anchor_ignores_term_and_the_command_receives_it() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut group = f.released(": > ready; sleep 100").await;
    let pgid = group.pgid();
    timeout(WAIT, async {
        while !f.directory.path().join("ready").exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    group.signal(GroupSignal::Terminate).unwrap();
    let status = timeout(WAIT, group.exited()).await.unwrap().unwrap();
    assert_eq!(status.code(), Some(143));
    assert_eq!(group_members(pgid).len(), 1);
    group.sweep_and_reap(status, WAIT).await;
    f.finish(pgid).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn terminate_escalates_from_term_to_kill() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut group = f.released("trap '' TERM; : > ready; sleep 100").await;
    let pgid = group.pgid();
    timeout(WAIT, async {
        while !f.directory.path().join("ready").exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let start = Instant::now();
    let reap = group
        .terminate(GroupSignal::Terminate, Duration::from_millis(300), WAIT)
        .await;
    assert!(reap.reaped);
    assert!(reap.status.unwrap().code().is_none());
    assert!(start.elapsed() >= Duration::from_millis(300));
    f.finish(pgid).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn terminate_retry_does_not_resend_term() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut group = f.released("sleep 100").await;
    let pgid = group.pgid();
    group
        .hooks
        .fail_kill_once
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(
        !group
            .terminate(GroupSignal::Terminate, Duration::from_millis(100), WAIT)
            .await
            .reaped
    );
    assert!(
        group
            .terminate(GroupSignal::Terminate, Duration::from_millis(100), WAIT)
            .await
            .reaped
    );
    assert_eq!(
        *group.hooks.signals.lock().unwrap(),
        vec![GroupSignal::Terminate, GroupSignal::Kill, GroupSignal::Kill]
    );
    f.finish(pgid).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_of_a_live_group_kills_the_group() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let group = f.released("sleep 100").await;
    let pgid = group.pgid();
    drop(group);
    f.finish(pgid).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_after_exit_before_sweep_kills_the_anchor() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut group = f.released("exit 0").await;
    let pgid = group.pgid();
    timeout(WAIT, group.exited()).await.unwrap().unwrap();
    drop(group);
    f.finish(pgid).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_termination_keeps_the_original_grace_deadline() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut group = f.released("trap '' TERM; : > ready; sleep 100").await;
    let pgid = group.pgid();
    timeout(WAIT, async {
        while !f.directory.path().join("ready").exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        timeout(
            Duration::from_millis(100),
            group.terminate(GroupSignal::Terminate, Duration::from_millis(300), WAIT)
        )
        .await
        .is_err()
    );
    sleep(Duration::from_millis(250)).await;
    let reap = timeout(
        Duration::from_secs(1),
        group.terminate(GroupSignal::Terminate, Duration::from_secs(10), WAIT),
    )
    .await
    .unwrap();
    assert!(reap.reaped);
    assert_eq!(
        *group.hooks.signals.lock().unwrap(),
        vec![GroupSignal::Terminate, GroupSignal::Kill]
    );
    f.finish(pgid).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reap_timeout_retry_does_not_resend_signals() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut group = f.released("sleep 100").await;
    let pgid = group.pgid();
    group
        .hooks
        .reap_timeout_once
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let first = group
        .terminate(GroupSignal::Kill, Duration::ZERO, WAIT)
        .await;
    assert!(!first.reaped);
    assert!(!first.output_forced);
    let second = group
        .terminate(GroupSignal::Terminate, Duration::from_secs(10), WAIT)
        .await;
    assert!(second.reaped);
    assert!(second.status.is_some());
    assert_eq!(
        *group.hooks.signals.lock().unwrap(),
        vec![GroupSignal::Kill]
    );
    f.finish(pgid).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_natural_sweep_retains_ownership_for_retry() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut group = f.released("sleep 100 & exit 7").await;
    let pgid = group.pgid();
    let status = timeout(WAIT, group.exited()).await.unwrap().unwrap();
    assert_eq!(status.code(), Some(7));
    for _ in 0..2 {
        group
            .hooks
            .fail_kill_once
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let reap = group.sweep_and_reap(status, WAIT).await;
        assert!(!reap.reaped);
        assert_eq!(reap.status.unwrap().code(), Some(7));
        assert!(!group_members(pgid).is_empty());
    }
    let reap = group.sweep_and_reap(status, WAIT).await;
    assert!(reap.reaped);
    assert_eq!(reap.status.unwrap().code(), Some(7));
    f.finish(pgid).await;
}
