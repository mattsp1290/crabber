use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn withheld_gate_never_runs_the_command() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let command = ": > canary";
    let spawned = f.spawn(command, &[]);
    let pgid = spawned.pgid();
    let mut group = spawned.withhold_gate();
    sleep(Duration::from_millis(300)).await;
    assert!(!f.directory.path().join("canary").exists());
    assert!(
        group
            .terminate(GroupSignal::Kill, Duration::ZERO, WAIT)
            .await
            .reaped
    );
    assert!(!f.directory.path().join("canary").exists());
    f.finish(pgid).await;
    let mut group = f.released(command).await;
    let pgid = group.pgid();
    let status = timeout(WAIT, group.exited()).await.unwrap().unwrap();
    assert!(status.success());
    assert!(f.directory.path().join("canary").exists());
    group.sweep_and_reap(status, WAIT).await;
    f.finish(pgid).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn gate_release_after_supervisor_death_is_a_gate_fault() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut spawned = f.spawn(": > canary", &[]);
    let pgid = spawned.pgid();
    signal_group(pgid, GroupSignal::Kill).unwrap();
    // Sending KILL is not proof of death. Reap the owned supervisor before
    // testing the dead-reader gate path, including Darwin's zombie-only EPERM.
    timeout(WAIT, spawned.exited()).await.unwrap().unwrap();
    // Other concurrent spawns may transiently retain a pipe reader. A gate
    // write outcome alone does not prove supervisor liveness; both outcomes
    // must retain the group for cleanup after the owned supervisor is reaped.
    let mut group = match spawned.release_gate().await {
        Ok(group) => group,
        Err(error) => *error.group,
    };
    assert!(
        group
            .terminate(GroupSignal::Kill, Duration::ZERO, WAIT)
            .await
            .reaped
    );
    f.finish(pgid).await;
    assert!(!f.directory.path().join("canary").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn anchor_keeps_the_group_alive_until_the_sweep() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut group = f.released("exit 0").await;
    let pgid = group.pgid();
    let status = timeout(WAIT, group.exited()).await.unwrap().unwrap();
    assert_eq!(group_members(pgid).len(), 1);
    group.sweep_and_reap(status, WAIT).await;
    f.finish(pgid).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn anchor_starts_with_an_empty_path() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut group = f
        .spawn("exit 0", &[("PATH".into(), "".into())])
        .release_gate()
        .await
        .unwrap();
    let pgid = group.pgid();
    let status = timeout(WAIT, group.exited()).await.unwrap().unwrap();
    assert!(status.success());
    assert_eq!(group_members(pgid).len(), 1);
    group.sweep_and_reap(status, WAIT).await;
    f.finish(pgid).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_gate_write_returns_the_owned_group_for_cleanup() {
    if !shell_available() {
        return;
    }
    let f = Fixture::new();
    let mut spawned = f.spawn(": > canary", &[]);
    let pgid = spawned.pgid();
    spawned.fail_gate_once();
    let Err(mut error) = spawned.release_gate().await else {
        panic!("injected gate-write failure must return ownership");
    };
    assert!(
        error
            .group
            .terminate(GroupSignal::Kill, Duration::ZERO, WAIT)
            .await
            .reaped
    );
    f.finish(pgid).await;
    assert!(!f.directory.path().join("canary").exists());
}
