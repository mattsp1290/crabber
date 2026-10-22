use super::*;
#[tokio::test(flavor = "multi_thread")]
async fn closerange_fails_and_exited_leader_stays_zombie() {
    for code in [
        "import os; os.closerange(3, 1024)",
        "import os; os._exit(3)",
    ] {
        let Some(mut runner) = real().await else {
            return;
        };
        let id = runner.child.pgid().as_raw_nonzero().get();
        assert!(matches!(
            runner
                .execute(
                    code,
                    &CancellationToken::new(),
                    &CancellationToken::new(),
                    Instant::now() + WAIT
                )
                .await,
            ExecuteOutcome::Failed {
                may_have_executed: true
            }
        ));
        tokio::time::timeout(WAIT, async {
            while !runner.child.leader_exited() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            group_members(id)
                .iter()
                .any(|(pid, state)| *pid == id && state.starts_with('Z'))
        );
        stop(&mut runner).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn forked_child_does_not_hide_runner_death() {
    let Some(mut runner) = real().await else {
        return;
    };
    let now = Instant::now();
    assert!(matches!(
        runner
            .execute(
                "import os,time\nif os.fork() == 0: time.sleep(60); os._exit(0)\nos._exit(1)",
                &CancellationToken::new(),
                &CancellationToken::new(),
                now + Duration::from_secs(30)
            )
            .await,
        ExecuteOutcome::Failed {
            may_have_executed: true
        }
    ));
    assert!(now.elapsed() < WAIT);
    stop(&mut runner).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn term_honoring_ends_early_and_ignoring_requires_kill() {
    for ignore in [false, true] {
        let Some(mut runner) = real().await else {
            return;
        };
        if ignore {
            assert!(matches!(runner.execute("import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)", &CancellationToken::new(), &CancellationToken::new(), Instant::now() + Duration::from_millis(200)).await, ExecuteOutcome::TimedOut { may_have_executed: true }));
        }
        let now = Instant::now();
        let grace = if ignore {
            GRACE
        } else {
            Duration::from_secs(5)
        };
        assert!(runner.terminate(grace, WAIT).await.reaped);
        if ignore {
            assert!(now.elapsed() >= GRACE);
        } else {
            assert!(now.elapsed() < grace / 2);
        }
        group_gone(runner.child.pgid().as_raw_nonzero().get()).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_terminate_keeps_term_deadline() {
    let Some(mut runner) = real().await else {
        return;
    };
    assert!(matches!(
        runner
            .execute(
                "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)",
                &CancellationToken::new(),
                &CancellationToken::new(),
                Instant::now() + Duration::from_millis(200)
            )
            .await,
        ExecuteOutcome::TimedOut {
            may_have_executed: true
        }
    ));
    let grace = Duration::from_secs(3);
    let now = Instant::now();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), runner.terminate(grace, WAIT))
            .await
            .is_err()
    );
    assert!(runner.terminate(grace, WAIT).await.reaped);
    let elapsed = now.elapsed();
    assert!(elapsed >= grace);
    assert!(elapsed < grace + Duration::from_millis(750));
    group_gone(runner.child.pgid().as_raw_nonzero().get()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_death_and_multiprocessing_fork_are_detected() {
    for code in [
        "import os,time\nif os.fork() == 0: time.sleep(60); os._exit(0)\ntime.sleep(.1); os._exit(1)",
        "import multiprocessing,os; pool = multiprocessing.get_context('fork').Pool(1); os._exit(1)",
    ] {
        let Some(mut runner) = real().await else {
            return;
        };
        assert!(matches!(
            runner
                .execute(
                    code,
                    &CancellationToken::new(),
                    &CancellationToken::new(),
                    Instant::now() + WAIT
                )
                .await,
            ExecuteOutcome::Failed {
                may_have_executed: true
            }
        ));
        stop(&mut runner).await;
    }
    let Some(mut runner) = real().await else {
        return;
    };
    response(&mut runner, "import threading,os,time; threading.Thread(target=lambda: (time.sleep(.1), os._exit(1))).start()").await;
    tokio::time::timeout(WAIT, async {
        while !runner.child.leader_exited() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fixture leader did not exit while idle");
    assert!(matches!(
        runner
            .execute(
                "1",
                &CancellationToken::new(),
                &CancellationToken::new(),
                Instant::now() + WAIT
            )
            .await,
        ExecuteOutcome::Failed {
            may_have_executed: false
        }
    ));
    stop(&mut runner).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_that_leaves_group_is_killed_directly() {
    let Some(mut runner) = real().await else {
        return;
    };
    // The parent's process group need not equal its PID (PTY and CI differ).
    let code = format!(
        "import os,time; os.setpgid(0, {}); time.sleep(60)",
        rustix::process::getpgrp().as_raw_nonzero().get()
    );
    assert!(matches!(
        runner
            .execute(
                &code,
                &CancellationToken::new(),
                &CancellationToken::new(),
                Instant::now() + Duration::from_millis(200)
            )
            .await,
        ExecuteOutcome::TimedOut {
            may_have_executed: true
        }
    ));
    stop(&mut runner).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn detached_descendant_is_outside_cleanup_contract() {
    let Some(mut runner) = real().await else {
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("holder");
    let code = format!(
        "import os,time,pathlib\npid = os.fork()\nif pid == 0:\n os.setsid()\n pathlib.Path({}).write_text(str(os.getpid()))\n time.sleep(60)\n os._exit(0)\nwhile not pathlib.Path({}).exists(): time.sleep(.01)\npid",
        serde_json::to_string(file.to_str().unwrap()).unwrap(),
        serde_json::to_string(file.to_str().unwrap()).unwrap()
    );
    // Always recover the holder even if a later assertion panics.
    struct Holder(std::path::PathBuf);
    impl Drop for Holder {
        fn drop(&mut self) {
            if let Ok(raw) = std::fs::read_to_string(&self.0)
                && let Some(pid) = raw
                    .parse::<i32>()
                    .ok()
                    .and_then(rustix::process::Pid::from_raw)
            {
                let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
            }
        }
    }
    let guard = Holder(file.clone());
    let result = response(&mut runner, &code).await;
    let pid: i32 = result.result.text.parse().unwrap();
    stop(&mut runner).await;
    let output = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "stat="])
        .output()
        .unwrap();
    assert!(
        !String::from_utf8(output.stdout)
            .unwrap()
            .trim()
            .starts_with('Z')
    );
    assert!(output.status.success());
    drop(guard);
}
