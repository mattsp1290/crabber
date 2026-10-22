use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn failed_natural_sweep_keeps_capacity_until_an_explicit_retry_succeeds() {
    if !shell_available() {
        return;
    }
    let mut limits = options();
    limits.limits.max_running = 1;
    let p = policy(limits);
    let dir = tempfile::tempdir().unwrap();
    let (failure, signals) = {
        let hooks = p.test_hooks.lock().unwrap();
        (hooks.fail_kill.clone(), hooks.signals.clone())
    };
    failure.store(true, std::sync::atomic::Ordering::SeqCst);
    let result = p.start(
        context(dir.path(), CancellationToken::new()),
        json!({"command":"sleep 100 & : > ready; while [ ! -f go ]; do sleep 0.01; done; exit 7"}),
    ).await.unwrap();
    let id = result["id"].as_str().unwrap();
    let job = p.registry.lock().unwrap().jobs[id].clone();
    struct RetryOnDrop(Arc<std::sync::atomic::AtomicBool>, Arc<Job>);
    impl Drop for RetryOnDrop {
        fn drop(&mut self) {
            self.0.store(false, std::sync::atomic::Ordering::SeqCst);
            self.1.request_attempt();
        }
    }
    let _cleanup = RetryOnDrop(failure.clone(), job.clone());
    timeout(Duration::from_secs(3), async {
        while !dir.path().join("ready").exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    std::fs::write(dir.path().join("go"), "").unwrap();
    let mut attempt = job.attempt.subscribe();
    timeout(
        Duration::from_secs(3),
        attempt.wait_for(|a| a.last == Some(job::AttemptResult::Incomplete)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(p.live_jobs(), 1);
    assert_eq!(job.state(), State::Running);
    assert!(
        p.start(
            context(dir.path(), CancellationToken::new()),
            json!({"command":"exit 0"})
        )
        .await
        .unwrap_err()
        .to_string()
        .ends_with("capacity-exhausted")
    );
    assert!(
        p.kill(
            context(dir.path(), CancellationToken::new()),
            json!({"id":id})
        )
        .await
        .unwrap_err()
        .to_string()
        .ends_with("termination-incomplete")
    );
    assert_eq!(p.live_jobs(), 1);
    assert_eq!(
        p.status(
            context(dir.path(), CancellationToken::new()),
            json!({"id":id})
        )
        .unwrap()["state"],
        "running"
    );
    let pgid = p.test_hooks.lock().unwrap().pgids[0];
    assert!(!group_members(pgid).is_empty());
    let count = signals.lock().unwrap().len();
    sleep(Duration::from_millis(100)).await;
    assert_eq!(
        signals.lock().unwrap().len(),
        count,
        "cached exit must not cause automatic retries"
    );
    failure.store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        p.kill(
            context(dir.path(), CancellationToken::new()),
            json!({"id":id})
        )
        .await
        .unwrap()["state"],
        "failed"
    );
    assert_eq!(
        p.status(
            context(dir.path(), CancellationToken::new()),
            json!({"id":id})
        )
        .unwrap()["exit_code"],
        7
    );
    assert_eq!(p.live_jobs(), 0);
    finish(&p).await;
}
