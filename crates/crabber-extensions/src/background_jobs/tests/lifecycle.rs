use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn cancel_after_spawn_withholds_the_gate_and_recovers_the_slot() {
    if !shell_available() {
        return;
    }
    let p = policy(options());
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    p.test_hooks.lock().unwrap().after_spawn = Some(Arc::new(move |_| {
        let token = token.clone();
        Box::pin(async move {
            token.cancel();
        })
    }));
    assert!(
        p.start(context(dir.path(), cancel), json!({"command":": > canary"}))
            .await
            .unwrap_err()
            .to_string()
            .ends_with("cancelled")
    );
    timeout(Duration::from_secs(3), async {
        while p.live_jobs() != 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    sleep(Duration::from_millis(500)).await;
    assert!(!dir.path().join("canary").exists());
    p.test_hooks.lock().unwrap().after_spawn = None;
    let result = p
        .start(
            context(dir.path(), CancellationToken::new()),
            json!({"command":": > positive"}),
        )
        .await
        .unwrap();
    terminal(&p, dir.path(), result["id"].as_str().unwrap()).await;
    assert!(dir.path().join("positive").exists());
    finish(&p).await;
}

async fn dropped_start(cancel_token: bool) {
    let p = policy(options());
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    {
        let entered = entered.clone();
        let release = release.clone();
        p.test_hooks.lock().unwrap().after_spawn = Some(Arc::new(move |_| {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
            })
        }));
    }
    let task = {
        let p = p.clone();
        let ctx = context(dir.path(), cancel.clone());
        tokio::spawn(async move { p.start(ctx, json!({"command":": > canary"})).await })
    };
    timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    if cancel_token {
        cancel.cancel();
    }
    release.notify_one();
    if cancel_token {
        timeout(Duration::from_secs(3), async {
            while p.live_jobs() != 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!dir.path().join("canary").exists());
    } else {
        let id = timeout(Duration::from_secs(3), async {
            loop {
                let list = p
                    .list(context(dir.path(), CancellationToken::new()), json!({}))
                    .unwrap();
                if let Some(id) = list["jobs"][0]["id"].as_str() {
                    return id.to_owned();
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(terminal(&p, dir.path(), &id).await["state"], "succeeded");
        assert!(dir.path().join("canary").exists());
    }
    assert_eq!(p.registry.lock().unwrap().starting, 0);
    finish(&p).await;
}
#[tokio::test(flavor = "multi_thread")]
async fn dropped_start_future_with_a_live_token_publishes_the_job() {
    if shell_available() {
        dropped_start(false).await;
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn dropped_start_future_with_a_cancelled_token_hides_the_job() {
    if shell_available() {
        dropped_start(true).await;
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn close_during_start_terminates_the_published_job() {
    if !shell_available() {
        return;
    }
    let p = policy(options());
    let dir = tempfile::tempdir().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    {
        let entered = entered.clone();
        let release = release.clone();
        p.test_hooks.lock().unwrap().after_spawn = Some(Arc::new(move |_| {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
            })
        }));
    }
    let start = {
        let p = p.clone();
        let ctx = context(dir.path(), CancellationToken::new());
        tokio::spawn(async move { p.start(ctx, json!({"command":": > canary"})).await })
    };
    timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    let close = {
        let p = p.clone();
        tokio::spawn(async move { p.close(Duration::from_secs(3)).await })
    };
    timeout(Duration::from_secs(3), async {
        while !p.registry.lock().unwrap().closing {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    release.notify_one();
    assert!(start.await.unwrap().is_err());
    close.await.unwrap().unwrap();
    assert!(!dir.path().join("canary").exists());
    assert!(p.registry.lock().unwrap().jobs.is_empty());
    finish(&p).await;
}
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_kills_share_the_attempt_and_owner_isolation_holds() {
    if !shell_available() {
        return;
    }
    let p = policy(options());
    let dir = tempfile::tempdir().unwrap();
    let result = p
        .start(
            context(dir.path(), CancellationToken::new()),
            json!({"command":"sleep 30"}),
        )
        .await
        .unwrap();
    let id = result["id"].as_str().unwrap();
    let mut other = context(dir.path(), CancellationToken::new());
    other.session_id = SessionId::from("other");
    assert!(
        p.status(other, json!({"id":id}))
            .unwrap_err()
            .to_string()
            .ends_with("job-not-found")
    );
    let (a, b) = tokio::join!(
        p.kill(
            context(dir.path(), CancellationToken::new()),
            json!({"id":id})
        ),
        p.kill(
            context(dir.path(), CancellationToken::new()),
            json!({"id":id})
        )
    );
    assert_eq!(a.unwrap()["state"], "killed");
    assert_eq!(b.unwrap()["state"], "killed");
    assert_eq!(p.live_jobs(), 0);
    finish(&p).await;
}
