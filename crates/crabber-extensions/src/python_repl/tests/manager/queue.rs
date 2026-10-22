use super::*;
#[tokio::test(flavor = "multi_thread")]
async fn queue_excludes_holder_is_fifo_and_rejects_at_bound() {
    let Some(f) = Fixture::new(|_| {}) else {
        return;
    };
    f.execute(key("one"), "order=[]").await.unwrap();
    let barrier = Barrier::new();
    f.manager.hooks.lock().unwrap().before_request_write = Some(barrier.hook());
    let manager = f.manager.clone();
    let root = f.root.clone();
    let holder = tokio::spawn(async move {
        manager
            .execute_owner(
                key("one"),
                root,
                CancellationToken::new(),
                "order.append(0)".into(),
                Duration::from_secs(10),
            )
            .await
    });
    barrier.entered().await;
    f.manager.hooks.lock().unwrap().before_request_write = None;
    let mut queued = Vec::new();
    for n in 1..=2 {
        let manager = f.manager.clone();
        let root = f.root.clone();
        queued.push(tokio::spawn(async move {
            manager
                .execute_owner(
                    key("one"),
                    root,
                    CancellationToken::new(),
                    format!("order.append({n})"),
                    Duration::from_secs(10),
                )
                .await
        }));
        timeout(Duration::from_secs(5), async {
            while f.manager.queued(&key("one")) != n {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    code(
        f.execute(key("one"), "order.append(3)").await.unwrap_err(),
        "queue-full",
    );
    barrier.release();
    holder.await.unwrap().unwrap();
    for task in queued {
        task.await.unwrap().unwrap();
    }
    assert_eq!(
        f.execute(key("one"), "order").await.unwrap().result.text,
        "[0, 1, 2]"
    );
    assert_eq!(f.manager.queued(&key("one")), 0);
    f.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn queued_cancellation_never_reaches_python() {
    for drop_caller in [false, true] {
        let Some(f) = Fixture::new(|_| {}) else {
            return;
        };
        f.execute(key("one"), "x=1").await.unwrap();
        let barrier = Barrier::new();
        f.manager.hooks.lock().unwrap().before_request_write = Some(barrier.hook());
        let manager = f.manager.clone();
        let root = f.root.clone();
        let holder = tokio::spawn(async move {
            manager
                .execute_owner(
                    key("one"),
                    root,
                    CancellationToken::new(),
                    "x=2".into(),
                    Duration::from_secs(10),
                )
                .await
        });
        barrier.entered().await;
        f.manager.hooks.lock().unwrap().before_request_write = None;
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let manager = f.manager.clone();
        let root = f.root.clone();
        let task = tokio::spawn(async move {
            manager
                .execute_owner(
                    key("one"),
                    root,
                    token,
                    "x=3".into(),
                    Duration::from_secs(10),
                )
                .await
        });
        timeout(Duration::from_secs(5), async {
            while f.manager.queued(&key("one")) != 1 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        if drop_caller {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            cancel.cancel();
            code(task.await.unwrap().unwrap_err(), "cancelled");
        }
        timeout(Duration::from_secs(5), async {
            while f.manager.queued(&key("one")) != 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        barrier.release();
        holder.await.unwrap().unwrap();
        assert_eq!(f.execute(key("one"), "x").await.unwrap().result.text, "2");
        f.close().await;
    }
}
