use super::*;
#[tokio::test(flavor = "multi_thread")]
async fn committed_result_lost_after_send_resets_before_next_call() {
    use std::{future::Future, task::Poll};
    for drop_caller in [false, true] {
        let Some(f) = Fixture::new(|_| {}) else {
            return;
        };
        f.execute(key("one"), "x=1").await.unwrap();
        // Receiving the result can precede the tracked task's after-send hook.
        // Finish the seed task before installing the next call's barrier.
        timeout(Duration::from_secs(5), async {
            while f.manager.cleanup.tracker().pending() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("seed execution must finish before installing the next hook");
        let before_send = Barrier::new();
        let after_send = Barrier::new();
        {
            let mut hooks = f.manager.hooks.lock().unwrap();
            hooks.before_send = Some(before_send.hook());
            hooks.after_send = Some(after_send.hook());
        }
        let cancel = CancellationToken::new();
        let mut call = Box::pin(f.manager.execute_owner(
            key("one"),
            f.root.clone(),
            cancel.clone(),
            "x=2".into(),
            Duration::from_secs(10),
        ));
        // Admit once, then hold the caller unpolled while the tracked task sends.
        std::future::poll_fn(|cx| {
            assert!(call.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        before_send.entered().await;
        before_send.release();
        after_send.entered().await;
        if drop_caller {
            drop(call);
        } else {
            cancel.cancel();
            code(&call.await.unwrap_err(), "cancelled");
        }
        {
            let mut hooks = f.manager.hooks.lock().unwrap();
            hooks.before_send = None;
            hooks.after_send = None;
        }
        after_send.release();
        let next = f.execute(key("one"), "'x' in globals()").await.unwrap();
        assert_eq!(next.result.text, "False");
        assert_eq!(next.generation, 1);
        assert_eq!(next.state_reset_reason, "canceled");
        f.close().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_boundaries_and_dropped_receiver_reset() {
    for existing in [false, true] {
        for boundary in [
            "before_write",
            "after_write",
            "before_commit",
            "before_send",
        ] {
            let Some(f) = Fixture::new(|_| {}) else {
                return;
            };
            if existing {
                f.execute(key("one"), "x=1").await.unwrap();
            }
            let barrier = Barrier::new();
            {
                let mut h = f.manager.hooks.lock().unwrap();
                match boundary {
                    "before_write" => h.before_request_write = Some(barrier.hook()),
                    "after_write" => h.after_request_write = Some(barrier.hook()),
                    "before_commit" => h.before_commit = Some(barrier.hook()),
                    _ => h.before_send = Some(barrier.hook()),
                }
            }
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
                        "x=2".into(),
                        Duration::from_secs(10),
                    )
                    .await
            });
            barrier.entered().await;
            if boundary == "before_send" {
                task.abort();
                let _ = task.await;
            } else {
                cancel.cancel();
                code(&task.await.unwrap().unwrap_err(), "cancelled");
            }
            {
                let mut h = f.manager.hooks.lock().unwrap();
                h.before_request_write = None;
                h.after_request_write = None;
                h.before_commit = None;
                h.before_send = None;
            }
            barrier.release();
            let next = f.execute(key("one"), "'x' in globals()").await.unwrap();
            assert_eq!(next.result.text, "False", "{boundary}");
            let reset = existing || boundary == "before_send";
            assert_eq!(next.state_reset, reset, "{boundary}");
            assert_eq!(next.generation, u64::from(reset));
            if reset {
                assert_eq!(next.state_reset_reason, "canceled");
            }
            f.close().await;
        }
    }
}
