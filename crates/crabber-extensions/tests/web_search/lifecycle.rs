use super::*;
#[tokio::test]
async fn direct_executor_requires_context_and_validates_before_backend() {
    let host = PendingHost::new();
    let tool = executor(configured(host.searcher, limits())).await;
    assert_eq!(
        tool.execute(arguments()).await.unwrap_err().to_string(),
        "tool execution failed: web search failed: context"
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_eq!(
        tool.execute_json_with_context(context(cancel), arguments())
            .await
            .unwrap_err()
            .to_string(),
        "tool execution failed: web search failed: cancelled"
    );
    assert_eq!(
        tool.execute_json_with_context(
            context(CancellationToken::new()),
            json!({"query":"x","extra":true})
        )
        .await
        .unwrap_err()
        .to_string(),
        "tool execution failed: web search input invalid: shape"
    );
    assert_eq!(
        tool.execute_json_with_context(context(CancellationToken::new()), json!({"query":"nul\0"}))
            .await
            .unwrap_err()
            .to_string(),
        "tool execution failed: web search input invalid: query"
    );
    assert_eq!(host.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test(start_paused = true)]
async fn cancel_and_deadline_drop_futures_and_free_capacity() {
    let host = PendingHost::new();
    let tool = executor(configured(host.searcher, limits())).await;
    let cancel = CancellationToken::new();
    let first = spawn_call(tool.clone(), cancel.clone());
    host.entered.notified().await;
    assert_eq!(
        tool.execute_json_with_context(context(CancellationToken::new()), arguments())
            .await
            .unwrap_err()
            .to_string(),
        "tool execution failed: web search failed: capacity"
    );
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    cancel.cancel();
    assert_eq!(
        first.await.unwrap().unwrap_err().to_string(),
        "tool execution failed: web search failed: cancelled"
    );
    assert_eq!(host.dropped.load(Ordering::SeqCst), 1);
    let t0 = tokio::time::Instant::now();
    let next = spawn_call(tool, CancellationToken::new());
    host.entered.notified().await;
    assert_eq!(
        next.await.unwrap().unwrap_err().to_string(),
        "tool execution failed: web search failed: timed_out"
    );
    assert_eq!(tokio::time::Instant::now() - t0, limits().max_wait);
    assert_eq!(host.calls.load(Ordering::SeqCst), 2);
    assert_eq!(host.dropped.load(Ordering::SeqCst), 2);
}
#[tokio::test(start_paused = true)]
async fn runtime_deadline_and_exact_deadline_reply_are_durable_failures() {
    for exact in [false, true] {
        let dropped = Arc::new(AtomicUsize::new(0));
        let host: Searcher = {
            let dropped = dropped.clone();
            Arc::new(move |r| {
                let flag = DropFlag(dropped.clone());
                Box::pin(async move {
                    let _flag = flag;
                    if exact {
                        tokio::time::sleep(r.max_wait()).await;
                        Ok(vec![record()])
                    } else {
                        std::future::pending().await
                    }
                })
            })
        };
        let store = Arc::new(MemoryStore::new());
        let provider = Arc::new(FakeProvider::scripted(vec![call(), done()]));
        let a = agent(
            Arc::new(configured(host, limits())),
            store.clone(),
            provider.clone(),
            PermissionDecision::Allow,
        );
        let run = a.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let call = snapshot_tool(&store, &session).await;
        assert_eq!(call.status, crabber::core::ToolCallStatus::Failed);
        assert_eq!(
            result_value(&call),
            json!("tool execution failed: web search failed: timed_out")
        );
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(format!("{:?}", provider.requests()[1]).contains("web search failed: timed_out"));
        a.close_extensions().await.unwrap();
    }
}
#[tokio::test(start_paused = true)]
async fn direct_reply_at_deadline_is_discarded() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let host: Searcher = {
        let dropped = dropped.clone();
        Arc::new(move |r| {
            let flag = DropFlag(dropped.clone());
            Box::pin(async move {
                let _flag = flag;
                tokio::time::sleep(r.max_wait()).await;
                Ok(vec![record()])
            })
        })
    };
    let tool = executor(configured(host, limits())).await;
    assert_eq!(
        tool.execute_json_with_context(context(CancellationToken::new()), arguments())
            .await
            .unwrap_err()
            .to_string(),
        "tool execution failed: web search failed: timed_out"
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
#[tokio::test(start_paused = true)]
async fn interrupt_drops_search_and_allows_next_run_and_close() {
    let host = PendingHost::new();
    let store = Arc::new(MemoryStore::new());
    let a = agent(
        Arc::new(configured(host.searcher, limits())),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![call(), call(), done()])),
        PermissionDecision::Allow,
    );
    let run = a.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    host.entered.notified().await;
    run.interrupt();
    assert!(matches!(
        run.done().await,
        Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
    ));
    assert_eq!(
        snapshot_tool(&store, &session).await.status,
        crabber::core::ToolCallStatus::Running
    );
    assert_eq!(host.dropped.load(Ordering::SeqCst), 1);
    let next = a.prompt(None, "next").await.unwrap();
    next.done().await.unwrap();
    assert_eq!(host.calls.load(Ordering::SeqCst), 2);
    assert_eq!(host.dropped.load(Ordering::SeqCst), 2);
    a.close_extensions().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn live_plan_blocks_close_until_cancelled_and_released() {
    let registry = Registry::new().with_close_timeout(Duration::from_secs(1));
    let host = PendingHost::new();
    let handle = registry
        .mount(Arc::new(configured(host.searcher, limits())), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::from("session"));
    let cancel = CancellationToken::new();
    let pending = spawn_call(plan.tools[0].executor.clone(), cancel.clone());
    host.entered.notified().await;
    let t0 = tokio::time::Instant::now();
    assert!(matches!(
        handle.close().await,
        Err(ExtensionError::MountCloseTimeout { .. })
    ));
    assert_eq!(tokio::time::Instant::now() - t0, Duration::from_secs(1));
    cancel.cancel();
    assert!(
        pending
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .ends_with("cancelled")
    );
    drop(plan);
    handle.close().await.unwrap();
    assert_eq!(host.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn ready_poll_completion_checks_exact_deadline_and_releases_capacity() {
    // Inject clock movement during a ready poll to exercise completion checking.
    // Existing auto-advance tests independently prove the caller wait bound.
    for elapsed in [
        Duration::from_secs(4),
        Duration::from_secs(5),
        Duration::from_secs(6),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let host: Searcher = {
            let calls = calls.clone();
            let dropped = dropped.clone();
            Arc::new(move |_| {
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                let flag = DropFlag(dropped.clone());
                Box::pin(std::future::poll_fn(move |cx| {
                    let _keep_alive = &flag;
                    if first {
                        let mut jump = Box::pin(tokio::time::advance(elapsed));
                        let _ = jump.as_mut().poll(cx);
                    }
                    Poll::Ready(Ok(vec![record()]))
                }))
            })
        };
        let tool = executor(configured(host, limits())).await;
        let reply = tool
            .execute_json_with_context(context(CancellationToken::new()), arguments())
            .await;
        if elapsed < limits().max_wait {
            assert_eq!(reply.unwrap(), json!({"results": [record()]}));
        } else {
            assert_eq!(
                reply.unwrap_err().to_string(),
                "tool execution failed: web search failed: timed_out"
            );
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(
            tool.execute_json_with_context(context(CancellationToken::new()), arguments())
                .await
                .unwrap(),
            json!({"results": [record()]})
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn cancellation_during_ready_poll_takes_precedence_over_success() {
    let cancel = CancellationToken::new();
    let host: Searcher = {
        let cancel = cancel.clone();
        Arc::new(move |_| {
            let cancel = cancel.clone();
            Box::pin(async move {
                cancel.cancel();
                Ok(vec![record()])
            })
        })
    };
    let tool = executor(configured(host, limits())).await;
    assert_eq!(
        tool.execute_json_with_context(context(cancel), arguments())
            .await
            .unwrap_err()
            .to_string(),
        "tool execution failed: web search failed: cancelled"
    );
}
