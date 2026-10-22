use super::*;

#[tokio::test(start_paused = true)]
async fn timed_out_runner_keeps_slot_until_exit_and_child_token_is_independent() {
    let mut host = PendingHost::new(false);
    let tool = executor(configured(host.runner.clone(), limits())).await;
    let cancel = CancellationToken::new();
    let t0 = tokio::time::Instant::now();
    let first = spawn_call(tool.clone(), cancel.clone());
    host.entered(1).await;
    assert_eq!(first.await.unwrap().unwrap(), json!({"status":"timed_out"}));
    assert_eq!(tokio::time::Instant::now() - t0, limits().max_wait);
    assert!(host.seen.lock().unwrap()[0].cancellation().is_cancelled());
    assert!(!cancel.is_cancelled());
    assert_eq!(
        tool.execute_json_with_context(context(CancellationToken::new()), arguments())
            .await
            .unwrap(),
        json!({"status":"unavailable"})
    );
    assert_eq!(host.counts.borrow().0, 1);
    host.release.notify_one();
    host.exited(1).await;
    assert_eq!(host.dropped.load(Ordering::SeqCst), 1);
    let next = spawn_call(tool.clone(), CancellationToken::new());
    host.entered(2).await;
    host.release.notify_one();
    assert_eq!(
        next.await.unwrap().unwrap(),
        json!({"status":"completed","output":"released"})
    );
}

#[tokio::test(start_paused = true)]
async fn cancellation_joins_cooperative_runner_and_frees_capacity() {
    let mut host = PendingHost::new(true);
    let tool = executor(configured(host.runner.clone(), limits())).await;
    for n in 1..=2 {
        let cancel = CancellationToken::new();
        let first = spawn_call(tool.clone(), cancel.clone());
        host.entered(n).await;
        cancel.cancel();
        assert_eq!(
            first.await.unwrap().unwrap_err().to_string(),
            "tool execution failed: delegate task failed: cancelled"
        );
        host.exited(n).await;
        assert!(
            host.seen.lock().unwrap()[n - 1]
                .cancellation()
                .is_cancelled()
        );
        assert_eq!(host.dropped.load(Ordering::SeqCst), n);
    }
}

#[tokio::test(start_paused = true)]
async fn runtime_deadline_and_exact_deadline_reply_are_completed_timeouts() {
    for exact in [false, true] {
        let mut host = PendingHost::new(true);
        let runner = if exact {
            Arc::new(|request: Request| {
                Box::pin(async move {
                    tokio::time::sleep(request.max_wait()).await;
                    Ok(Response::Completed("late".into()))
                })
                    as Pin<Box<dyn Future<Output = Result<Response, ExtensionError>> + Send>>
            }) as Runner
        } else {
            host.runner.clone()
        };
        let store = Arc::new(MemoryStore::new());
        let provider = Arc::new(FakeProvider::scripted(vec![call(), done()]));
        let agent = agent(
            Arc::new(configured(runner, limits())),
            store.clone(),
            provider.clone(),
            PermissionDecision::Allow,
        );
        let run = agent.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
        let call = snapshot_tool(&store, &session).await;
        assert_eq!(call.status, crabber::core::ToolCallStatus::Completed);
        assert_eq!(result_value(&call), json!({"status":"timed_out"}));
        assert!(format!("{:?}", provider.requests()[1]).contains("timed_out"));
        agent.close_extensions().await.unwrap();
        if !exact {
            host.exited(1).await;
            assert!(host.seen.lock().unwrap()[0].cancellation().is_cancelled());
            assert_eq!(host.dropped.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn parallel_runtime_capacity_settles_both_calls_with_one_invocation() {
    let mut host = PendingHost::new(true);
    let mut calls = call();
    calls.pop();
    calls.extend(call());
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![calls, done()]));
    let agent = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store.clone())
        .provider(provider)
        .config(agent_config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(
            Arc::new(configured(host.runner.clone(), limits())),
            Scope::Global,
        )
        .execution_mode(crabber::runtime::ExecutionMode::Parallel { max: 2 })
        .build()
        .unwrap();
    let run = agent.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session,
            limits: SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 10000,
                encoded_bytes: 100000,
            },
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("snapshot")
    };
    assert_eq!(page.tool_calls.len(), 2);
    let mut statuses: Vec<_> = page
        .tool_calls
        .iter()
        .map(|call| {
            assert_eq!(call.status, crabber::core::ToolCallStatus::Completed);
            result_value(call)["status"].as_str().unwrap().to_owned()
        })
        .collect();
    statuses.sort();
    assert_eq!(statuses, ["timed_out", "unavailable"]);
    agent.close_extensions().await.unwrap();
    host.exited(1).await;
    assert_eq!(host.counts.borrow().0, 1);
}

#[tokio::test(start_paused = true)]
async fn mount_shutdown_waits_for_exit_or_grace_and_retains_detached_capacity() {
    for mode in 0..3 {
        let mut host = PendingHost::new(mode == 0);
        let registry = Registry::new().with_close_timeout(Duration::from_secs(1));
        let handle = registry
            .mount(
                Arc::new(configured(host.runner.clone(), limits())),
                Scope::Global,
            )
            .await
            .unwrap();
        let plan = registry.acquire(&SessionId::from("session"));
        let tool = plan.tools[0].executor.clone();
        let first = spawn_call(tool.clone(), CancellationToken::new());
        host.entered(1).await;
        assert_eq!(first.await.unwrap().unwrap(), json!({"status":"timed_out"}));
        if mode == 0 {
            host.exited(1).await;
        }
        drop(plan);
        if mode == 1 {
            let release = host.release.clone();
            tokio::spawn(async move {
                tokio::time::sleep(limits().shutdown_grace / 2).await;
                release.notify_one();
            });
        }
        let t0 = tokio::time::Instant::now();
        handle.close().await.unwrap();
        let elapsed = tokio::time::Instant::now() - t0;
        match mode {
            0 => assert!(elapsed < limits().shutdown_grace),
            1 => {
                assert!(elapsed >= limits().shutdown_grace / 2);
                assert!(elapsed < limits().shutdown_grace);
                host.exited(1).await;
            }
            _ => {
                assert!(elapsed >= limits().shutdown_grace);
                assert!(elapsed < limits().shutdown_grace + Duration::from_secs(1));
                assert_eq!(host.counts.borrow().1, 0);
                assert_eq!(
                    tool.execute_json_with_context(context(CancellationToken::new()), arguments())
                        .await
                        .unwrap(),
                    json!({"status":"unavailable"})
                );
                host.release.notify_one();
                host.exited(1).await;
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn live_plan_blocks_close_until_cancel_and_release() {
    let mut host = PendingHost::new(true);
    let registry = Registry::new().with_close_timeout(Duration::from_secs(1));
    let handle = registry
        .mount(
            Arc::new(configured(host.runner.clone(), limits())),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::from("session"));
    let cancel = CancellationToken::new();
    let first = spawn_call(plan.tools[0].executor.clone(), cancel.clone());
    host.entered(1).await;
    let t0 = tokio::time::Instant::now();
    assert!(matches!(
        handle.close().await,
        Err(ExtensionError::MountCloseTimeout { .. })
    ));
    assert_eq!(tokio::time::Instant::now() - t0, Duration::from_secs(1));
    cancel.cancel();
    first.await.unwrap().unwrap_err();
    host.exited(1).await;
    drop(plan);
    let t0 = tokio::time::Instant::now();
    handle.close().await.unwrap();
    assert!(tokio::time::Instant::now() - t0 < limits().shutdown_grace);
}

#[tokio::test(start_paused = true)]
async fn shared_instance_shutdown_waits_for_other_mounts_runner() {
    let mut host = PendingHost::new(true);
    let ext = Arc::new(configured(
        host.runner.clone(),
        Limits {
            max_in_flight: 2,
            ..limits()
        },
    ));
    let a = Registry::new().with_close_timeout(Duration::from_secs(1));
    let b = Registry::new().with_close_timeout(Duration::from_secs(1));
    let ha = a.mount(ext.clone(), Scope::Global).await.unwrap();
    let hb = b.mount(ext, Scope::Global).await.unwrap();
    let plan = b.acquire(&SessionId::from("session"));
    let cancel = CancellationToken::new();
    let first = spawn_call(plan.tools[0].executor.clone(), cancel.clone());
    host.entered(1).await;
    tokio::spawn(async move {
        tokio::time::sleep(limits().shutdown_grace / 2).await;
        cancel.cancel();
    });
    let t0 = tokio::time::Instant::now();
    ha.close().await.unwrap();
    let elapsed = tokio::time::Instant::now() - t0;
    assert!(elapsed >= limits().shutdown_grace / 2);
    assert!(elapsed < limits().shutdown_grace);
    first.await.unwrap().unwrap_err();
    host.exited(1).await;
    drop(plan);
    hb.close().await.unwrap();
}

#[tokio::test]
async fn runtime_interrupt_cancels_runner_and_next_run_is_admitted() {
    let mut host = PendingHost::new(true);
    let store = Arc::new(MemoryStore::new());
    // Interrupted calls never consume the following provider turn.
    let provider = Arc::new(FakeProvider::scripted(vec![call(), call()]));
    let agent = agent(
        Arc::new(configured(host.runner.clone(), limits())),
        store.clone(),
        provider,
        PermissionDecision::Allow,
    );
    for n in 1..=2 {
        let run = agent.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        host.entered(n).await;
        run.interrupt();
        assert!(matches!(
            run.done().await,
            Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
        ));
        host.exited(n).await;
        assert!(
            host.seen.lock().unwrap()[n - 1]
                .cancellation()
                .is_cancelled()
        );
        assert_eq!(host.dropped.load(Ordering::SeqCst), n);
        assert_eq!(
            snapshot_tool(&store, &session).await.status,
            crabber::core::ToolCallStatus::Running
        );
    }
    agent.close_extensions().await.unwrap();
}
