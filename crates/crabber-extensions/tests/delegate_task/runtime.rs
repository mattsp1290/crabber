use super::*;

struct PollPanic;
impl Future for PollPanic {
    type Output = Result<Response, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        panic!("HOST-PRIVATE-CANARY")
    }
}
#[tokio::test]
async fn all_immediate_outcomes_are_durable_and_reenter_the_model_with_authoritative_context() {
    for (reply, expected) in [
        (
            Response::Completed("synthetic result".into()),
            json!({"status":"completed","output":"synthetic result"}),
        ),
        (
            Response::Failed("failed".into()),
            json!({"status":"failed","output":"failed"}),
        ),
        (
            Response::Rejected("unknown profile".into()),
            json!({"status":"rejected","output":"unknown profile"}),
        ),
        (
            Response::Unavailable("busy".into()),
            json!({"status":"unavailable","output":"busy"}),
        ),
        (
            Response::Completed(String::new()),
            json!({"status":"completed"}),
        ),
    ] {
        let seen = Arc::new(std::sync::Mutex::new(None));
        let callback = runner(reply);
        let host: Runner = {
            let seen = seen.clone();
            Arc::new(move |request| {
                *seen.lock().unwrap() = Some(request.clone());
                callback(request)
            })
        };
        let store = Arc::new(MemoryStore::new());
        let provider = Arc::new(FakeProvider::scripted(vec![call(), done()]));
        let agent = agent(
            Arc::new(configured(host, limits())),
            store.clone(),
            provider.clone(),
            PermissionDecision::Allow,
        );
        let run = agent.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
        let call = snapshot_tool(&store, &session).await;
        assert_eq!(call.status, crabber::core::ToolCallStatus::Completed);
        assert_eq!(result_value(&call), expected);
        let request = seen.lock().unwrap().take().unwrap();
        assert_eq!(request.session_id(), &session);
        assert_eq!(request.run_id(), &call.run_id);
        assert_eq!(request.call_id(), &call.id);
        assert_eq!(request.workspace().workspace_id(), Some("workspace"));
        assert_eq!(request.workspace().directory(), Some("/workspace"));
        assert_eq!(request.task(), arguments()["task"].as_str().unwrap());
        assert_eq!(request.profile(), "read-only");
        assert_eq!(request.max_wait(), limits().max_wait);
        let expected_text = format!("{:?}", expected.to_string());
        assert!(
            format!("{:?}", provider.requests()[1])
                .contains(&expected_text[1..expected_text.len() - 1])
        );
        agent.close_extensions().await.unwrap();
    }
}

#[tokio::test]
async fn policy_deny_and_ask_do_not_invoke_runner() {
    let calls = Arc::new(AtomicUsize::new(0));
    let host: Runner = {
        let calls = calls.clone();
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Response::Completed(String::new())) })
        })
    };
    for decision in [PermissionDecision::Deny, PermissionDecision::Ask] {
        let store = Arc::new(MemoryStore::new());
        let agent = agent(
            Arc::new(configured(host.clone(), limits())),
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![call(), done()])),
            decision,
        );
        let run = agent.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let call = snapshot_tool(&store, &session).await;
        if matches!(decision, PermissionDecision::Ask) {
            assert_eq!(call.status, crabber::core::ToolCallStatus::Pending);
            assert!(call.result.is_none());
        } else {
            assert_eq!(call.status, crabber::core::ToolCallStatus::Failed);
            assert!(result_value(&call).is_string());
        }
        agent.close_extensions().await.unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn runtime_input_validation_and_profile_boundaries() {
    let calls = Arc::new(AtomicUsize::new(0));
    let host: Runner = {
        let calls = calls.clone();
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Response::Completed(String::new())) })
        })
    };
    let cases = [
        (json!({"task":"x"}), None),
        (json!({"task":3,"profile":"a"}), None),
        (json!({"task":"x","profile":"a","extra":true}), None),
        (json!({"task":"x".repeat(101),"profile":"a"}), Some("task")),
        (json!({"task":" \t","profile":"a"}), Some("task")),
        // Crabber may reject NUL while admitting durable call arguments,
        // before the extension runs. The direct-executor test below pins
        // the extension's own error; here failed status and zero host calls
        // are the runtime contract across both Crabber revisions.
        (json!({"task":"x\0","profile":"a"}), None),
        (json!({"task":"x","profile":""}), Some("profile")),
        (json!({"task":"x","profile":"-a"}), Some("profile")),
        (json!({"task":"x","profile":"a b"}), Some("profile")),
        (json!({"task":"x","profile":"é"}), Some("profile")),
        (
            json!({"task":"x","profile":"a".repeat(51)}),
            Some("profile"),
        ),
        (json!({"task":"x","profile":"a\0"}), None),
    ];
    for (args, code) in cases {
        let store = Arc::new(MemoryStore::new());
        let agent = agent(
            Arc::new(configured(host.clone(), limits())),
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![call_with(args), done()])),
            PermissionDecision::Allow,
        );
        let run = agent.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let call = snapshot_tool(&store, &session).await;
        assert_eq!(call.status, crabber::core::ToolCallStatus::Failed);
        if let Some(code) = code {
            assert_eq!(
                result_value(&call),
                json!(format!(
                    "tool execution failed: delegate task input invalid: {code}"
                ))
            );
        }
        agent.close_extensions().await.unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let tool = executor(configured(host, limits())).await;
    for profile in ["a", "read-only", "team/ci:v1.2_x"] {
        assert_eq!(
            tool.execute_json_with_context(
                context(CancellationToken::new()),
                json!({"task":"x","profile":profile})
            )
            .await
            .unwrap(),
            json!({"status":"completed"})
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let tool = executor(configured(
        runner(Response::Completed(String::new())),
        Limits {
            max_profile_bytes: 256,
            ..limits()
        },
    ))
    .await;
    assert!(
        tool.execute_json_with_context(
            context(CancellationToken::new()),
            json!({"task":"x","profile":"a".repeat(257)})
        )
        .await
        .unwrap_err()
        .to_string()
        .ends_with("profile")
    );
}

#[tokio::test]
async fn direct_executor_rejects_missing_context_precancellation_shape_and_nul() {
    let calls = Arc::new(AtomicUsize::new(0));
    let host: Runner = {
        let calls = calls.clone();
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Response::Completed(String::new())) })
        })
    };
    let tool = executor(configured(host, limits())).await;
    assert_eq!(
        tool.execute(arguments()).await.unwrap_err().to_string(),
        "tool execution failed: delegate task failed: context"
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_eq!(
        tool.execute_json_with_context(context(cancel), arguments())
            .await
            .unwrap_err()
            .to_string(),
        "tool execution failed: delegate task failed: cancelled"
    );
    for (args, code) in [
        (json!({"task":"x","profile":"a","extra":true}), "shape"),
        (json!({"task":"x\0","profile":"a"}), "task"),
        (json!({"task":"x","profile":"a\0"}), "profile"),
    ] {
        assert_eq!(
            tool.execute_json_with_context(context(CancellationToken::new()), args)
                .await
                .unwrap_err()
                .to_string(),
            format!("tool execution failed: delegate task input invalid: {code}")
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn runner_faults_are_one_sanitized_durable_failure_and_release_capacity() {
    let cases: Vec<Runner> = vec![
        Arc::new(|_| Box::pin(async { Err(ExtensionError::Tool("HOST-PRIVATE-CANARY".into())) })),
        Arc::new(|_| panic!("HOST-PRIVATE-CANARY")),
        Arc::new(|_| {
            Box::pin(async {
                tokio::task::yield_now().await;
                PollPanic.await
            })
        }),
        runner(Response::Completed("x".repeat(101))),
        runner(Response::Failed("nul\0output".into())),
    ];
    for host in cases {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted: Runner = {
            let calls = calls.clone();
            Arc::new(move |request| {
                calls.fetch_add(1, Ordering::SeqCst);
                host(request)
            })
        };
        let store = Arc::new(MemoryStore::new());
        let provider = Arc::new(FakeProvider::scripted(vec![call(), done(), call(), done()]));
        let agent = agent(
            Arc::new(configured(counted, limits())),
            store.clone(),
            provider.clone(),
            PermissionDecision::Allow,
        );
        for _ in 0..2 {
            let mut run = agent.prompt(None, "fixture").await.unwrap();
            let session = run.session_id().clone();
            let mut events = run.events();
            run.done().await.unwrap();
            let mut event_text = String::new();
            while let Ok(Some(event)) = events.recv().await {
                event_text.push_str(&format!("{event:?}"));
            }
            let call = snapshot_tool(&store, &session).await;
            assert_eq!(call.status, crabber::core::ToolCallStatus::Failed);
            assert_eq!(
                result_value(&call),
                json!("tool execution failed: delegate task failed: runner")
            );
            assert!(!event_text.contains("HOST-PRIVATE-CANARY"));
            assert!(!format!("{:?}", provider.requests()).contains("HOST-PRIVATE-CANARY"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        agent.close_extensions().await.unwrap();
    }
}

#[tokio::test]
async fn result_at_byte_limit_survives_json_escaping_and_next_model_input() {
    let output = "\u{1}".repeat(limits().max_result_bytes);
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![call(), done()]));
    let agent = agent(
        Arc::new(extension(Response::Completed(output.clone()))),
        store.clone(),
        provider.clone(),
        PermissionDecision::Allow,
    );
    let run = agent.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let call = snapshot_tool(&store, &session).await;
    assert_eq!(call.status, crabber::core::ToolCallStatus::Completed);
    assert_eq!(result_value(&call)["output"], output);
    assert!(
        format!("{:?}", provider.requests()[1])
            .contains(&"\\\\u0001".repeat(limits().max_result_bytes))
    );
    agent.close_extensions().await.unwrap();
}

struct CooperativeDropPanic {
    future: Pin<Box<dyn Future<Output = Result<Response, ExtensionError>> + Send>>,
    dropped: Arc<AtomicUsize>,
}
impl Future for CooperativeDropPanic {
    type Output = Result<Response, ExtensionError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}
impl Drop for CooperativeDropPanic {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
        panic!("HOST-PRIVATE-CANARY");
    }
}

#[tokio::test(start_paused = true)]
async fn cooperative_future_drop_panic_preserves_timeout_and_frees_slot() {
    let calls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let host: Runner = {
        let calls = calls.clone();
        let dropped = dropped.clone();
        Arc::new(move |request| {
            if calls.fetch_add(1, Ordering::SeqCst) > 0 {
                return Box::pin(async { Ok(Response::Completed("next".into())) });
            }
            Box::pin(CooperativeDropPanic {
                future: Box::pin(async move {
                    request.cancellation().cancelled().await;
                    Ok(Response::Failed("cancelled".into()))
                }),
                dropped: dropped.clone(),
            })
        })
    };
    let ext = Arc::new(configured(host, limits()));
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![call(), done(), call(), done()]));
    let agent = agent(ext, store.clone(), provider, PermissionDecision::Allow);
    let run = agent.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let call = snapshot_tool(&store, &session).await;
    assert_eq!(call.status, crabber::core::ToolCallStatus::Completed);
    assert_eq!(result_value(&call), json!({"status":"timed_out"}));
    for _ in 0..10 {
        if dropped.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let run = agent.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    assert_eq!(
        result_value(&snapshot_tool(&store, &session).await),
        json!({"status":"completed","output":"next"})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn exact_utf8_task_and_hard_profile_byte_boundaries_use_public_runtime() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let host: Runner = {
        let calls = calls.clone();
        let seen = seen.clone();
        Arc::new(move |request| {
            calls.fetch_add(1, Ordering::SeqCst);
            seen.lock()
                .unwrap()
                .push((request.task().to_owned(), request.profile().to_owned()));
            Box::pin(async { Ok(Response::Completed(String::new())) })
        })
    };
    let exact_task = "é".repeat(50);
    let exact_profile = "a".repeat(256);
    for (task, profile, error) in [
        (exact_task.clone(), "a".into(), None),
        (format!("{exact_task}a"), "a".into(), Some("task")),
        ("x".into(), exact_profile.clone(), None),
        ("x".into(), format!("{exact_profile}a"), Some("profile")),
    ] {
        let store = Arc::new(MemoryStore::new());
        let provider = Arc::new(FakeProvider::scripted(vec![
            call_with(json!({"task":task,"profile":profile})),
            done(),
        ]));
        let agent = agent(
            Arc::new(configured(
                host.clone(),
                Limits {
                    max_profile_bytes: 256,
                    ..limits()
                },
            )),
            store.clone(),
            provider,
            PermissionDecision::Allow,
        );
        let before = calls.load(Ordering::SeqCst);
        let run = agent.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let call = snapshot_tool(&store, &session).await;
        if let Some(code) = error {
            assert_eq!(call.status, crabber::core::ToolCallStatus::Failed);
            assert_eq!(
                result_value(&call),
                json!(format!(
                    "tool execution failed: delegate task input invalid: {code}"
                ))
            );
            assert_eq!(calls.load(Ordering::SeqCst), before);
        } else {
            assert_eq!(call.status, crabber::core::ToolCallStatus::Completed);
            assert_eq!(result_value(&call), json!({"status":"completed"}));
            assert_eq!(calls.load(Ordering::SeqCst), before + 1);
            assert_eq!(seen.lock().unwrap().last().unwrap(), &(task, profile));
        }
        agent.close_extensions().await.unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
