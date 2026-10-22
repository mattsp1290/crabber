use super::*;
#[tokio::test]
async fn successful_results_are_durable_and_reenter_model_with_authoritative_context() {
    let mut invalid = record();
    invalid.url = "relative".into();
    let caps = Source {
        title: "\u{1}".repeat(16),
        snippet: "\u{2}".repeat(32),
        url: format!("https://example.test/{}", "a".repeat(107)),
    };
    let mut repaired = vec![record()];
    for url in [
        "HTTP://example.test/",
        "https://example.test/%zz",
        "http:///missing-host",
    ] {
        repaired.push(Source {
            url: url.into(),
            ..record()
        });
    }
    for (records, expected, l) in [
        (vec![record()], json!({"results":[record()]}), limits()),
        (vec![], json!({"results":[]}), limits()),
        (
            vec![invalid, record(), record()],
            json!({"results":[record()]}),
            limits(),
        ),
        (vec![caps.clone()], json!({"results":[caps]}), limits()),
        (
            repaired,
            json!({"results":[record()]}),
            Limits {
                max_results: 4,
                ..limits()
            },
        ),
    ] {
        let seen = Arc::new(std::sync::Mutex::new(None));
        let callback = searcher(records);
        let host: Searcher = {
            let seen = seen.clone();
            Arc::new(move |request| {
                *seen.lock().unwrap() = Some(request.clone());
                callback(request)
            })
        };
        let store = Arc::new(MemoryStore::new());
        let provider = Arc::new(FakeProvider::scripted(vec![call(), done()]));
        let bound = l.worst_case_result_bytes();
        let agent = agent(
            Arc::new(configured(host, l)),
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
        assert_eq!(call.arguments, arguments());
        let request = seen.lock().unwrap().take().unwrap();
        assert_eq!(request.session_id(), &session);
        assert_eq!(request.run_id(), &call.run_id);
        assert_eq!(request.call_id(), &call.id);
        assert_eq!(request.workspace().workspace_id(), Some("workspace"));
        assert_eq!(request.workspace().directory(), Some("/workspace"));
        assert_eq!(request.query(), "synthetic bounded search");
        assert_eq!(request.max_wait(), limits().max_wait);
        let encoded = expected.to_string();
        assert!(encoded.len() <= bound);
        let expected_text = format!("{encoded:?}");
        assert!(
            format!("{:?}", provider.requests()[1])
                .contains(&expected_text[1..expected_text.len() - 1])
        );
        agent.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn permission_and_input_failures_never_invoke_backend() {
    let calls = Arc::new(AtomicUsize::new(0));
    let host: Searcher = {
        let calls = calls.clone();
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(vec![]) })
        })
    };
    let mut cases = vec![
        (arguments(), None, PermissionDecision::Deny),
        (arguments(), None, PermissionDecision::Ask),
    ];
    for (args, code) in [
        (json!({}), None),
        (json!({"query":3}), None),
        (json!({"query":"x","extra":true}), None),
        (json!({"query":""}), Some("query")),
        (json!({"query":" \t"}), Some("query")),
        // Newer Crabber rejects unstorable NUL arguments before the
        // extension; failure and zero backend calls remain authoritative.
        (json!({"query":"nul\0"}), None),
        (
            json!({"query":format!("  {}  ","x".repeat(101))}),
            Some("query"),
        ),
    ] {
        cases.push((args, code, PermissionDecision::Allow));
    }
    for (args, code, decision) in cases {
        let store = Arc::new(MemoryStore::new());
        let a = agent(
            Arc::new(configured(host.clone(), limits())),
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![call_with(args), done()])),
            decision,
        );
        let run = a.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let call = snapshot_tool(&store, &session).await;
        if matches!(decision, PermissionDecision::Ask) {
            assert_eq!(call.status, crabber::core::ToolCallStatus::Pending);
            assert!(call.result.is_none());
            a.close_extensions().await.unwrap();
            continue;
        }
        assert_eq!(call.status, crabber::core::ToolCallStatus::Failed);
        if let Some(code) = code {
            assert_eq!(
                result_value(&call),
                json!(format!(
                    "tool execution failed: web search input invalid: {code}"
                ))
            );
        } else {
            assert!(result_value(&call).is_string());
        }
        a.close_extensions().await.unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn utf8_query_boundary_is_checked_after_trimming() {
    let exact = "é".repeat(50);
    let seen = Arc::new(std::sync::Mutex::new(String::new()));
    let host: Searcher = {
        let seen = seen.clone();
        Arc::new(move |r| {
            *seen.lock().unwrap() = r.query().into();
            Box::pin(async { Ok(vec![]) })
        })
    };
    let store = Arc::new(MemoryStore::new());
    let a = agent(
        Arc::new(configured(host, limits())),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            call_with(json!({"query":format!("          {exact}          ")})),
            done(),
        ])),
        PermissionDecision::Allow,
    );
    let run = a.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    assert_eq!(
        result_value(&snapshot_tool(&store, &session).await),
        json!({"results":[]})
    );
    assert_eq!(*seen.lock().unwrap(), exact);
    a.close_extensions().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn parallel_capacity_is_rejected_without_queueing() {
    let host = PendingHost::new();
    let store = Arc::new(MemoryStore::new());
    let mut parallel = call();
    parallel.pop();
    parallel.extend(call());
    let a = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![parallel, done()])))
        .config(agent_config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .execution_mode(crabber::runtime::ExecutionMode::Parallel { max: 2 })
        .shared_extension(Arc::new(configured(host.searcher, limits())), Scope::Global)
        .build()
        .unwrap();
    let run = a.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
    let calls = snapshot_tools(&store, &session).await;
    assert_eq!(calls.len(), 2);
    let mut values = calls
        .iter()
        .map(|c| {
            assert_eq!(c.status, crabber::core::ToolCallStatus::Failed);
            result_value(c).as_str().unwrap().to_owned()
        })
        .collect::<Vec<_>>();
    values.sort();
    assert_eq!(
        values,
        vec![
            "tool execution failed: web search failed: capacity",
            "tool execution failed: web search failed: timed_out"
        ]
    );
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    assert_eq!(host.dropped.load(Ordering::SeqCst), 1);
    a.close_extensions().await.unwrap();
}
