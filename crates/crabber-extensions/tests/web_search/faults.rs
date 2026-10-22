use super::*;
struct PollPanic;
impl Future for PollPanic {
    type Output = Result<Vec<Source>, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        panic!("HOST-PRIVATE-CANARY")
    }
}
#[tokio::test]
async fn searcher_faults_are_one_sanitized_durable_failure_and_release_capacity() {
    let cases: Vec<Searcher> = vec![
        Arc::new(|_| Box::pin(async { Err(ExtensionError::Tool("HOST-PRIVATE-CANARY".into())) })),
        Arc::new(|_| panic!("HOST-PRIVATE-CANARY")),
        Arc::new(|_| {
            Box::pin(async {
                tokio::task::yield_now().await;
                PollPanic.await
            })
        }),
    ];
    for host in cases {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted: Searcher = {
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
                json!("tool execution failed: web search failed: searcher")
            );
            assert!(!event_text.contains("HOST-PRIVATE-CANARY"));
            assert!(!format!("{:?}", provider.requests()).contains("HOST-PRIVATE-CANARY"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        agent.close_extensions().await.unwrap();
    }
}

struct DropPanic(Arc<AtomicUsize>);
impl Future for DropPanic {
    type Output = Result<Vec<Source>, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for DropPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("HOST-PRIVATE-CANARY");
    }
}
#[tokio::test(start_paused = true)]
async fn drop_panic_preserves_timeout_and_releases_capacity() {
    let calls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let host: Searcher = {
        let calls = calls.clone();
        let dropped = dropped.clone();
        Arc::new(move |_| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Box::pin(DropPanic(dropped.clone()))
            } else {
                Box::pin(async { Ok(vec![record()]) })
            }
        })
    };
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![call(), done(), call(), done()]));
    let a = agent(
        Arc::new(configured(host, limits())),
        store.clone(),
        provider.clone(),
        PermissionDecision::Allow,
    );
    for expected in [
        json!("tool execution failed: web search failed: timed_out"),
        json!({"results":[record()]}),
    ] {
        let run = a.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        assert_eq!(
            result_value(&snapshot_tool(&store, &session).await),
            expected
        );
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(!format!("{:?}", provider.requests()).contains("HOST-PRIVATE-CANARY"));
    a.close_extensions().await.unwrap();
}
