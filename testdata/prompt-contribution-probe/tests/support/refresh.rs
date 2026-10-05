use super::*;
use tokio::sync::Barrier;

#[tokio::test]
async fn retry_sees_changed_instruction_file_once() {
    let h = FileHarness::new(
        vec![
            error_script(ProviderErrorKind::RateLimited),
            text_script("done"),
        ],
        false,
    )
    .await;
    h.run().await;
    let requests = h.fake.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].system.as_deref(), Some("base\nstatic\nv1"));
    assert_eq!(requests[1].system.as_deref(), Some("base\nstatic\nv2"));
    let contexts = h.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[0].attempt(), 1);
    assert_eq!(contexts[1].attempt(), 2);
    assert_eq!(contexts[0].turn_id(), contexts[1].turn_id());
    assert!(!contexts[1].after_compaction());
    assert_eq!(contexts[0].provider_id(), "fake");
    assert_eq!(contexts[0].model_id(), "scripted");
}
#[tokio::test]
async fn later_turn_refreshes() {
    let h = FileHarness::new(vec![call_script(), text_script("done")], true).await;
    h.run().await;
    let requests = h.fake.requests();
    assert_eq!(requests[0].system.as_deref(), Some("base\nstatic\nv1"));
    assert_eq!(requests[1].system.as_deref(), Some("base\nstatic\nv2"));
    let contexts = h.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[0].attempt(), 1);
    assert_eq!(contexts[1].attempt(), 1);
    assert_ne!(contexts[0].turn_id(), contexts[1].turn_id());
}
#[tokio::test]
async fn post_compaction_attempt_refreshes_and_compaction_is_excluded() {
    let h = FileHarness::new(
        vec![
            error_script(ProviderErrorKind::ContextOverflow),
            text_script("summary"),
            text_script("done"),
        ],
        false,
    )
    .await;
    h.run().await;
    let requests = h.fake.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].system.as_deref(), Some("base\nstatic\nv1"));
    assert_eq!(
        requests[1].system.as_deref(),
        Some(
            "Summarize this context for continuation. Preserve every standing instruction and unresolved task from the previous summary and new context."
        )
    );
    assert_eq!(requests[2].system.as_deref(), Some("base\nstatic\nv2"));
    let contexts = h.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[1].attempt(), 2);
    assert!(contexts[1].after_compaction());
}
#[tokio::test]
async fn session_shadows_global_by_name() {
    let registry = Registry::new();
    let global_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    mount(&registry, "global", Scope::Global, {
        let calls = global_calls.clone();
        move |r| {
            let calls = calls.clone();
            r.prompt_contributor(
                0,
                "same",
                Arc::new(move |_| {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Box::pin(async { Ok(Some("global".into())) })
                }),
            );
        }
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    for (session, text) in [("a", "session"), ("b", "global")] {
        let mut req = request();
        req.workspace_id = session.into();
        let bootstrap = FakeProvider::scripted(vec![text_script("bootstrap")]);
        let handle = runtime(store.clone(), &bootstrap, Registry::new())
            .start(req.clone())
            .await
            .unwrap();
        let session_id = handle.session_id().clone();
        handle.done().await.unwrap();
        req.session_id = Some(session_id.clone());
        if session == "a" {
            mount(&registry, "session", Scope::Session(session_id), |r| {
                r.prompt_contributor(0, "same", constant("session"));
            })
            .await;
        }
        let fake = FakeProvider::scripted(vec![text_script("done")]);
        assert_eq!(
            runtime(store.clone(), &fake, registry.clone())
                .start(req)
                .await
                .unwrap()
                .done()
                .await
                .unwrap()
                .status,
            RunStatus::Completed
        );
        assert_eq!(fake.requests()[0].system, Some(format!("base\n{text}")));
        if session == "a" {
            assert_eq!(global_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
    }
    assert_eq!(global_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}
#[tokio::test]
async fn order_is_deterministic() {
    let registry = Registry::new();
    mount(&registry, "first", Scope::Global, |r| {
        r.prompt_contributor(10, "z", constant("z"));
        r.prompt_contributor(0, "b", constant("b"));
    })
    .await;
    mount(&registry, "second", Scope::Global, |r| {
        r.prompt_contributor(0, "a", constant("a"));
    })
    .await;
    let fake = FakeProvider::scripted(vec![
        error_script(ProviderErrorKind::RateLimited),
        text_script("done"),
    ]);
    let runtime = runtime(Arc::new(MemoryStore::new()), &fake, registry);
    assert_eq!(
        runtime
            .start(request())
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
            .status,
        RunStatus::Completed
    );
    assert_eq!(
        fake.requests()
            .iter()
            .map(|r| r.system.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("base\na\nb\nz"), Some("base\na\nb\nz")]
    );
}
#[tokio::test]
async fn concurrent_sessions_are_isolated() {
    let registry = Registry::new();
    let barrier = Arc::new(Barrier::new(2));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    mount(&registry, "isolation", Scope::Global, {
        let barrier = barrier.clone();
        let contexts = contexts.clone();
        move |r| {
            let barrier = barrier.clone();
            let contexts = contexts.clone();
            r.prompt_contributor(
                0,
                "identity",
                Arc::new(move |context| {
                    let barrier = barrier.clone();
                    let contexts = contexts.clone();
                    Box::pin(async move {
                        let text = context.session_id().to_string();
                        contexts.lock().unwrap().push(context);
                        barrier.wait().await;
                        Ok(Some(text))
                    })
                }),
            );
        }
    })
    .await;
    let fake = FakeProvider::scripted(vec![text_script("a"), text_script("b")]);
    let runtime = runtime(Arc::new(MemoryStore::new()), &fake, registry);
    let mut a = request();
    a.workspace_id = "a".into();
    a.directory = "/a".into();
    let mut b = request();
    b.workspace_id = "b".into();
    b.directory = "/b".into();
    let a = runtime.start(a).await.unwrap();
    let a_id = a.session_id().clone();
    let a_run = a.run_id().clone();
    let b = runtime.start(b).await.unwrap();
    let b_id = b.session_id().clone();
    let b_run = b.run_id().clone();
    let (a, b) = tokio::join!(a.done(), b.done());
    assert_eq!(a.unwrap().status, RunStatus::Completed);
    assert_eq!(b.unwrap().status, RunStatus::Completed);
    for req in fake.requests() {
        assert_eq!(
            req.system,
            Some(format!("base\n{}", req.identity.session_id))
        );
    }
    let contexts = contexts.lock().unwrap();
    assert_eq!(contexts.len(), 2);
    for context in contexts.iter() {
        let (run, workspace, directory) = if context.session_id() == &a_id {
            (&a_run, "a", "/a")
        } else {
            assert_eq!(context.session_id(), &b_id);
            (&b_run, "b", "/b")
        };
        assert_eq!(context.run_id(), run);
        assert_eq!(context.workspace().workspace_id(), Some(workspace));
        assert_eq!(context.workspace().directory(), Some(directory));
    }
}
