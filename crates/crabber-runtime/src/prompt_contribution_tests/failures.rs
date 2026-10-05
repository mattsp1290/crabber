use super::*;
use crate::RuntimeError;
use crabber_extension::{
    CleanupTracker, MAX_PROMPT_CONTRIBUTION_BYTES, MAX_PROMPT_CONTRIBUTIONS_TOTAL_BYTES,
    PROMPT_CONTRIBUTION_DEADLINE,
};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn interrupt_during_contribution_settles_interrupted() {
    let registry = Registry::new();
    let entered = Arc::new(Notify::new());
    let captured = Arc::new(Mutex::new(None));
    mount(&registry, "cancel", Scope::Global, {
        let entered = entered.clone();
        let captured = captured.clone();
        move |r| {
            let entered = entered.clone();
            let captured = captured.clone();
            r.prompt_contributor(
                0,
                "wait",
                Arc::new(move |context| {
                    *captured.lock().unwrap() = Some(context.cancellation().clone());
                    entered.notify_one();
                    Box::pin(async move {
                        context.cancellation().cancelled().await;
                        Ok(None)
                    })
                }),
            );
        }
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![text_script("unused")]);
    let handle = runtime(store.clone(), &fake, registry)
        .start(request())
        .await
        .unwrap();
    let run_id = handle.run_id().clone();
    entered.notified().await;
    handle.interrupt();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Interrupted);
    assert!(captured.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().error,
        Some(RuntimeError::Interrupted.to_string())
    );
    assert_eq!(fake.requests().len(), 0);
}
#[tokio::test]
async fn oversize_contribution_fails_before_provider() {
    for (count, size, last, succeeds) in [
        (1, MAX_PROMPT_CONTRIBUTION_BYTES, 0, true),
        (1, MAX_PROMPT_CONTRIBUTION_BYTES + 1, 0, false),
        (
            MAX_PROMPT_CONTRIBUTIONS_TOTAL_BYTES / MAX_PROMPT_CONTRIBUTION_BYTES,
            MAX_PROMPT_CONTRIBUTION_BYTES,
            0,
            true,
        ),
        (4, MAX_PROMPT_CONTRIBUTION_BYTES, 1, false),
    ] {
        let registry = Registry::new();
        mount(&registry, "bytes", Scope::Global, move |r| {
            for i in 0..count {
                r.prompt_contributor(0, i.to_string(), constant(&"x".repeat(size)));
            }
            if last > 0 {
                r.prompt_contributor(1, "last", constant("x"));
            }
        })
        .await;
        let store = Arc::new(MemoryStore::new());
        let fake = FakeProvider::scripted(vec![text_script("done")]);
        let handle = runtime(store.clone(), &fake, registry)
            .start(request())
            .await
            .unwrap();
        let id = handle.run_id().clone();
        let result = handle.done().await;
        if succeeds {
            assert_eq!(result.unwrap().status, RunStatus::Completed);
        } else {
            assert!(matches!(result, Err(RuntimeError::Extension(_))));
        }
        assert_eq!(
            store.get_run(&id).await.unwrap().unwrap().status,
            if succeeds {
                RunStatus::Completed
            } else {
                RunStatus::Failed
            }
        );
        assert_eq!(fake.requests().len(), usize::from(succeeds));
        if !succeeds {
            assert_eq!(
                store.get_run(&id).await.unwrap().unwrap().error,
                Some(format!(
                    "extension: prompt contribution failed: {}",
                    if last > 0 { "last" } else { "0" }
                ))
            );
        }
    }
}
#[tokio::test(start_paused = true)]
async fn deadline_failure_keeps_spawned_work_tracked() {
    let registry = Registry::new();
    let entered = Arc::new(Notify::new());
    let captured: Arc<Mutex<Option<(CancellationToken, CleanupTracker)>>> =
        Arc::new(Mutex::new(None));
    let release = CancellationToken::new();
    let capacity = Arc::new(Semaphore::new(1));
    mount(&registry, "slow", Scope::Global, {
        let entered = entered.clone();
        let captured = captured.clone();
        let release = release.clone();
        let capacity = capacity.clone();
        move |r| {
            let entered = entered.clone();
            let captured = captured.clone();
            let release = release.clone();
            let capacity = capacity.clone();
            r.prompt_contributor(
                0,
                "slow",
                Arc::new(move |context| {
                    let release = release.clone();
                    let capacity = capacity.clone();
                    let entered = entered.clone();
                    let captured = captured.clone();
                    Box::pin(async move {
                        let permit = capacity.acquire_owned().await.unwrap();
                        *captured.lock().unwrap() =
                            Some((context.cancellation().clone(), context.cleanup().clone()));
                        context.cleanup().spawn(async move {
                            release.cancelled().await;
                            drop(permit);
                        });
                        entered.notify_one();
                        futures::future::pending().await
                    })
                }),
            );
        }
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![text_script("unused")]);
    let handle = runtime(store.clone(), &fake, registry)
        .start(request())
        .await
        .unwrap();
    entered.notified().await;
    tokio::time::advance(PROMPT_CONTRIBUTION_DEADLINE).await;
    let id = handle.run_id().clone();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::Extension(_))
    ));
    assert_eq!(
        store.get_run(&id).await.unwrap().unwrap().status,
        RunStatus::Failed
    );
    let (token, tracker) = captured.lock().unwrap().clone().unwrap();
    assert!(token.is_cancelled());
    assert_eq!(tracker.pending(), 1);
    assert_eq!(capacity.available_permits(), 0);
    assert_eq!(fake.requests().len(), 0);
    release.cancel();
    tokio::task::yield_now().await;
    assert_eq!(tracker.pending(), 0);
    assert_eq!(capacity.available_permits(), 1);
}
#[tokio::test]
async fn callback_error_and_panic_are_sanitized() {
    for panic in [false, true] {
        let registry = Registry::new();
        mount(&registry, "bad", Scope::Global, move |r| {
            r.prompt_contributor(
                0,
                "bad",
                Arc::new(move |_| {
                    Box::pin(async move {
                        assert!(!panic, "secret-token");
                        Err(ExtensionError::Tool("secret-token".into()))
                    })
                }),
            );
        })
        .await;
        let store = Arc::new(MemoryStore::new());
        let fake = FakeProvider::scripted(vec![text_script("unused")]);
        let handle = runtime(store.clone(), &fake, registry)
            .start(request())
            .await
            .unwrap();
        let id = handle.run_id().clone();
        let session = handle.session_id().clone();
        assert!(matches!(
            handle.done().await,
            Err(RuntimeError::Extension(_))
        ));
        assert_eq!(
            store.get_run(&id).await.unwrap().unwrap().status,
            RunStatus::Failed
        );
        let run = store.get_run(&id).await.unwrap().unwrap();
        assert_eq!(
            run.error.as_deref(),
            Some("extension: prompt contribution failed: bad")
        );
        assert!(
            !serde_json::to_string(&run)
                .unwrap()
                .contains("secret-token")
        );
        assert!(
            !serde_json::to_string(&store.list_messages(&session, None).await.unwrap())
                .unwrap()
                .contains("secret-token")
        );
        let events = store.list_events(&session, None, 100).await.unwrap();
        assert!(
            !serde_json::to_string(&events)
                .unwrap()
                .contains("secret-token")
        );
        assert!(
            events
                .iter()
                .all(|event| event.kind != crabber_core::EventKind::MessageStarted)
        );
        assert_eq!(fake.requests().len(), 0);
    }
}
#[tokio::test]
async fn failure_on_retry_makes_no_second_provider_call() {
    let registry = Registry::new();
    mount(&registry, "retry", Scope::Global, |r| {
        r.prompt_contributor(
            0,
            "retry",
            Arc::new(|context| {
                Box::pin(async move {
                    if context.attempt() == 1 {
                        Ok(Some("first".into()))
                    } else {
                        Err(ExtensionError::Tool("secret-token".into()))
                    }
                })
            }),
        );
    })
    .await;
    let fake = FakeProvider::scripted(vec![
        error_script(ProviderErrorKind::RateLimited),
        text_script("unused"),
    ]);
    let store = Arc::new(MemoryStore::new());
    let handle = runtime(store.clone(), &fake, registry)
        .start(request())
        .await
        .unwrap();
    let id = handle.run_id().clone();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::Extension(_))
    ));
    assert_eq!(
        store.get_run(&id).await.unwrap().unwrap().status,
        RunStatus::Failed
    );
    assert_eq!(fake.requests().len(), 1);
    assert_eq!(fake.requests()[0].system.as_deref(), Some("base\nfirst"));
}

struct PanicPayload;
impl Drop for PanicPayload {
    fn drop(&mut self) {
        panic!("secret payload destructor");
    }
}
fn panic_with_payload() -> Result<Option<String>, ExtensionError> {
    std::panic::panic_any(PanicPayload)
}
#[tokio::test]
async fn panic_payload_destructor_settles_failed_without_provider_call() {
    let registry = Registry::new();
    mount(&registry, "payload", Scope::Global, |r| {
        r.prompt_contributor(
            0,
            "payload",
            Arc::new(|_| Box::pin(async { panic_with_payload() })),
        );
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![text_script("unused")]);
    let handle = runtime(store.clone(), &fake, registry)
        .start(request())
        .await
        .unwrap();
    let id = handle.run_id().clone();
    let error = handle.done().await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "extension: prompt contribution failed: payload"
    );
    let run = store.get_run(&id).await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(
        run.error.as_deref(),
        Some("extension: prompt contribution failed: payload")
    );
    assert_eq!(fake.requests().len(), 0);
}

const TEST_MIDDLEWARE_HASH: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

struct ContentReader(Vec<u8>, Arc<Mutex<Vec<(String, usize)>>>);
#[async_trait]
impl WorkspaceReader for ContentReader {
    async fn read_limited(&self, path: &str, limit: usize) -> Result<Vec<u8>, WorkspaceReadError> {
        self.1.lock().unwrap().push((path.into(), limit));
        Ok(self.0.clone())
    }
}

struct RecordingResolver {
    entries: Vec<(WorkspaceContext, Arc<dyn WorkspaceReader>)>,
    calls: Arc<Mutex<Vec<WorkspaceContext>>>,
    failure: Option<WorkspaceReadErrorKind>,
}
#[async_trait]
impl WorkspaceReaderResolver for RecordingResolver {
    async fn resolve(
        &self,
        workspace: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        self.calls.lock().unwrap().push(workspace.clone());
        if let Some(kind) = self.failure {
            return Err(WorkspaceReadError::new(kind));
        }
        self.entries
            .iter()
            .find(|(candidate, _)| candidate == workspace)
            .map(|(_, reader)| reader.clone())
            .ok_or_else(|| WorkspaceReadError::new(WorkspaceReadErrorKind::Denied))
    }
}

struct ReaderMiddleware(Arc<Mutex<Vec<WorkspaceContext>>>, usize);
#[async_trait]
impl SystemPromptMiddleware for ReaderMiddleware {
    async fn contribute(&self, context: ModelAttemptContext) -> Result<Option<String>, String> {
        self.0.lock().unwrap().push(context.workspace().clone());
        let resolver = context
            .workspace_reader_resolver()
            .ok_or("MissingCapability(workspace_reader_resolver)")?;
        let reader = resolver
            .resolve(context.workspace())
            .await
            .map_err(|error| format!("backend detail: {:?}", error.kind()))?;
        let bytes = reader
            .read_limited("AGENTS.md", self.1)
            .await
            .map_err(|error| format!("backend detail: {:?}", error.kind()))?;
        if bytes.len() > self.1 {
            return Err("workspace reader exceeded requested limit".into());
        }
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| "workspace content was not UTF-8".into())
    }
}

struct ForgingMiddleware {
    forged: WorkspaceContext,
    observed_error: Arc<Mutex<Option<WorkspaceReadErrorKind>>>,
}

#[async_trait]
impl SystemPromptMiddleware for ForgingMiddleware {
    async fn contribute(&self, context: ModelAttemptContext) -> Result<Option<String>, String> {
        let resolver = context
            .workspace_reader_resolver()
            .ok_or("MissingCapability(workspace_reader_resolver)")?;
        match resolver.resolve(&self.forged).await {
            Ok(_) => Ok(Some("forged workspace content".into())),
            Err(error) => {
                *self.observed_error.lock().unwrap() = Some(error.kind());
                Err(format!(
                    "forged={} backend detail: {:?}",
                    self.forged.directory().unwrap_or("<missing>"),
                    error.kind()
                ))
            }
        }
    }
}

async fn typed_registry(contexts: Arc<Mutex<Vec<WorkspaceContext>>>, limit: usize) -> Registry {
    let registry = Registry::new();
    mount(&registry, "typed-reader", Scope::Global, move |registrar| {
        registrar
            .system_prompt_middleware(
                "workspace-reader",
                0,
                MiddlewareDescriptor::new("test-reader", "1", TEST_MIDDLEWARE_HASH).unwrap(),
                Arc::new(ReaderMiddleware(contexts.clone(), limit)),
            )
            .unwrap();
    })
    .await;
    registry
}

#[tokio::test]
async fn model_middleware_workspace_resolver_is_scoped_to_exact_admitted_workspace() {
    let first = WorkspaceContext::from_persisted("workspace-a", "/admitted/a");
    let second = WorkspaceContext::from_persisted("workspace-b", "/admitted/b");
    let first_reads = Arc::new(Mutex::new(Vec::new()));
    let second_reads = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let resolver: Arc<dyn WorkspaceReaderResolver> = Arc::new(RecordingResolver {
        entries: vec![
            (
                first.clone(),
                Arc::new(ContentReader(b"first-only".to_vec(), first_reads.clone())),
            ),
            (
                second.clone(),
                Arc::new(ContentReader(b"second-only".to_vec(), second_reads.clone())),
            ),
        ],
        calls: calls.clone(),
        failure: None,
    });
    let fake = FakeProvider::scripted(vec![text_script("one"), text_script("two")]);
    let runtime = Orchestrator::builder()
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(typed_registry(contexts.clone(), 32).await))
        .workspace_reader_resolver(resolver)
        .build()
        .unwrap();
    for (workspace_id, directory) in [
        ("workspace-a", "/admitted/a"),
        ("workspace-b", "/admitted/b"),
    ] {
        let mut input = request();
        input.workspace_id = workspace_id.into();
        input.directory = directory.into();
        runtime.start(input).await.unwrap().done().await.unwrap();
    }
    assert_eq!(*calls.lock().unwrap(), vec![first.clone(), second.clone()]);
    assert_eq!(*contexts.lock().unwrap(), vec![first, second]);
    assert_eq!(*first_reads.lock().unwrap(), vec![("AGENTS.md".into(), 32)]);
    assert_eq!(
        *second_reads.lock().unwrap(),
        vec![("AGENTS.md".into(), 32)]
    );
    let requests = fake.requests();
    assert_eq!(requests[0].system.as_deref(), Some("base\nfirst-only"));
    assert_eq!(requests[1].system.as_deref(), Some("base\nsecond-only"));
}

#[tokio::test]
async fn model_middleware_workspace_resolver_denies_forged_context_without_host_dispatch() {
    let forged = WorkspaceContext::from_persisted("other-workspace", "/secret/backend");
    let host_calls = Arc::new(Mutex::new(Vec::new()));
    let observed_error = Arc::new(Mutex::new(None));
    let registry = Registry::new();
    mount(&registry, "typed-forger", Scope::Global, {
        let forged = forged.clone();
        let observed_error = observed_error.clone();
        move |registrar| {
            registrar
                .system_prompt_middleware(
                    "forged-workspace",
                    0,
                    MiddlewareDescriptor::new("test-forger", "1", TEST_MIDDLEWARE_HASH).unwrap(),
                    Arc::new(ForgingMiddleware {
                        forged: forged.clone(),
                        observed_error: observed_error.clone(),
                    }),
                )
                .unwrap();
        }
    })
    .await;
    let resolver: Arc<dyn WorkspaceReaderResolver> = Arc::new(RecordingResolver {
        entries: vec![(
            forged,
            Arc::new(ContentReader(
                b"secret".to_vec(),
                Arc::new(Mutex::new(Vec::new())),
            )),
        )],
        calls: host_calls.clone(),
        failure: None,
    });
    let fake = FakeProvider::scripted(vec![text_script("unused")]);
    let runtime = Orchestrator::builder()
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(registry))
        .workspace_reader_resolver(resolver)
        .build()
        .unwrap();

    let error = runtime
        .start(request())
        .await
        .unwrap()
        .done()
        .await
        .unwrap_err();
    let outward = error.to_string();
    assert_eq!(
        outward,
        "extension: prompt contribution failed: forged-workspace"
    );
    assert!(!outward.contains("/secret/backend"));
    assert!(!outward.contains("Denied"));
    assert_eq!(
        *observed_error.lock().unwrap(),
        Some(WorkspaceReadErrorKind::Denied)
    );
    assert_eq!(*host_calls.lock().unwrap(), Vec::<WorkspaceContext>::new());
    assert_eq!(fake.requests().len(), 0);
}

#[tokio::test]
async fn model_middleware_workspace_failures_are_sanitized_before_provider_dispatch() {
    for failure in [
        None,
        Some(WorkspaceReadErrorKind::Denied),
        Some(WorkspaceReadErrorKind::Io),
    ] {
        let fake = FakeProvider::scripted(vec![text_script("unused")]);
        let store = Arc::new(MemoryStore::new());
        let mut builder = Orchestrator::builder()
            .store(store.clone())
            .resolver(Arc::new(fake.clone()))
            .plan_provider(Arc::new(
                typed_registry(Arc::new(Mutex::new(Vec::new())), 4).await,
            ));
        if let Some(kind) = failure {
            builder = builder.workspace_reader_resolver(Arc::new(RecordingResolver {
                entries: vec![],
                calls: Arc::new(Mutex::new(Vec::new())),
                failure: Some(kind),
            }));
        }
        let handle = builder.build().unwrap().start(request()).await.unwrap();
        let run_id = handle.run_id().clone();
        assert!(matches!(
            handle.done().await,
            Err(RuntimeError::Extension(_))
        ));
        assert_eq!(fake.requests().len(), 0);
        assert_eq!(
            store
                .get_run(&run_id)
                .await
                .unwrap()
                .unwrap()
                .error
                .as_deref(),
            Some("extension: prompt contribution failed: workspace-reader")
        );
    }
}

#[tokio::test]
async fn model_middleware_workspace_rejects_nonconforming_reader_before_provider_dispatch() {
    let fake = FakeProvider::scripted(vec![text_script("unused")]);
    let runtime = Orchestrator::builder()
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(
            typed_registry(Arc::new(Mutex::new(Vec::new())), 4).await,
        ))
        .workspace_reader_resolver(Arc::new(RecordingResolver {
            entries: vec![(
                WorkspaceContext::from_persisted("workspace", "/workspace"),
                Arc::new(ContentReader(
                    b"five!".to_vec(),
                    Arc::new(Mutex::new(Vec::new())),
                )),
            )],
            calls: Arc::new(Mutex::new(Vec::new())),
            failure: None,
        }))
        .build()
        .unwrap();
    assert!(matches!(
        runtime.start(request()).await.unwrap().done().await,
        Err(RuntimeError::Extension(_))
    ));
    assert_eq!(fake.requests().len(), 0);
}
