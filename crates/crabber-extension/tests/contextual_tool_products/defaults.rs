use super::*;

struct DefaultTool(super::Tool);
impl ToolExecutor for DefaultTool {
    fn execute<'a, 'async_trait>(
        &'a self,
        _: Value,
    ) -> BoxFuture<'async_trait, Result<ToolOutput, ExtensionError>>
    where
        'a: 'async_trait,
        Self: 'async_trait,
    {
        self.0.0.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(OriginalCall(self.0.0.clone()))
    }
}

// This manual future is the literal JSON method future, without an async
// fixture wrapper that would drop the original during its own unwind.
struct JsonCall(OriginalCall);
impl Future for JsonCall {
    type Output = Result<Value, ExtensionError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        std::task::ready!(Pin::new(&mut self.0).poll(cx))?;
        Poll::Ready(Ok(Value::Null))
    }
}
struct JsonTool(super::Tool);
impl JsonToolExecutor for JsonTool {
    fn execute<'a, 'async_trait>(
        &'a self,
        _: Value,
    ) -> BoxFuture<'async_trait, Result<Value, ExtensionError>>
    where
        'a: 'async_trait,
        Self: 'async_trait,
    {
        self.0.0.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(JsonCall(OriginalCall(self.0.0.clone())))
    }
}

struct ContextTool(super::Tool);
impl ToolExecutor for ContextTool {
    fn execute<'a, 'async_trait>(
        &'a self,
        _: Value,
    ) -> BoxFuture<'async_trait, Result<ToolOutput, ExtensionError>>
    where
        'a: 'async_trait,
        Self: 'async_trait,
    {
        panic!("context override must be selected")
    }
    fn execute_with_context<'a, 'async_trait>(
        &'a self,
        supplied: ToolContext,
        _: Value,
    ) -> BoxFuture<'async_trait, Result<ToolOutput, ExtensionError>>
    where
        'a: 'async_trait,
        Self: 'async_trait,
    {
        assert_eq!(supplied.session_id, *context().session_id());
        self.0.0.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(OriginalCall(self.0.0.clone()))
    }
}
pub(super) fn tool(seen: Arc<Seen>) -> Box<dyn ToolExecutor> {
    match seen.literal_adapter.load(Ordering::SeqCst) {
        1 => Box::new(DefaultTool(super::Tool(seen))),
        2 => Box::new(JsonTool(super::Tool(seen))),
        3 => Box::new(ContextTool(super::Tool(seen))),
        _ => panic!("unknown literal adapter"),
    }
}

const MODES: [&str; 4] = [
    "tool-default-context",
    "json-tool-execute",
    "json-default-context",
    "context-override",
];
async fn scenario(mode: &str, poll: bool, disposal: bool, semantic: bool) {
    let seen = Seen::new(Call::Ready);
    seen.literal_adapter.store(
        match mode {
            "tool-default-context" => 1,
            "json-tool-execute" | "json-default-context" => 2,
            "context-override" => 3,
            _ => panic!("unknown literal adapter"),
        },
        Ordering::SeqCst,
    );
    seen.literal_poll_panic.store(poll, Ordering::SeqCst);
    seen.callback_panic.store(disposal, Ordering::SeqCst);
    seen.semantic_error.store(semantic, Ordering::SeqCst);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let executor = plan.tools[0].executor.clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let result = with_callback_invocations(owner.capability(), async {
        if mode == "json-tool-execute" {
            executor.execute(Value::Null).await
        } else {
            let supplied = ToolContext::new(
                context().session_id().clone(),
                crabber_core::RunId::new(),
                crabber_core::ToolCallId::new(),
                tokio_util::sync::CancellationToken::new(),
                HostServices::default(),
                WorkspaceContext::from_persisted("workspace", "/workspace"),
                Arc::new(|_| {}),
            );
            executor.execute_with_context(supplied, Value::Null).await
        }
    })
    .await;
    drop(executor);
    drop(owner);
    resources.close();
    let joined = resources.drain(Duration::from_secs(3)).await;
    graph(&host).request_close().unwrap();
    let disposed = graph(&host)
        .tool_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    let closed = provider.close_all(Duration::from_secs(3)).await;
    assert_eq!(joined.pending, 0);
    if poll || disposal {
        assert!(result.is_err() && disposed.is_err() && closed.is_err());
        assert_ne!(joined.failures, [] as [ResourceFailure; 0]);
    } else {
        if semantic {
            assert!(
                matches!(result, Err(ExtensionError::Tool(text)) if text == "literal semantic error")
            );
        } else {
            assert_eq!(
                result.unwrap(),
                ToolOutput::from_json(&Value::Null).unwrap()
            );
        }
        assert!(disposed.is_ok() && closed.is_ok());
        assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
    }
    assert_eq!(seen.effects.load(Ordering::SeqCst), 1);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}

#[test]
fn literal_default_and_adapter_failures_are_sticky() {
    let mut failed = Vec::new();
    for mode in MODES {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "defaults::literal_failure_child", "--nocapture"])
            .env("CRABBER_LITERAL_DEFAULT_CHILD", mode)
            .env("RUST_BACKTRACE", "0")
            .output()
            .unwrap();
        if !output.status.success() {
            eprintln!(
                "{mode}: status={}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
            failed.push(mode);
        }
    }
    assert!(
        failed.is_empty(),
        "uncontained literal adapters: {failed:?}"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_failure_child() {
    let Ok(mode) = std::env::var("CRABBER_LITERAL_DEFAULT_CHILD") else {
        return;
    };
    for (poll, disposal) in [(true, false), (false, true), (true, true)] {
        scenario(&mode, poll, disposal, false).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_and_adapter_semantic_results_keep_original_joins() {
    for mode in MODES {
        for semantic in [false, true] {
            scenario(mode, false, false, semantic).await;
        }
    }
}
