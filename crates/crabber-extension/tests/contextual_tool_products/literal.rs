use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_semantic_errors_preserve_successful_original_joins() {
    for semantic in [false, true] {
        let seen = Seen::new(Call::Ready);
        seen.literal.store(true, Ordering::SeqCst);
        seen.semantic_error.store(semantic, Ordering::SeqCst);
        let (provider, host) = setup(seen.clone(), FactoryScope::Session);
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        let facade = plan.tools[0].executor.clone();
        drop(plan);
        let resources = ToolResourceOwner::new();
        let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
        let result =
            with_callback_invocations(owner.capability(), facade.execute(Value::Null)).await;
        if semantic {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("literal semantic error")
            );
        } else {
            assert!(result.is_ok());
        }
        drop(facade);
        drop(owner);
        resources.close();
        let joined = resources.drain(Duration::from_secs(3)).await;
        assert_eq!(joined.pending, 0);
        assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
        graph(&host).request_close().unwrap();
        graph(&host)
            .tool_product(0)
            .unwrap()
            .disposed(Duration::from_secs(3))
            .await
            .unwrap();
        provider.close_all(Duration::from_secs(3)).await.unwrap();
        assert_eq!(seen.effects.load(Ordering::SeqCst), 1);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    }
}
// Both methods return OriginalCall directly, without a nested awaited fixture.
pub(super) struct Tool(pub(super) super::Tool);
impl ToolExecutor for Tool {
    fn execute<'life0, 'async_trait>(
        &'life0 self,
        _: Value,
    ) -> BoxFuture<'async_trait, Result<ToolOutput, ExtensionError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.0.0.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(OriginalCall(self.0.0.clone()))
    }
    fn execute_with_context<'life0, 'async_trait>(
        &'life0 self,
        _: ToolContext,
        arguments: Value,
    ) -> BoxFuture<'async_trait, Result<ToolOutput, ExtensionError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.execute(arguments)
    }
}
#[test]
fn poll_and_drop_panics_are_sticky() {
    for mode in ["execute", "context"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "literal::double_panic_child", "--nocapture"])
            .env("CRABBER_LITERAL_PRODUCT_CHILD", mode)
            .env("RUST_BACKTRACE", "0")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "mode={mode}, status={}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn double_panic_child() {
    let Ok(mode) = std::env::var("CRABBER_LITERAL_PRODUCT_CHILD") else {
        return;
    };
    let seen = Seen::new(Call::Ready);
    seen.literal.store(true, Ordering::SeqCst);
    seen.literal_poll_panic.store(true, Ordering::SeqCst);
    seen.callback_panic.store(true, Ordering::SeqCst);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let executor = plan.tools[0].executor.clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let result = with_callback_invocations(owner.capability(), async {
        if mode == "context" {
            let context = ToolContext::new(
                context().session_id().clone(),
                crabber_core::RunId::new(),
                crabber_core::ToolCallId::new(),
                tokio_util::sync::CancellationToken::new(),
                HostServices::default(),
                WorkspaceContext::from_persisted("workspace", "/workspace"),
                Arc::new(|_| {}),
            );
            executor.execute_with_context(context, Value::Null).await
        } else {
            executor.execute(Value::Null).await
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
    assert!(result.is_err() && disposed.is_err() && closed.is_err());
    assert_ne!(joined.failures, [] as [ResourceFailure; 0]);
    assert_eq!(seen.effects.load(Ordering::SeqCst), 1);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
