use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_semantic_errors_preserve_successful_original_joins() {
    for semantic in [false, true] {
        let seen = Seen::new();
        seen.literal.store(true, Ordering::SeqCst);
        seen.semantic_error.store(semantic, Ordering::SeqCst);
        let (provider, host) = setup(seen.clone(), FactoryScope::Session);
        let plan = provider.acquire_plan_in(&context()).await.unwrap();
        let facade = plan.host_services.workspace_fs.clone().unwrap();
        drop(plan);
        let resources = ToolResourceOwner::new();
        let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
        let result = with_callback_invocations(owner.capability(), facade.read("literal")).await;
        if semantic {
            assert!(result.unwrap_err().contains("literal semantic error"));
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
            .workspace_fs_product(0)
            .unwrap()
            .disposed(Duration::from_secs(3))
            .await
            .unwrap();
        provider.close_all(Duration::from_secs(3)).await.unwrap();
        assert_eq!(seen.effects.load(Ordering::SeqCst), 1);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    }
}
// Exact async-trait ABI: Read is the literal borrowed method future.
pub(super) struct Filesystem(pub(super) super::Filesystem);
impl WorkspaceFs for Filesystem {
    fn read<'life0, 'life1, 'async_trait>(
        &'life0 self,
        path: &'life1 str,
    ) -> BoxFuture<'async_trait, Result<Vec<u8>, String>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.0.0.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(Read {
            seen: self.0.0.clone(),
            path: path.as_bytes().to_vec(),
        })
    }
}
#[test]
fn poll_and_drop_panics_are_sticky() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "literal::double_panic_child", "--nocapture"])
        .env("CRABBER_LITERAL_PRODUCT_CHILD", "1")
        .env("RUST_BACKTRACE", "0")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "status={}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn double_panic_child() {
    if std::env::var_os("CRABBER_LITERAL_PRODUCT_CHILD").is_none() {
        return;
    }
    let seen = Seen::new();
    seen.literal.store(true, Ordering::SeqCst);
    seen.literal_poll_panic.store(true, Ordering::SeqCst);
    seen.call_panic.store(true, Ordering::SeqCst);
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let filesystem = plan.host_services.workspace_fs.clone().unwrap();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let result = with_callback_invocations(owner.capability(), filesystem.read("literal")).await;
    drop(filesystem);
    drop(owner);
    resources.close();
    let joined = resources.drain(Duration::from_secs(3)).await;
    graph(&host).request_close().unwrap();
    let disposed = graph(&host)
        .workspace_fs_product(0)
        .unwrap()
        .disposed(Duration::from_secs(3))
        .await;
    let closed = provider.close_all(Duration::from_secs(3)).await;
    assert!(result.is_err() && disposed.is_err() && closed.is_err());
    assert_ne!(joined.failures, [] as [ResourceFailure; 0]);
    assert_eq!(seen.effects.load(Ordering::SeqCst), 1);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
