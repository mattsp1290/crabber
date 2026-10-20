use super::*;

struct DisposalPanic;
impl Future for DisposalPanic {
    type Output = Result<Value, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(Value::Null))
    }
}
impl Drop for DisposalPanic {
    fn drop(&mut self) {
        panic!("kernel callback disposal failed");
    }
}
struct BrokenHook(bool);
#[async_trait]
impl Extension for BrokenHook {
    fn id(&self) -> &'static str {
        "broken-kernel-hook"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        format!("disposal-panic={}", self.0)
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let bad_drop = self.0;
        r.on_hook(
            crabber_extension::TurnPrepare::ID,
            0,
            "bad-drop",
            Arc::new(move |_| {
                if bad_drop {
                    Box::pin(DisposalPanic)
                } else {
                    Box::pin(async { Err(ExtensionError::Plan("semantic hook rejection".into())) })
                }
            }),
        );
        Ok(())
    }
}
pub(super) async fn contract(store: Arc<dyn Store>) {
    for bad_drop in [false, true] {
        run(store.clone(), bad_drop).await;
    }
}
async fn run(store: Arc<dyn Store>, bad_drop: bool) {
    let registry = Arc::new(Registry::new());
    registry
        .mount_owned(Arc::new(BrokenHook(bad_drop)), Scope::Global)
        .await
        .unwrap();
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![text_script(
            "unused",
        )])))
        .plan_provider(registry.clone())
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    let result = handle.done().await;
    let attempt = store
        .latest_execution_attempt(&run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    if bad_drop {
        assert_eq!(
            attempt.state,
            crabber_session::AttemptState::Started,
            "kernel disposal failure certified native cleanup: {attempt:?}, result={result:?}"
        );
        assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
        assert!(matches!(
            result,
            Err(RuntimeError::AttemptCleanupIncomplete { .. })
        ));
        assert!(registry.close_all().await.is_err());
    } else {
        assert_eq!(attempt.state, crabber_session::AttemptState::Failed);
        assert_eq!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
        assert!(matches!(result, Err(RuntimeError::Extension(_))));
        registry.close_all().await.unwrap();
    }
}
#[tokio::test]
async fn kernel_callback_disposal_failure_cannot_certify_attempt_cleanup() {
    contract(Arc::new(MemoryStore::new())).await;
}
