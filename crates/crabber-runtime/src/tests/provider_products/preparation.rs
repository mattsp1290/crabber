//! The real preparation bridge does not create a model callback owner.
use super::*;
use crabber_extension::{
    Extension, PreparationResourceContext, PreparationResourceContract, Registrar, RunPlan, Scope,
};
struct Tools;
#[async_trait]
impl Extension for Tools {
    fn id(&self) -> &'static str {
        "provider-product-tools"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixed".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0))))));
        Ok(())
    }
}
struct PreparedPlans(Arc<ContextualPlanProvider>);

// This fixture tests the real preparation-to-binding refusal, not initialization
// deadline exactness. Keep its configured two seconds while excluding OS/SQL
// scheduling from that clock; native store clocks and checks are unchanged.
struct ResumeClock;
impl Drop for ResumeClock {
    fn drop(&mut self) {
        tokio::time::resume();
    }
}
async fn acquire_with_frozen_clock(
    operation: impl std::future::Future<Output = Result<RunPlan, ExtensionError>>,
) -> Result<RunPlan, ExtensionError> {
    tokio::time::pause();
    let _resume = ResumeClock;
    let started = std::time::Instant::now();
    let watchdog = Duration::from_secs(10);
    tokio::pin!(operation);
    loop {
        assert!(
            started.elapsed() < watchdog,
            "preparation acquisition wall-clock watchdog"
        );
        tokio::select! {
            biased;
            result = &mut operation => {
                assert!(started.elapsed() < watchdog, "preparation acquisition wall-clock watchdog");
                return result;
            },
            // Stay runnable so a paused runtime never advances the fixture's
            // initialization clock while genuine retained SQL/tasks progress.
            () = tokio::task::yield_now() => {}
        }
    }
}

#[async_trait]
impl RunPlanProvider for PreparedPlans {
    fn registration_fingerprint(
        &self,
        context: &SessionContext,
    ) -> Result<InputFingerprint, ExtensionError> {
        self.0.registration_fingerprint(context)
    }
    fn preparation_resource_contract(
        &self,
        _: &SessionContext,
    ) -> Result<Option<PreparationResourceContract>, ExtensionError> {
        Ok(Some(PreparationResourceContract::new(
            ComponentIdentity {
                id: "actual-product-preparation-fixture".into(),
                version: "1".into(),
            },
            InputFingerprint::new("c".repeat(64)).unwrap(),
        )?))
    }
    async fn acquire_plan(&self, _: &SessionId) -> Result<RunPlan, ExtensionError> {
        panic!("no legacy fallback")
    }
    async fn acquire_plan_in_preparation(
        &self,
        context: &PreparationResourceContext,
    ) -> Result<RunPlan, ExtensionError> {
        // Actual library acquisition/service bridge; no surrogate callback owner.
        acquire_with_frozen_clock(self.0.acquire_plan_in_preparation(context)).await
    }
}
async fn contract(store: Arc<dyn Store>) {
    let registry = Registry::new();
    registry
        .mount_owned(Arc::new(Tools), Scope::Global)
        .await
        .unwrap();
    let seen = Arc::new(Seen::default());
    let fake = FakeProvider::scripted(vec![call_script(ToolCallId::new(), r#"{"text":"paused"}"#)]);
    let plans = Arc::new(
        ContextualPlanProvider::new_with_provider_products(
            registry,
            vec![Arc::new(Factory {
                seen: seen.clone(),
                fake: fake.clone(),
                blocked: false,
            })],
            Duration::from_secs(2),
            Arc::new(Host),
        )
        .unwrap(),
    );
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(PreparedPlans(plans.clone())))
        .policy(Arc::new(PausePolicy))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    let context = SessionContext::new(handle.session_id().clone(), "test", "/tmp");
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    runtime
        .drain_session_execution_tasks(context.session_id(), Duration::from_secs(2))
        .await;
    let original = store.get_run(&run).await.unwrap().unwrap();
    let result = runtime.resume_execution(&run).await;
    let error = result
        .err()
        .expect("preparation has no model callback owner");
    let before_cleanup = (
        seen.binds.load(Ordering::SeqCst),
        seen.creates.load(Ordering::SeqCst),
        fake.requests().len(),
    );
    for progress in runtime.retained_preparation_status(context.session_id()) {
        let _ = runtime
            .cleanup_retained_preparation(&progress.preparation)
            .await;
    }
    runtime
        .drain_session_execution_tasks(context.session_id(), Duration::from_secs(2))
        .await;
    plans
        .close_session(&context, Duration::from_secs(2))
        .await
        .unwrap();
    let current = store.get_run(&run).await.unwrap().unwrap();
    assert_eq!(before_cleanup, (1, 1, 1), "{error:?}");
    assert_eq!(
        current.claim_token, original.claim_token,
        "no resume claim after refused binding"
    );
    assert_eq!(current.status, RunStatus::Paused);
    assert!(
        format!("{error:?}").contains("ResourcesUnsupported"),
        "{error:?}"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn memory_some_resume_refuses_before_box_binding_effect() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "current_thread")]
async fn sqlite_some_resume_refuses_before_box_binding_effect() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resume.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "current_thread")]
async fn postgres_some_resume_refuses_before_box_binding_effect() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    contract(Arc::new(
        crabber_session::PostgresStore::connect(&url).await.unwrap(),
    ))
    .await;
}
