//! Cold restoration failure preserves the original, genuinely recoverable admission.
use super::*;
use crabber::{
    core::ProviderBindingDescriptor,
    providers::{BoundProvider, ProviderBindingError},
    session::{AdmissionExecutionState, session_owner::SessionOwnerState},
};

#[derive(Clone)]
struct ColdResolver {
    fake: FakeProvider,
    dir: PathBuf,
    available: bool,
}

#[async_trait]
impl Resolver for ColdResolver {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        self.fake.binding_descriptor(selection)
    }

    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        append(&self.dir.join("ledger"), "resolve");
        Ok(Arc::new(self.clone()))
    }

    async fn restore_binding(
        &self,
        descriptor: &ProviderBindingDescriptor,
    ) -> Result<BoundProvider, ProviderBindingError> {
        // The declared route remains identical; only actual restoration is unavailable.
        let selection = config().selection;
        assert_eq!(self.binding_descriptor(&selection)?, *descriptor);
        append(&self.dir.join("ledger"), "restore");
        if !self.available {
            return Err(ProviderBindingError::Unavailable);
        }
        self.bind(&selection).await
    }
}

#[async_trait]
impl Streamer for ColdResolver {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        append(&self.dir.join("ledger"), "provider");
        self.fake.stream(request).await
    }
}

struct ColdTool(PathBuf);
#[async_trait]
impl crabber::JsonToolExecutor for ColdTool {
    async fn execute(
        &self,
        _: serde_json::Value,
    ) -> Result<serde_json::Value, crabber::ExtensionError> {
        append(&self.0.join("ledger"), "tool");
        Ok(serde_json::json!({"ok":true}))
    }
}

fn cold_agent(store: Arc<dyn Store>, dir: &Path, available: bool) -> Agent {
    let call = ToolCallId::new();
    let fake = FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call.clone(),
                name: "cold_effect".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone { call_id: call },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("restored".into()),
            StreamDelta::Completed,
        ],
    ]);
    Agent::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(store)
        .provider(Arc::new(ColdResolver {
            fake,
            dir: dir.to_owned(),
            available,
        }))
        .config(config())
        .policy(Arc::new(crabber::StaticPolicy::new(
            crabber::PermissionDecision::Allow,
        )))
        .shared_tool(Arc::new(crabber::ToolDefinition {
            info: crabber::core::ToolInfo {
                name: "cold_effect".into(),
                description: "durable cold restoration test effect".into(),
                parameters: serde_json::json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(ColdTool(dir.to_owned())),
        }))
        .build()
        .unwrap()
}

// Exact equality remains useful without printing private row contents on failure.
#[derive(PartialEq)]
struct NativeSnapshot(serde_json::Value);
impl std::fmt::Debug for NativeSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeSnapshot([redacted])")
    }
}

// These are local comparisons of actual private rows, never reconstructed permissions.
async fn native_snapshot(store: &dyn Store, receipt: &AdmissionReceipt) -> NativeSnapshot {
    let run = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    let attempt = store
        .latest_execution_attempt(&receipt.run_id)
        .await
        .unwrap();
    let execution = store
        .load_admission_execution(&receipt.session_id, &options().key)
        .await
        .unwrap()
        .unwrap();
    let binding = store
        .execution_binding(&receipt.session_id, &receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    let lookup = store
        .lookup_admission(&receipt.session_id, &options().key)
        .await
        .unwrap();
    NativeSnapshot(serde_json::json!({
        "run": run,
        "fence": RunFence { run_id: run.id.clone(), claim_token: run.claim_token },
        "attempt": attempt,
        "execution": execution,
        "binding": binding,
        "receipt": lookup,
    }))
}

async fn metadata_retry(agent: &Agent, receipt: &AdmissionReceipt, context: &TraceContext) {
    assert_eq!(
        agent
            .lookup_admission(&receipt.session_id, &options().key)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert!(matches!(agent.prompt_keyed_with_context(
        receipt.session_id.clone(), "process input", options(), Some(context.clone()),
    ).await.unwrap(), Admission::Replayed(actual) if actual == *receipt));
}

async fn seed(agent: &Agent, store: &dyn Store, session: &SessionId, context: &TraceContext) {
    let error = agent
        .prompt_keyed_with_context(
            session.clone(),
            "process input",
            options(),
            Some(context.clone()),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error.operation_error(),
        RuntimeError::Store(StoreError::Validation(_))
    ));
    let original = error.preparation_reconciliation().unwrap();
    assert_eq!(
        store
            .get_session_owner(session, &original.owner.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    assert!(
        agent
            .retry_preparation_completion(original)
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(agent.retained_preparation_status(session), []);
    agent.close_extensions().await.unwrap();
}

async fn restore(
    agent: &Agent,
    store: &dyn Store,
    dir: &Path,
    receipt: &AdmissionReceipt,
    context: &TraceContext,
    available: bool,
) {
    let before = native_snapshot(store, receipt).await;
    let resolves = count(&dir.join("ledger"), "resolve");
    let restores = count(&dir.join("ledger"), "restore");
    metadata_retry(agent, receipt, context).await;
    assert_eq!(before, native_snapshot(store, receipt).await);
    assert_eq!(count(&dir.join("ledger"), "resolve"), resolves);
    assert_eq!(count(&dir.join("ledger"), "restore"), restores);
    let outcome = agent
        .recover_admission_with_context(
            receipt.session_id.clone(),
            "process input",
            options(),
            Some(context.clone()),
        )
        .await;
    if let Err(error) = &outcome {
        // Finite diagnostics distinguish restoration refusal from earlier native guards.
        let class = match error.operation_error() {
            RuntimeError::ProviderBinding(ProviderBindingError::Unavailable) => {
                "backend_unavailable"
            }
            RuntimeError::PreparationRecoveryRequired => "preparation_recovery_required",
            RuntimeError::Store(_) => "store",
            RuntimeError::SessionOwner(_) => "session_owner",
            RuntimeError::AdmissionExecution(_) => "admission_execution",
            _ => "other",
        };
        eprintln!("cold_restore_available={available} error_class={class}");
    }
    assert_eq!(count(&dir.join("ledger"), "restore"), restores + 1);
    if available {
        let Admission::Started {
            receipt: actual,
            handle,
        } = outcome.unwrap()
        else {
            panic!("eligible original must start when its actual backend is available")
        };
        assert_eq!(actual, *receipt);
        let result = handle.done().await.unwrap();
        assert_eq!(result.status, RunStatus::Completed);
        assert_eq!(result.run_id, receipt.run_id);
        assert_eq!(result.session_id, receipt.session_id);
    } else {
        let error = outcome.unwrap_err();
        assert!(matches!(
            error.operation_error(),
            RuntimeError::ProviderBinding(ProviderBindingError::Unavailable)
        ));
        assert_eq!(before, native_snapshot(store, receipt).await);
        assert_eq!(count(&dir.join("ledger"), "resolve"), resolves);
        assert_eq!(count(&dir.join("ledger"), "provider"), 0);
        assert_eq!(count(&dir.join("ledger"), "tool"), 0);
    }
    let after = native_snapshot(store, receipt).await;
    let effects = (
        count(&dir.join("ledger"), "provider"),
        count(&dir.join("ledger"), "tool"),
    );
    metadata_retry(agent, receipt, context).await;
    assert_eq!(after, native_snapshot(store, receipt).await);
    assert_eq!(
        (
            count(&dir.join("ledger"), "provider"),
            count(&dir.join("ledger"), "tool")
        ),
        effects
    );
    assert_eq!(count(&dir.join("ledger"), "restore"), restores + 1);
    agent.close_extensions().await.unwrap();
}

// Cancellation closes the original preparation; its durable key is not permission
// to repeat arbitrary plan acquisition when the backend becomes available later.
async fn reavailability_refusal(
    agent: &Agent,
    store: &dyn Store,
    dir: &Path,
    receipt: &AdmissionReceipt,
    context: &TraceContext,
) {
    let before = native_snapshot(store, receipt).await;
    let counts = (
        count(&dir.join("ledger"), "restore"),
        count(&dir.join("ledger"), "resolve"),
        count(&dir.join("ledger"), "provider"),
        count(&dir.join("ledger"), "tool"),
    );
    assert_eq!((counts.2, counts.3), (0, 0));
    metadata_retry(agent, receipt, context).await;
    let error = agent
        .recover_admission_with_context(
            receipt.session_id.clone(),
            "process input",
            options(),
            Some(context.clone()),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error.operation_error(),
        RuntimeError::PreparationRecoveryRequired
    ));
    let preparation = error.preparation_reconciliation().unwrap();
    assert_eq!(
        store
            .get_session_owner(&receipt.session_id, &preparation.owner.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    assert_eq!(before, native_snapshot(store, receipt).await);
    metadata_retry(agent, receipt, context).await;
    assert_eq!(before, native_snapshot(store, receipt).await);
    assert_eq!(
        (
            count(&dir.join("ledger"), "restore"),
            count(&dir.join("ledger"), "resolve"),
            count(&dir.join("ledger"), "provider"),
            count(&dir.join("ledger"), "tool"),
        ),
        counts
    );
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn cold_backend_child() {
    let Some((backend, dir, mode)) = Backend::child() else {
        return;
    };
    let session: SessionId =
        serde_json::from_slice(&fs::read(dir.join("session.json")).unwrap()).unwrap();
    let context: TraceContext =
        serde_json::from_slice(&fs::read(dir.join("context.json")).unwrap()).unwrap();
    let store = backend.connect().await;
    // Inject only the original post-commit response loss. Cold restoration uses
    // the actual native backend, including its original-owner cleanup lookups.
    let execution_store: Arc<dyn Store> = if mode == "commit-loss" {
        Arc::new(FaultStore {
            inner: store.clone(),
            failure: backend.failure(),
            mode: mode.clone(),
            dir: dir.clone(),
        })
    } else {
        store.clone()
    };
    let available = mode != "cold-unavailable";
    let agent = cold_agent(execution_store, &dir, available);
    if mode == "commit-loss" {
        seed(&agent, store.as_ref(), &session, &context).await;
    } else {
        let receipt: AdmissionReceipt =
            serde_json::from_slice(&fs::read(dir.join("committed.json")).unwrap()).unwrap();
        let binding = store
            .execution_binding(&session, &receipt.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            FakeProvider::scripted(vec![])
                .binding_descriptor(&config().selection)
                .unwrap(),
            binding.capsule.provider
        );
        if mode == "cold-reavailable" {
            reavailability_refusal(&agent, store.as_ref(), &dir, &receipt, &context).await;
        } else {
            restore(&agent, store.as_ref(), &dir, &receipt, &context, available).await;
        }
    }
    // A successful child exit plus this mode-specific marker proves the exact test ran.
    append(&dir.join("children"), &mode);
}

async fn run_child(backend: &Backend, dir: &Path, mode: &str) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args([
        "--exact",
        "process::backend_unavailable::cold_backend_child",
        "--nocapture",
    ]);
    backend.child_environment(&mut command, dir, mode);
    finish(ChildGuard(command.spawn().unwrap())).await;
    assert_eq!(count(&dir.join("children"), mode), 1);
}

async fn eligible_original(
    kind: &BackendKind,
) -> (Handoff, Backend, AdmissionReceipt, Arc<dyn Store>) {
    let (dir, session) = handoff();
    let backend = kind.prepare(&dir.0).await;
    let store = backend.connect().await;
    fs::write(dir.0.join("short-admission-lease"), "yes").unwrap();
    run_child(&backend, &dir.0, "commit-loss").await;
    let receipt: AdmissionReceipt =
        serde_json::from_slice(&fs::read(dir.0.join("committed.json")).unwrap()).unwrap();
    assert_eq!(receipt.session_id, session);
    let record = store
        .load_admission_execution(&session, &options().key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, AdmissionExecutionState::Unstarted);
    // No native Begin has happened; successful cold recovery below must create it.
    assert!(
        store
            .latest_execution_attempt(&receipt.run_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(count(&dir.0.join("ledger"), "restore"), 0);
    let deadline = Instant::now() + Duration::from_secs(40);
    while store
        .get_run(&receipt.run_id)
        .await
        .unwrap()
        .unwrap()
        .lease_until
        > time::OffsetDateTime::now_utc()
    {
        assert!(
            Instant::now() < deadline,
            "original durable lease did not expire"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (dir, backend, receipt, store)
}

async fn journey(kind: BackendKind) {
    let (dir, backend, receipt, store) = eligible_original(&kind).await;
    let before = native_snapshot(store.as_ref(), &receipt).await;
    run_child(&backend, &dir.0, "cold-unavailable").await;
    assert_eq!(before, native_snapshot(store.as_ref(), &receipt).await);
    assert_eq!(count(&dir.0.join("ledger"), "restore"), 1);
    assert_eq!(count(&dir.0.join("ledger"), "provider"), 0);
    assert_eq!(count(&dir.0.join("ledger"), "tool"), 0);
    run_child(&backend, &dir.0, "cold-reavailable").await;
    assert_eq!(before, native_snapshot(store.as_ref(), &receipt).await);
    assert_eq!(count(&dir.0.join("ledger"), "restore"), 1);
    assert_eq!(count(&dir.0.join("ledger"), "provider"), 0);
    assert_eq!(count(&dir.0.join("ledger"), "tool"), 0);

    // Independent eligibility control: identical protocol/declarations, a new
    // native original. This does NOT prove reavailability recovery of the failed one.
    let (control, backend, receipt, store) = eligible_original(&kind).await;
    run_child(&backend, &control.0, "cold-available").await;
    assert_eq!(count(&control.0.join("ledger"), "restore"), 1);
    assert_eq!(count(&control.0.join("ledger"), "provider"), 2);
    assert_eq!(count(&control.0.join("ledger"), "tool"), 1);
    assert_eq!(
        store
            .get_run(&receipt.run_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
    assert!(
        store
            .latest_execution_attempt(&receipt.run_id)
            .await
            .unwrap()
            .is_some(),
        "available restart must really execute a native Attempt"
    );
    assert_eq!(
        store
            .load_admission_execution(&receipt.session_id, &options().key)
            .await
            .unwrap()
            .unwrap()
            .state,
        AdmissionExecutionState::Started
    );
    assert_eq!(
        store
            .lookup_admission(&receipt.session_id, &options().key)
            .await
            .unwrap(),
        Some(receipt)
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_cold_backend_unavailable_preserves_original() {
    journey(BackendKind::Sqlite).await;
}

#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_cold_backend_unavailable_preserves_original() {
    let _guard = POSTGRES_JOURNEY_LOCK.lock().await;
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1"),
            "CRABBER_TEST_POSTGRES_URL required"
        );
        return;
    };
    crabber::session::PostgresStore::migrate(&url)
        .await
        .unwrap();
    journey(BackendKind::Postgres(url)).await;
}
