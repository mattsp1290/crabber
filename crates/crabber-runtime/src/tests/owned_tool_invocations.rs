//! Real ordinary/resumable tool consumers of concrete owned mount facades.
use super::*;
use crabber_extension::{Extension, Registrar, Registry, Scope};
use std::time::Duration;
#[path = "owned_tool_invocations/cancellation.rs"]
mod cancellation;
#[path = "owned_tool_invocations/guards.rs"]
mod guards;
#[path = "owned_tool_invocations/services.rs"]
mod services;

struct Tools(Vec<Arc<ToolDefinition>>);
#[async_trait]
impl Extension for Tools {
    fn id(&self) -> &'static str {
        "runtime-owned-tools"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        for tool in &self.0 {
            registrar.tool(tool.clone());
        }
        Ok(())
    }
}
async fn runtime(
    store: Arc<dyn Store>,
    tools: Vec<Arc<ToolDefinition>>,
    scripts: Vec<Vec<StreamDelta>>,
) -> (Orchestrator, Arc<Registry>) {
    let registry = Arc::new(Registry::new());
    registry
        .mount_owned(Arc::new(Tools(tools)), Scope::Global)
        .await
        .unwrap();
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .resolver(Arc::new(FakeProvider::scripted(scripts)))
        .plan_provider(registry.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    (runtime, registry)
}
async fn journey(store: Arc<dyn Store>) {
    use crabber_core::{AuthorityRequirement, ControlId};
    use crabber_session::{ApprovalResolverClaim, ToolInteractionAnswer, ToolInteractionReply};
    let effects = Arc::new(AtomicUsize::new(0));
    let mut input = call_script(
        ToolCallId::new(),
        r#"{"questions":[{"name":"color","prompt":"Choose","choices":["blue"]}]}"#,
    );
    for delta in &mut input {
        if let StreamDelta::ToolCallStart { name, .. } = delta {
            *name = "request_input".into();
        }
    }
    let (runtime, registry) = runtime(
        store.clone(),
        vec![
            tool(Arc::new(EchoTool(effects.clone()))),
            Arc::new(
                ToolDefinition::input(
                    "request_input",
                    AuthorityRequirement::new("operator", "1").unwrap(),
                )
                .unwrap(),
            ),
        ],
        vec![
            call_script(ToolCallId::new(), r#"{"text":"one effect"}"#),
            input,
            text_script("done"),
        ],
    )
    .await;
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let snapshot = runtime
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let question = &snapshot.interactions[0];
    let answered = runtime
        .answer_tool_interaction(ToolInteractionAnswer {
            id: ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: question.id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "host".into(),
                authority: question.description.authority().clone(),
                authority_claim: "authenticated operator".into(),
            },
            reply: ToolInteractionReply::Answer {
                value: json!("blue"),
            },
        })
        .await
        .unwrap();
    let request = crate::ContinueTool {
        id: ControlId::new(),
        session_id: session,
        run_id: run.clone(),
        continuation: snapshot.continuation,
        expected_revision: answered.revision,
    };
    let crate::ToolContinuation::Started { handle, .. } =
        runtime.continue_tool(request.clone()).await.unwrap()
    else {
        panic!("fresh continuation")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    registry.close_all().await.unwrap();
    assert!(matches!(
        runtime.continue_tool(request).await.unwrap(),
        crate::ToolContinuation::Replayed(_)
    ));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .latest_execution_attempt(&run)
            .await
            .unwrap()
            .unwrap()
            .attempt
            .cleanup,
        crabber_session::AttemptCleanup::Complete
    );
}
async fn contract(store: Arc<dyn Store>) {
    journey(store.clone()).await;
    guards::contract(store.clone()).await;
    services::contract(store.clone()).await;
    cancellation::contract(store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_owned_tool_invocations() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_owned_tool_invocations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tools.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_owned_tool_invocations() {
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

#[path = "owned_tool_invocations/products.rs"]
mod products;

#[path = "owned_tool_invocations/workspace_fs_products.rs"]
mod workspace_fs_products;

#[path = "owned_tool_invocations/subprocess_products.rs"]
mod subprocess_products;

#[path = "owned_tool_invocations/workspace_reader_products.rs"]
mod workspace_reader_products;

#[path = "owned_tool_invocations/guard_products.rs"]
mod guard_products;

#[path = "owned_tool_invocations/mixed_products.rs"]
mod mixed_products;
