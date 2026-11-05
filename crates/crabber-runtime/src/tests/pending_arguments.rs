use super::*;
use crabber_extension::{Extension, Point, Registrar, Registry, Scope, ToolPrepare};

struct Prepare(Value);
#[async_trait]
impl ToolPipeline for Prepare {
    async fn prepare(&self, _: &crabber_core::ToolInfo, _: Value) -> Result<Value, String> {
        Ok(self.0.clone())
    }
    async fn transform_result(
        &self,
        _: &crabber_extension::ToolResultContext,
        _: &crabber_core::ToolInfo,
        result: crabber_core::ToolOutput,
    ) -> Result<crabber_extension::TransformOutput, String> {
        Ok(crabber_extension::TransformOutput::new(result))
    }
}

#[derive(Default)]
struct Capture(Mutex<Vec<Value>>);
#[async_trait]
impl JsonToolExecutor for Capture {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        self.0.lock().unwrap().push(arguments);
        Ok(json!("executed"))
    }
}

struct PreparedExtension(Arc<Capture>);
#[async_trait]
impl Extension for PreparedExtension {
    fn id(&self) -> &'static str {
        "pending-arguments"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let mut info = tool(self.0.clone()).info.clone();
        info.parameters = json!({"type":"object"});
        registrar.tool(Arc::new(ToolDefinition {
            info,
            executor: self.0.clone(),
        }));
        registrar.on_transform(
            ToolPrepare::ID,
            0,
            "prepare",
            Arc::new(|mut value| {
                if value["input"]["text"] == "pipeline" {
                    value["input"]["text"] = json!("extension");
                }
                Box::pin(async move { Ok(value) })
            }),
        );
        Ok(())
    }
}

#[derive(Default)]
struct Published(Mutex<Vec<EventRecord>>);
impl Observer for Published {
    fn emit(&self, event: &EventRecord) {
        if event.kind == EventKind::ToolCallPending {
            self.0.lock().unwrap().push(event.clone());
        }
    }
}

async fn staged(
    store: Arc<dyn Store>,
    prepared: Value,
    name: &str,
) -> (SessionId, EventRecord, ToolCallRecord, Vec<Value>) {
    let executor = Arc::new(Capture::default());
    let observer = Arc::new(Published::default());
    let registry = Arc::new(Registry::new());
    registry
        .mount_owned(Arc::new(PreparedExtension(executor.clone())), Scope::Global)
        .await
        .unwrap();
    let id = ToolCallId::new();
    let mut script = call_script(id.clone(), r#"{"text":"requested"}"#);
    for delta in &mut script {
        if let StreamDelta::ToolCallStart { name: actual, .. } = delta {
            *actual = name.into();
        }
    }
    let runtime = Orchestrator::builder()
        .behavior_version("pending-arguments.v1")
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![
            script,
            text_script("done"),
        ])))
        .plan_provider(registry.clone())
        .tool_pipeline(Arc::new(Prepare(prepared)))
        .observer(observer.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let pending = store
        .list_events(&session, None, 100)
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.kind == EventKind::ToolCallPending)
        .unwrap();
    let mut published = observer.0.lock().unwrap()[0].clone();
    published.cursor = pending.cursor;
    assert_eq!(published, pending);
    let crabber_session::SnapshotOutcome::Page(page) = store
        .snapshot(crabber_session::SnapshotRequest {
            session_id: session.clone(),
            continuation: None,
            limits: crabber_session::SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 1000,
                text_bytes: 32 * 1024 * 1024,
                encoded_bytes: 32 * 1024 * 1024,
            },
        })
        .await
        .unwrap()
    else {
        panic!("complete fixture snapshot")
    };
    let call = page
        .tool_calls
        .into_iter()
        .find(|call| call.id == id)
        .unwrap();
    assert_eq!(call.run_id, run);
    let seen = executor.0.lock().unwrap().clone();
    (session, pending, call, seen)
}

#[tokio::test]
async fn memory_pending_arguments_are_final_prepared_input_and_not_model_request() {
    let (_, event, call, seen) = staged(
        Arc::new(MemoryStore::new()),
        json!({"text":"pipeline"}),
        "echo",
    )
    .await;
    assert_eq!(seen, [json!({"text":"extension"})]);
    assert_eq!(event.payload["arguments"], call.arguments);
    assert_eq!(event.payload["arguments"], seen[0]);
    assert_ne!(seen[0], json!({"text":"requested"}));
}

#[tokio::test]
async fn rejected_and_unknown_pending_inputs_do_not_execute() {
    for (name, input, sentinel) in [
        ("echo", json!("invalid object"), "$crabber_prepare_error"),
        (
            "echo",
            json!({"text":"nul\0input"}),
            "$crabber_prepare_error",
        ),
        (
            "missing",
            json!({"text":"pipeline"}),
            "$crabber_unknown_tool",
        ),
    ] {
        let (_, event, call, seen) = staged(Arc::new(MemoryStore::new()), input, name).await;
        assert_eq!(seen, [] as [Value; 0]);
        assert!(call.arguments.get(sentinel).is_some(), "{call:?}");
        assert_eq!(event.payload["arguments"], call.arguments);
        assert_eq!(call.status, crabber_core::ToolCallStatus::Failed);
    }
}

fn event_depth_boundary() -> Value {
    for depth in 100..130 {
        let mut arguments = json!(null);
        for _ in 0..depth {
            arguments = json!([arguments]);
        }
        arguments = json!({"deep":arguments});
        let record = ToolCallRecord {
            id: ToolCallId::new(),
            run_id: RunId::new(),
            name: "echo".into(),
            arguments: arguments.clone(),
            status: crabber_core::ToolCallStatus::Pending,
            retry_safe: true,
            result: None,
        };
        let event = EventRecord {
            cursor: None,
            session_id: SessionId::new(),
            run_id: record.run_id.clone(),
            turn_id: None,
            kind: EventKind::ToolCallPending,
            payload: json!({"call_id":record.id,"name":"echo","status":"pending","message_id":null,"arguments":arguments}),
            correlation: Some(record.id.to_string()),
            live_only: false,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        if crabber_session::ensure_storable(&record).is_ok()
            && crabber_session::ensure_storable(&event).is_err()
        {
            return record.arguments;
        }
    }
    panic!("no complete-record/event depth boundary found")
}

async fn boundary_contract(store: Arc<dyn Store>) -> Vec<(SessionId, EventRecord)> {
    let deep = event_depth_boundary();
    let (session, event, call, seen) = staged(store.clone(), deep.clone(), "echo").await;
    assert_eq!(seen.as_slice(), std::slice::from_ref(&deep));
    assert_eq!(call.arguments, deep);
    assert_eq!(call.status, crabber_core::ToolCallStatus::Completed);
    assert_eq!(
        call.result.unwrap().status,
        crabber_core::ToolResultStatus::Completed
    );
    assert!(event.payload.get("arguments").is_none());
    assert_eq!(event.payload["arguments_unavailable"], "event_not_storable");
    let escaped = json!({"text": r#"\u0000\"\\"#.repeat(100_000)});
    let (other, enriched, call, seen) = staged(store, escaped.clone(), "echo").await;
    assert_eq!(seen, [escaped]);
    assert_eq!(enriched.payload["arguments"], call.arguments);
    assert!(enriched.payload.get("arguments_unavailable").is_none());
    vec![(session, event), (other, enriched)]
}

#[tokio::test]
async fn memory_pending_event_depth_fallback_preserves_execution_and_large_escaped_input() {
    boundary_contract(Arc::new(MemoryStore::new())).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_pending_enriched_and_unavailable_events_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pending.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let store = Arc::new(crabber_session::SqliteStore::connect(&path).await.unwrap());
    let saved = boundary_contract(store).await;
    let reopened = crabber_session::SqliteStore::connect(&path).await.unwrap();
    for (session, expected) in saved {
        assert!(
            reopened
                .list_events(&session, None, 100)
                .await
                .unwrap()
                .contains(&expected)
        );
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_pending_enriched_and_unavailable_events_round_trip() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let store = Arc::new(crabber_session::PostgresStore::connect(&url).await.unwrap());
        let saved = boundary_contract(store.clone()).await;
        for (session, expected) in saved {
            assert!(
                store
                    .list_events(&session, None, 100)
                    .await
                    .unwrap()
                    .contains(&expected)
            );
        }
    })
    .await
    .unwrap();
}
