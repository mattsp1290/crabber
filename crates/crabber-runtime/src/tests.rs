use crate::{Observer, Orchestrator, PermissionDecision, Request, RuntimeError, StaticPolicy};
use async_trait::async_trait;
use crabber_core::{ContentBlock, EventKind, Message, Role, ToolCallId};
use crabber_extension::{ExtensionError, StaticPlanProvider, ToolDefinition, ToolExecutor};
use crabber_providers::{FakeProvider, Selection, StreamDelta};
use crabber_session::{MemoryStore, Store};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Notify, oneshot};

#[derive(Default)]
struct RecordingObserver(Mutex<Vec<EventKind>>);

impl Observer for RecordingObserver {
    fn emit(&self, event: &crabber_core::EventRecord) {
        self.0
            .lock()
            .expect("observer poisoned")
            .push(event.kind.clone());
    }
}

struct EchoTool(Arc<AtomicUsize>);

#[async_trait]
impl ToolExecutor for EchoTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(arguments)
    }
}

struct GatedTool {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
}

#[async_trait]
impl ToolExecutor for GatedTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        if let Some(sender) = self.entered.lock().expect("gate poisoned").take() {
            let _ = sender.send(());
        }
        self.release.notified().await;
        Ok(arguments)
    }
}

fn tool(executor: Arc<dyn ToolExecutor>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: crabber_core::ToolInfo {
            name: "echo".into(),
            description: "Echo input".into(),
            parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
            retry_safe: true,
            required_permissions: Vec::new(),
        },
        executor,
    })
}

fn request() -> Request {
    Request {
        session_id: None,
        workspace_id: "test".into(),
        directory: "/tmp".into(),
        title: "test".into(),
        text: "hello".into(),
        selection: Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
        system_prompt: None,
    }
}

fn orchestrator(
    store: Arc<MemoryStore>,
    fake: FakeProvider,
    executor: Arc<dyn ToolExecutor>,
    observer: Arc<RecordingObserver>,
) -> Orchestrator {
    Orchestrator::builder()
        .store(store)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(executor)],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .observer(observer)
        .build()
        .unwrap()
}

fn call_script(call_id: ToolCallId, arguments: &str) -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("before".into()),
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: "echo".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: arguments.into(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}

fn text_script(text: &str) -> Vec<StreamDelta> {
    vec![StreamDelta::TextDelta(text.into()), StreamDelta::Completed]
}

fn contains_text(messages: &[Message], text: &str) -> bool {
    messages
        .iter()
        .flat_map(|message| &message.parts)
        .any(|part| matches!(&part.content, ContentBlock::Text { text: actual } if actual == text))
}

#[tokio::test]
async fn text_tool_text_has_exact_order_and_three_generated_messages() {
    let store = Arc::new(MemoryStore::new());
    let observer = Arc::new(RecordingObserver::default());
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("after"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = orchestrator(
        Arc::clone(&store),
        fake.clone(),
        Arc::new(EchoTool(Arc::clone(&executed))),
        Arc::clone(&observer),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let result = handle.done().await.unwrap();
    assert_eq!(result.status, crabber_core::RunStatus::Completed);
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    let messages = store.list_messages(&session, None).await.unwrap();
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.role != Role::User)
            .count(),
        3
    );
    assert!(contains_text(&messages, "before"));
    assert!(contains_text(&messages, "after"));
    assert_eq!(fake.requests().len(), 2);
    assert_eq!(
        observer.0.lock().unwrap().as_slice(),
        &[
            EventKind::RunAdmitted,
            EventKind::RunStarted,
            EventKind::TurnStarted,
            EventKind::TextDelta,
            EventKind::MessageCommitted,
            EventKind::ToolCallPending,
            EventKind::ToolCallRunning,
            EventKind::ToolCallSettled,
            EventKind::TurnCompleted,
            EventKind::TurnStarted,
            EventKind::TextDelta,
            EventKind::MessageCommitted,
            EventKind::TurnCompleted,
            EventKind::RunSettled,
        ]
    );
    let durable = store.list_events(&session, None, 100).await.unwrap();
    assert!(
        durable
            .iter()
            .all(|event| !event.live_only && event.kind != EventKind::TextDelta)
    );
}

#[tokio::test]
async fn invalid_tool_arguments_settle_failed_and_continue() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), "{}"),
        text_script("recovered"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = orchestrator(
        Arc::clone(&store),
        fake.clone(),
        Arc::new(EchoTool(Arc::clone(&executed))),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    handle.done().await.unwrap();
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 2);
    let messages = store.list_messages(&session, None).await.unwrap();
    assert!(contains_text(&messages, "recovered"));
    assert!(
        messages
            .iter()
            .flat_map(|message| &message.parts)
            .any(|part| matches!(
                &part.content,
                ContentBlock::ToolResult { is_error: true, .. }
            ))
    );
    assert!(
        store
            .list_unfinished_tool_calls(&fake.requests()[0].identity.run_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn ask_policy_uses_default_deny_approver() {
    let store = Arc::new(MemoryStore::new());
    let observer = Arc::new(RecordingObserver::default());
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("after denial"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::clone(&executed))))],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .observer(Arc::clone(&observer) as Arc<dyn Observer>)
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    handle.done().await.unwrap();
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 2);
    assert!(
        observer
            .0
            .lock()
            .unwrap()
            .contains(&EventKind::PermissionRequested)
    );
    assert!(
        observer
            .0
            .lock()
            .unwrap()
            .contains(&EventKind::PermissionDecided)
    );
    let messages = store.list_messages(&session, None).await.unwrap();
    assert!(
        messages
            .iter()
            .flat_map(|message| &message.parts)
            .any(|part| matches!(
                &part.content,
                ContentBlock::ToolResult { is_error: true, .. }
            ))
    );
}

#[tokio::test]
async fn steering_and_follow_up_reach_later_requests_and_busy_is_rejected() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("second"),
        text_script("third"),
    ]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let gate = Arc::new(GatedTool {
        entered: Mutex::new(Some(entered_tx)),
        release: Arc::clone(&release),
    });
    let runtime = orchestrator(
        Arc::clone(&store),
        fake.clone(),
        gate,
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let mut busy = request();
    busy.session_id = Some(handle.session_id().clone());
    assert!(matches!(
        runtime.start(busy).await,
        Err(RuntimeError::SessionBusy)
    ));
    handle.steer("steer now").await.unwrap();
    handle.follow_up("follow later").await.unwrap();
    release.notify_one();
    handle.done().await.unwrap();
    let requests = fake.requests();
    assert_eq!(requests.len(), 3);
    assert!(contains_text(&requests[1].messages, "steer now"));
    assert!(!contains_text(&requests[1].messages, "follow later"));
    assert!(contains_text(&requests[2].messages, "follow later"));
}
