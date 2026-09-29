use crate::{
    ApprovalRequester, ModelStream, Observer, Orchestrator, PermissionDecision, Request,
    RuntimeError, StaticPolicy,
};
use async_trait::async_trait;
use crabber_core::{ContentBlock, EventKind, ManualClock, Message, Role, ToolCallId};
use crabber_extension::{ExtensionError, StaticPlanProvider, ToolDefinition, ToolExecutor};
use crabber_providers::{
    DeltaStream, FakeProvider, ModelRequest, ProviderError, Selection, StreamDelta, Streamer,
};
use crabber_session::{MemoryStore, Store};
use serde_json::{Value, json};
use std::sync::{
    Arc, Condvar, Mutex,
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

struct CancellableTool {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
    completed: Arc<AtomicUsize>,
}

#[async_trait]
impl ToolExecutor for CancellableTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        if let Some(sender) = self.entered.lock().expect("tool gate poisoned").take() {
            let _ = sender.send(());
        }
        self.release.notified().await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(arguments)
    }
}

struct BlockingSettledObserver {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    released: Arc<(Mutex<bool>, Condvar)>,
}

impl Observer for BlockingSettledObserver {
    fn emit(&self, event: &crabber_core::EventRecord) {
        if event.kind == EventKind::RunSettled {
            if let Some(sender) = self.entered.lock().expect("observer gate poisoned").take() {
                let _ = sender.send(());
            }
            let (lock, ready) = &*self.released;
            let mut released = lock.lock().expect("observer gate poisoned");
            while !*released {
                released = ready.wait(released).expect("observer gate poisoned");
            }
        }
    }
}

struct GatedModelStream {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
}

#[async_trait]
impl ModelStream for GatedModelStream {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        let entered = self.entered.lock().expect("model gate poisoned").take();
        if let Some(sender) = entered {
            let _ = sender.send(());
            self.release.notified().await;
        }
        next.stream(request).await
    }
}

struct GatedApprover {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
}

#[async_trait]
impl ApprovalRequester for GatedApprover {
    async fn approve(&self, _tool: &crabber_core::ToolInfo, _arguments: &Value) -> bool {
        if let Some(sender) = self.entered.lock().expect("approval gate poisoned").take() {
            let _ = sender.send(());
        }
        self.release.notified().await;
        true
    }
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
    assert_eq!(
        durable
            .iter()
            .map(|event| event.kind.clone())
            .collect::<Vec<_>>(),
        vec![
            EventKind::RunAdmitted,
            EventKind::RunStarted,
            EventKind::TurnStarted,
            EventKind::MessageCommitted,
            EventKind::ToolCallPending,
            EventKind::ToolCallRunning,
            EventKind::ToolCallSettled,
            EventKind::TurnCompleted,
            EventKind::TurnStarted,
            EventKind::MessageCommitted,
            EventKind::TurnCompleted,
            EventKind::RunSettled,
        ]
    );
    assert!(durable.iter().all(|event| !event.live_only));
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

#[tokio::test]
async fn enum_and_nested_schema_failures_never_execute_tool() {
    for arguments in [
        r#"{"choice":"forbidden","nested":{"flag":true}}"#,
        r#"{"choice":"allowed","nested":{}}"#,
    ] {
        let store = Arc::new(MemoryStore::new());
        let fake = FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), arguments),
            text_script("continued"),
        ]);
        let executed = Arc::new(AtomicUsize::new(0));
        let mut definition = tool(Arc::new(EchoTool(Arc::clone(&executed))));
        Arc::get_mut(&mut definition).unwrap().info.parameters = json!({
            "type":"object", "required":["choice","nested"],
            "properties":{
                "choice":{"enum":["allowed"]},
                "nested":{"type":"object","required":["flag"],"properties":{"flag":{"type":"boolean"}}}
            }
        });
        let runtime = Orchestrator::builder()
            .store(Arc::clone(&store) as Arc<dyn Store>)
            .resolver(Arc::new(fake.clone()))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![definition],
                Vec::new(),
            )))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .build()
            .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        handle.done().await.unwrap();
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert_eq!(fake.requests().len(), 2);
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
}

#[tokio::test]
async fn stream_eof_without_completed_fails_without_assistant_commit() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![vec![StreamDelta::TextDelta("partial".into())]]);
    let runtime = orchestrator(
        Arc::clone(&store),
        fake,
        Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::Provider(_))
    ));
    let messages = store.list_messages(&session, None).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].role, Role::User);
    assert!(store.list_unfinished_runs().await.unwrap().is_empty());
    assert_eq!(
        store
            .list_events(&session, None, 100)
            .await
            .unwrap()
            .iter()
            .map(|event| event.kind.clone())
            .collect::<Vec<_>>(),
        vec![
            EventKind::RunAdmitted,
            EventKind::RunStarted,
            EventKind::TurnStarted,
            EventKind::RunSettled,
        ]
    );
}

#[tokio::test]
async fn heartbeat_keeps_gated_tool_run_owned_past_initial_lease() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("finished"),
        text_script("another run"),
    ]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(GatedTool {
                entered: Mutex::new(Some(entered_tx)),
                release: Arc::clone(&release),
            }))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(25));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if store.list_unfinished_runs().await.unwrap()[0].lease_until
                > now + time::Duration::seconds(50)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    clock.set(now + time::Duration::seconds(35));
    release.notify_one();
    assert_eq!(
        handle.done().await.unwrap().status,
        crabber_core::RunStatus::Completed
    );
    let mut next = request();
    next.session_id = Some(session);
    assert_eq!(
        runtime
            .start(next)
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
            .status,
        crabber_core::RunStatus::Completed
    );
}

#[tokio::test]
async fn heartbeat_covers_model_stream_and_approval_waits() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("done"),
    ]);
    let (model_tx, model_rx) = oneshot::channel();
    let (approval_tx, approval_rx) = oneshot::channel();
    let model_release = Arc::new(Notify::new());
    let approval_release = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .model_stream(Arc::new(GatedModelStream {
            entered: Mutex::new(Some(model_tx)),
            release: Arc::clone(&model_release),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .approver(Arc::new(GatedApprover {
            entered: Mutex::new(Some(approval_tx)),
            release: Arc::clone(&approval_release),
        }))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), model_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(25));
    wait_for_lease(&store, now + time::Duration::seconds(50)).await;
    model_release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), approval_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(50));
    wait_for_lease(&store, now + time::Duration::seconds(75)).await;
    approval_release.notify_one();
    assert_eq!(
        handle.done().await.unwrap().status,
        crabber_core::RunStatus::Completed
    );
}

async fn wait_for_lease(store: &MemoryStore, threshold: time::OffsetDateTime) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if store.list_unfinished_runs().await.unwrap()[0].lease_until > threshold {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn heartbeat_interval_requires_margin_inside_lease() {
    for interval in [
        std::time::Duration::ZERO,
        std::time::Duration::from_secs(15),
        std::time::Duration::from_secs(30),
    ] {
        assert!(matches!(
            Orchestrator::builder().heartbeat_interval(interval).build(),
            Err(RuntimeError::InvalidConfiguration(_))
        ));
    }
}

#[tokio::test]
async fn reclaimed_lease_cancels_pending_approval_before_tool_body() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("must not run"),
    ]);
    let executed = Arc::new(AtomicUsize::new(0));
    let (approval_tx, approval_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::clone(&executed))))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .approver(Arc::new(GatedApprover {
            entered: Mutex::new(Some(approval_tx)),
            release: Arc::clone(&release),
        }))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), approval_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(31));
    let new_fence = store
        .claim_expired_run(&run_id, "replacement owner")
        .await
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
        .await
        .unwrap();
    assert!(matches!(result, Err(RuntimeError::LeaseLost)));
    release.notify_one();
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 1);
    let kinds = store
        .list_events(&session, None, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(kinds.contains(&EventKind::PermissionRequested));
    assert!(!kinds.contains(&EventKind::PermissionDecided));
    assert!(!kinds.contains(&EventKind::ToolCallSettled));
    assert!(!kinds.contains(&EventKind::RunSettled));
    let unfinished = store.list_unfinished_runs().await.unwrap();
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].owner, "replacement owner");
    assert_eq!(unfinished[0].claim_token, new_fence.claim_token);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_settlement_wins_heartbeat_tick_during_observer_callback() {
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![text_script("done")]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let released = Arc::new((Mutex::new(false), Condvar::new()));
    let observer = Arc::new(BlockingSettledObserver {
        entered: Mutex::new(Some(entered_tx)),
        released: Arc::clone(&released),
    });
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .observer(observer)
        .heartbeat_interval(std::time::Duration::from_millis(5))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().status,
        crabber_core::RunStatus::Completed
    );
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    {
        let (lock, ready) = &*released;
        *lock.lock().unwrap() = true;
        ready.notify_all();
    }
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        crabber_core::RunStatus::Completed
    );
}

#[tokio::test]
async fn reclaimed_lease_cancels_pending_tool_body() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
        text_script("must not run"),
    ]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .store(Arc::clone(&store) as Arc<dyn Store>)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(CancellableTool {
                entered: Mutex::new(Some(entered_tx)),
                release: Arc::clone(&release),
                completed: Arc::clone(&completed),
            }))],
            Vec::new(),
        )))
        .clock(clock.clone())
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    clock.set(now + time::Duration::seconds(31));
    let new_fence = store
        .claim_expired_run(&run_id, "replacement owner")
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
            .await
            .unwrap(),
        Err(RuntimeError::LeaseLost)
    ));
    release.notify_one();
    tokio::task::yield_now().await;
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 1);
    let kinds = store
        .list_events(&session, None, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(!kinds.contains(&EventKind::ToolCallSettled));
    assert!(!kinds.contains(&EventKind::RunSettled));
    assert_eq!(
        store.get_run(&run_id).await.unwrap().unwrap().claim_token,
        new_fence.claim_token
    );
}
