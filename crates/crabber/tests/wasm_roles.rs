#![cfg(feature = "wasm")]
use async_trait::async_trait;
#[cfg(feature = "postgres")]
use crabber::session::PostgresStore;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, ToolDefinition, ToolExecutor,
    core::{
        Clock, ContentBlock, EventKind, EventRecord, ManualClock, Message, MessageId, Part, PartId,
        PartKind, Role, RunId, RunStatus, SessionId, SystemClock, ToolCallId, ToolCallRecord,
        ToolCallStatus, ToolInfo,
    },
    extension::{Extension, Registrar, Scope},
    runtime::{InterruptPolicy, PermissionPolicy},
    session::{AdmitRequest, MemoryStore, Store},
    wasm::{InstanceMode, Limits, ModuleConfig},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn fixture(name: &str) -> ModuleConfig {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/wasm/target/wasm32-wasip2/release")
        .canonicalize()
        .expect("cargo xtask build-fixtures");
    let path = root.join(format!("{}.wasm", name.replace('-', "_")));
    let hash = Sha256::digest(std::fs::read(&path).unwrap()).into();
    ModuleConfig {
        name: name.into(),
        path,
        allowed_root: root,
        expected_sha256: hash,
        config_json: "{}".into(),
        limits: Limits::default(),
        instance_mode: InstanceMode::PerCall,
    }
}

fn call(name: &str, input: &Value) -> Vec<StreamDelta> {
    let id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: name.into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: input.to_string(),
        },
        StreamDelta::ToolCallDone { call_id: id },
        StreamDelta::Completed,
    ]
}

fn provider(script: Vec<StreamDelta>) -> FakeProvider {
    FakeProvider::scripted(vec![
        script,
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ])
}

fn config() -> AgentConfig {
    AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    })
}

struct CountTool(Arc<AtomicUsize>);

#[async_trait]
impl ToolExecutor for CountTool {
    async fn execute(&self, _input: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"value":"secret"}))
    }
}

fn native_tool(name: &str, calls: &Arc<AtomicUsize>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: name.into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(CountTool(Arc::clone(calls))),
    })
}

struct NativeResumeExtension(Arc<AtomicUsize>);

#[async_trait]
impl Extension for NativeResumeExtension {
    fn id(&self) -> &'static str {
        "test/native-resume"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        String::new()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(native_tool("native-resume", &self.0));
        Ok(())
    }
}

struct PausePolicy;

impl PermissionPolicy for PausePolicy {
    fn decide(&self, _tool: &ToolInfo, _arguments: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(&self, _tool: &ToolInfo, _arguments: &Value) -> InterruptPolicy {
        InterruptPolicy::Pause
    }
}

async fn fresh_agent_resumes_paused_run(store: Arc<dyn Store>, resume_store: Arc<dyn Store>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let first = Agent::builder()
        .store(Arc::clone(&store))
        .provider(Arc::new(provider(call("native-resume", &json!({})))))
        .config(config())
        .policy(Arc::new(PausePolicy))
        .extension(
            Arc::new(NativeResumeExtension(Arc::clone(&calls))),
            Scope::Global,
        )
        .wasm_extension(fixture("echo-tool"))
        .build()
        .unwrap();
    let paused = first
        .prompt(None, "pause")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(paused.status, RunStatus::Paused);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let fingerprint = store
        .get_run(&paused.run_id)
        .await
        .unwrap()
        .unwrap()
        .plan_fingerprint;
    drop(first);

    let second = Agent::builder()
        .store(Arc::clone(&resume_store))
        .provider(Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("resumed".into()),
            StreamDelta::Completed,
        ]])))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .extension(
            Arc::new(NativeResumeExtension(Arc::clone(&calls))),
            Scope::Global,
        )
        .wasm_extension(fixture("echo-tool"))
        .build()
        .unwrap();
    let result = second.resume(&paused.run_id).await.unwrap();
    assert_eq!(result.run_id, paused.run_id);
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        resume_store
            .get_run(&paused.run_id)
            .await
            .unwrap()
            .unwrap()
            .plan_fingerprint,
        fingerprint
    );
}

#[tokio::test]
async fn fresh_agent_resumes_native_and_wasm_plan() {
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new());
    fresh_agent_resumes_paused_run(Arc::clone(&store), store).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn freshly_connected_postgres_agent_resumes_native_and_wasm_plan() {
    let url = match std::env::var("CRABBER_TEST_POSTGRES_URL") {
        Ok(url) => url,
        Err(std::env::VarError::NotPresent)
            if std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref() == Ok("1") =>
        {
            panic!("CRABBER_TEST_POSTGRES_URL is required")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("CRABBER_TEST_POSTGRES_URL must be valid Unicode")
        }
    };
    PostgresStore::migrate(&url).await.unwrap();
    let first_store: Arc<dyn Store> = Arc::new(PostgresStore::connect(&url).await.unwrap());
    // Reconnect between admission and resume to exercise the durable host path.
    let second_store: Arc<dyn Store> = Arc::new(PostgresStore::connect(&url).await.unwrap());
    fresh_agent_resumes_paused_run(first_store, second_store).await;
}

async fn admit_expired_retry_safe_run(
    store: &Arc<dyn Store>,
    clock: &ManualClock,
    fingerprint: String,
) -> RunId {
    let session_id = SessionId::new();
    let message_id = MessageId::new();
    let now = clock.now();
    let admitted = store
        .admit_run(AdmitRequest {
            session_id: None,
            workspace_id: "test".into(),
            directory: ".".into(),
            title: "recovery".into(),
            user_message: Message {
                id: message_id.clone(),
                session_id,
                run_id: None,
                role: Role::User,
                parent_id: None,
                parts: vec![Part {
                    id: PartId::new(),
                    message_id,
                    ordinal: 0,
                    kind: PartKind::UserInputText,
                    content: ContentBlock::Text {
                        text: "recover".into(),
                    },
                }],
                created_at: now,
            },
            config_hash: "test".into(),
            plan_fingerprint: fingerprint,
            owner: "expired".into(),
            lease: Duration::from_secs(30),
        })
        .await
        .unwrap();
    let event = EventRecord {
        cursor: None,
        session_id: admitted.session.id.clone(),
        run_id: admitted.run.id.clone(),
        turn_id: None,
        kind: EventKind::ToolCallPending,
        payload: Value::Null,
        correlation: None,
        live_only: false,
        created_at: now,
    };
    store
        .execution(admitted.fence)
        .await
        .unwrap()
        .create_tool_call(
            ToolCallRecord {
                id: ToolCallId::new(),
                run_id: admitted.run.id.clone(),
                name: "native-resume".into(),
                arguments: json!({}),
                status: ToolCallStatus::Pending,
                retry_safe: true,
                result: None,
            },
            event,
        )
        .await
        .unwrap();
    clock.set(now + Duration::from_secs(31));
    admitted.run.id
}

async fn fresh_agent_recovers_expired_running_run(
    store: Arc<dyn Store>,
    resume_store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let first = Agent::builder()
        .store(Arc::clone(&store))
        .provider(Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("seed".into()),
            StreamDelta::Completed,
        ]])))
        .config(config())
        .extension(
            Arc::new(NativeResumeExtension(Arc::clone(&calls))),
            Scope::Global,
        )
        .wasm_extension(fixture("echo-tool"))
        .build()
        .unwrap();
    let seed = first
        .prompt(None, "seed")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let fingerprint = store
        .get_run(&seed.run_id)
        .await
        .unwrap()
        .unwrap()
        .plan_fingerprint;
    drop(first);
    let run_id = admit_expired_retry_safe_run(&store, &clock, fingerprint.clone()).await;

    let second = Agent::builder()
        .store(Arc::clone(&resume_store))
        .provider(Arc::new(FakeProvider::scripted(Vec::new())))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .extension(
            Arc::new(NativeResumeExtension(Arc::clone(&calls))),
            Scope::Global,
        )
        .wasm_extension(fixture("echo-tool"))
        .build()
        .unwrap();
    let recovered = second.recover().await.unwrap().recovered;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].run_id, run_id);
    assert_eq!(recovered[0].status, RunStatus::Interrupted);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        resume_store
            .get_run(&run_id)
            .await
            .unwrap()
            .unwrap()
            .plan_fingerprint,
        fingerprint
    );
}

#[tokio::test]
async fn fresh_agent_recovers_native_and_wasm_plan() {
    let clock = Arc::new(ManualClock::new(
        SystemClock.now() - Duration::from_hours(24),
    ));
    let store: Arc<dyn Store> = Arc::new(MemoryStore::with_clock(clock.clone()));
    fresh_agent_recovers_expired_running_run(Arc::clone(&store), store, clock).await;
}

#[tokio::test]
async fn echo_executes_through_runtime() {
    let fake = provider(call("echo", &json!({"value":"hello"})));
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .store(store.clone())
        .provider(Arc::new(fake))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .wasm_extension(fixture("echo-tool"))
        .build()
        .unwrap();
    let result = agent
        .prompt(None, "echo")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let messages = store.list_messages(&result.session_id, None).await.unwrap();
    assert!(serde_json::to_string(&messages).unwrap().contains("hello"));
}

#[tokio::test]
async fn policy_denies_dangerous_tool() {
    let fake = provider(call("dangerous", &json!({})));
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(fake))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool(native_tool("dangerous", &calls))
        .wasm_extension(fixture("deny-policy"))
        .build()
        .unwrap();
    agent
        .prompt(None, "dangerous")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn guest_allow_overrides_host_deny_and_ask_requires_approval() {
    let calls = Arc::new(AtomicUsize::new(0));
    let allow_agent = Agent::builder()
        .memory()
        .provider(Arc::new(provider(call("safe", &json!({})))))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Deny)))
        .tool(native_tool("safe", &calls))
        .wasm_extension(fixture("deny-policy"))
        .build()
        .unwrap();
    allow_agent
        .prompt(None, "safe")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let ask_agent = Agent::builder()
        .memory()
        .provider(Arc::new(provider(call("ask_me", &json!({})))))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool(native_tool("ask_me", &calls))
        .wasm_extension(fixture("deny-policy"))
        .build()
        .unwrap();
    ask_agent
        .prompt(None, "ask_me")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn policy_guest_receives_permission_and_run_context() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut info = native_tool("contextual", &calls).info.clone();
    info.required_permissions = vec!["network".into()];
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(provider(call("contextual", &json!({})))))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Deny)))
        .tool(Arc::new(ToolDefinition {
            info,
            executor: Arc::new(CountTool(Arc::clone(&calls))),
        }))
        .wasm_extension(fixture("deny-policy"))
        .build()
        .unwrap();
    agent
        .prompt(None, "contextual")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn policy_guest_state_persists_across_runs() {
    let fake = FakeProvider::scripted(vec![
        call("stateful", &json!({})),
        vec![StreamDelta::TextDelta("one".into()), StreamDelta::Completed],
        call("stateful", &json!({})),
        vec![StreamDelta::TextDelta("two".into()), StreamDelta::Completed],
    ]);
    let store = Arc::new(MemoryStore::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .store(store.clone())
        .provider(Arc::new(fake))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Deny)))
        .tool(native_tool("stateful", &calls))
        .wasm_extension(fixture("all-in-one"))
        .build()
        .unwrap();
    let first = agent
        .prompt(None, "first")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let state = store
        .get_extension_state("all-in-one", &first.session_id)
        .await
        .unwrap();
    assert_eq!(state.get("policy-count").map(String::as_str), Some("1"));
    agent
        .prompt(Some(first.session_id.clone()), "second")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let state = store
        .get_extension_state("all-in-one", &first.session_id)
        .await
        .unwrap();
    assert_eq!(state.get("policy-count").map(String::as_str), Some("2"));
}

#[tokio::test]
async fn prompt_section_renders_for_each_run_with_its_run_id() {
    let fake = FakeProvider::scripted(vec![
        vec![StreamDelta::TextDelta("one".into()), StreamDelta::Completed],
        vec![StreamDelta::TextDelta("two".into()), StreamDelta::Completed],
    ]);
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(fake.clone()))
        .config(config())
        .wasm_extension(fixture("all-in-one"))
        .build()
        .unwrap();
    let first = agent.prompt(None, "first").await.unwrap();
    let first_id = first.run_id().to_string();
    first.done().await.unwrap();
    let second = agent.prompt(None, "second").await.unwrap();
    let second_id = second.run_id().to_string();
    second.done().await.unwrap();
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0]
            .system
            .as_deref()
            .unwrap_or_default()
            .contains(&first_id)
    );
    assert!(
        requests[1]
            .system
            .as_deref()
            .unwrap_or_default()
            .contains(&second_id)
    );
    assert_ne!(first_id, second_id);
}

#[tokio::test]
async fn banner_reaches_model_request() {
    let fake = FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]]);
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(fake.clone()))
        .config(config())
        .wasm_extension(fixture("banner-context"))
        .build()
        .unwrap();
    agent
        .prompt(None, "hello")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert!(
        fake.requests()[0]
            .system
            .as_deref()
            .unwrap_or_default()
            .contains("banner turn")
    );
}

#[tokio::test]
async fn middleware_rewrites_tool_output() {
    let fake = provider(call("native", &json!({})));
    let store = Arc::new(MemoryStore::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .store(store.clone())
        .provider(Arc::new(fake))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool(native_tool("native", &calls))
        .wasm_extension(fixture("redact-middleware"))
        .build()
        .unwrap();
    let result = agent
        .prompt(None, "native")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let messages = store.list_messages(&result.session_id, None).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        serde_json::to_string(&messages)
            .unwrap()
            .contains("[REDACTED]")
    );
}
