use crabber::{
    Admission, AdmissionKey, AdmissionOptions, Agent, AgentConfig, FakeProvider, InputFingerprint,
    RuntimeError, Selection, SessionId, StreamDelta,
    core::{CoreError, Role},
    extension::PromptSection,
    runtime::{CompactionPolicy, ExecutionMode},
    session::{MemoryStore, Store},
};
use std::sync::Arc;

fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("test-key").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}
fn config() -> AgentConfig {
    AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    })
}
fn build(store: Arc<MemoryStore>, provider: Arc<FakeProvider>, config: AgentConfig) -> Agent {
    Agent::builder()
        .store(store)
        .provider(provider)
        .config(config)
        .build()
        .unwrap()
}
fn provider() -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]]))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_and_terminal_retries_execute_once() {
    let store = Arc::new(MemoryStore::new());
    let provider = provider();
    let agent = Arc::new(build(store.clone(), provider.clone(), config()));
    let session = SessionId::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(24));
    let mut tasks = Vec::new();
    for _ in 0..24 {
        let agent = agent.clone();
        let session = session.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            agent
                .prompt_keyed(session, "hello", options())
                .await
                .unwrap()
        }));
    }
    let mut receipt = None;
    let mut handle = None;
    for task in tasks {
        let outcome = task.await.unwrap();
        if let Some(expected) = &receipt {
            assert_eq!(outcome.receipt(), expected);
        }
        receipt = Some(outcome.receipt().clone());
        if let Admission::Started {
            handle: started, ..
        } = outcome
        {
            assert!(handle.is_none());
            handle = Some(started);
        }
    }
    let result = handle.unwrap().done().await.unwrap();
    let replay = agent
        .prompt_keyed(session.clone(), "hello", options())
        .await
        .unwrap();
    assert!(matches!(replay, Admission::Replayed(_)));
    assert_eq!(Some(replay.receipt().clone()), receipt);
    assert_eq!(replay.receipt().run_id, result.run_id);
    assert_eq!(
        agent
            .lookup_admission(&session, &options().key)
            .await
            .unwrap(),
        receipt
    );
    let messages = store.list_all_messages(&session).await.unwrap();
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.role == Role::User)
            .count(),
        1
    );
    assert!(
        messages
            .iter()
            .all(|message| message.run_id.as_ref() == Some(&result.run_id))
    );
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn stale_claim_cannot_hide_changed_inspectable_semantics() {
    let store = Arc::new(MemoryStore::new());
    let provider = provider();
    let agent = build(store.clone(), provider.clone(), config());
    let session = SessionId::new();
    let Admission::Started { handle, .. } = agent
        .prompt_keyed(session.clone(), "hello", options())
        .await
        .unwrap()
    else {
        panic!()
    };
    handle.done().await.unwrap();
    for change in 0..12 {
        let mut config = config();
        let mut text = "hello";
        let mut opts = options();
        match change {
            0 => text = "changed",
            1 => config.system_prompt = Some("changed".into()),
            2 => config.selection.model_id = "changed".into(),
            3 => config.selection.provider_id = "changed".into(),
            4 => config.title = "changed".into(),
            5 => opts.behavior_fingerprint = InputFingerprint::new("c".repeat(64)).unwrap(),
            9 => opts.fingerprint = InputFingerprint::new("d".repeat(64)).unwrap(),
            _ => {}
        }
        let mut builder = Agent::builder()
            .store(store.clone())
            .provider(provider.clone())
            .config(config);
        match change {
            6 => builder = builder.execution_mode(ExecutionMode::Parallel { max: 2 }),
            7 => {
                builder = builder.compaction(CompactionPolicy {
                    trigger_ratio: 0.7,
                    keep_tail_messages: 4,
                });
            }
            8 => {
                builder = builder.prompt_section(Arc::new(PromptSection {
                    name: "new".into(),
                    order: 0,
                    text: "new semantics".into(),
                }));
            }
            10 => {
                builder = builder.tool(Arc::new(crabber::ToolDefinition {
                    info: crabber::core::ToolInfo {
                        name: "new-tool".into(),
                        description: "new".into(),
                        parameters: serde_json::json!({"type":"object"}),
                        retry_safe: false,
                        required_permissions: vec![],
                    },
                    executor: Arc::new(UnusedTool),
                }));
            }
            11 => {
                builder = builder.extension(
                    Arc::new(VersionedExtension),
                    crabber::extension::Scope::Global,
                );
            }
            _ => {}
        }
        assert!(
            matches!(
                builder
                    .build()
                    .unwrap()
                    .prompt_keyed(session.clone(), text, opts)
                    .await,
                Err(RuntimeError::Store(CoreError::AdmissionConflict))
            ),
            "change {change}"
        );
    }
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn identity_and_unkeyed_compatibility() {
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed
        ];
        3
    ]));
    let agent = build(store.clone(), provider.clone(), config());
    let legacy = agent
        .prompt(None, "legacy")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let Admission::Started { handle, receipt } = agent
        .prompt_keyed(legacy.session_id.clone(), "keyed", options())
        .await
        .unwrap()
    else {
        panic!()
    };
    handle.done().await.unwrap();
    assert_eq!(receipt.session_id, legacy.session_id);
    for directory in [false, true] {
        let mut config = config();
        if directory {
            config.directory = "/different".into();
        } else {
            config.workspace_id = "different".into();
        }
        let changed = build(store.clone(), provider.clone(), config);
        assert!(matches!(
            changed
                .prompt_keyed(legacy.session_id.clone(), "keyed", options())
                .await,
            Err(RuntimeError::Store(CoreError::SessionIdentityMismatch))
        ));
    }
    agent
        .prompt(Some(legacy.session_id), "legacy continues")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(provider.requests().len(), 3);
}

#[test]
fn metadata_validation_and_redaction() {
    for value in ["", "contains spaces", "line\nbreak", &"a".repeat(129)] {
        let error = AdmissionKey::new(value).unwrap_err();
        assert_eq!(error.to_string(), "invalid input: invalid admission key");
        assert!(
            serde_json::from_str::<AdmissionKey>(&serde_json::to_string(value).unwrap()).is_err()
        );
    }
    for value in ["", "secret", &"f".repeat(65), &"G".repeat(64)] {
        assert!(InputFingerprint::new(value).is_err());
        assert!(
            serde_json::from_str::<InputFingerprint>(&serde_json::to_string(value).unwrap())
                .is_err()
        );
    }
    assert!(!format!("{:?}", options()).contains("test-key"));
    assert!(!format!("{:?}", options()).contains(&"a".repeat(64)));
}

struct UnusedTool;
#[async_trait::async_trait]
impl crabber::ToolExecutor for UnusedTool {
    async fn execute(
        &self,
        _: serde_json::Value,
    ) -> Result<serde_json::Value, crabber::ExtensionError> {
        panic!("conflicting admission must not execute tools")
    }
}
struct VersionedExtension;
#[async_trait::async_trait]
impl crabber::extension::Extension for VersionedExtension {
    fn config_hash(&self) -> String {
        "versioned-config".into()
    }
    fn id(&self) -> &'static str {
        "new-extension"
    }
    fn version(&self) -> &'static str {
        "2"
    }
    async fn install(
        &self,
        _: &mut crabber::extension::Registrar,
    ) -> Result<(), crabber::ExtensionError> {
        Ok(())
    }
}
