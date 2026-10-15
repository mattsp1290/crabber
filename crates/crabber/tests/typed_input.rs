use async_trait::async_trait;
use crabber::core::{
    AdmissionKind, ContentBlock, InputContent, InputContext, InputEnvelope, InputMessage,
    InputPart, InputState, MediaKind, MediaSource, MessageContent, Role, SessionContext, SessionId,
    SessionRevision, ToolCallId, ToolInfo,
};
use crabber::session::{MemoryStore, Store, StoreError};
use crabber::{
    Admission, AdmissionKey, AdmissionOptions, Agent, AgentConfig, ExecutionContext, FakeProvider,
    InputFingerprint, PermissionDecision, Selection, StaticPolicy, StreamDelta, ToolDefinition,
    ToolExecutor,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct CountedEcho(Arc<AtomicUsize>);
#[async_trait]
impl ToolExecutor for CountedEcho {
    async fn execute(
        &self,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, crabber::ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(input)
    }
}
fn input(session: &SessionId, revision: SessionRevision) -> InputEnvelope {
    InputEnvelope::new(
        vec![
            InputMessage {
                id: Some(format!("provided-user-{}", session.0)),
                parent_id: None,
                role: Role::User,
                parts: vec![
                    InputPart {
                        id: Some(format!("provided-caption-{}", session.0)),
                        content: InputContent::Content {
                            content: MessageContent::Text {
                                text: "caption".into(),
                            },
                        },
                    },
                    InputPart {
                        id: None,
                        content: InputContent::Content {
                            content: MessageContent::Media {
                                kind: MediaKind::Image,
                                mime_type: "image/png".into(),
                                source: MediaSource::File {
                                    file_id: "opaque-file".into(),
                                },
                            },
                        },
                    },
                ],
            },
            InputMessage {
                id: None,
                parent_id: Some(format!("provided-user-{}", session.0)),
                role: Role::Assistant,
                parts: vec![InputPart {
                    id: None,
                    content: InputContent::Content {
                        content: MessageContent::Text {
                            text: "host-approved history".into(),
                        },
                    },
                }],
            },
        ],
        vec![],
        vec![InputContext {
            description: "host data".into(),
            value: json!({"kept":"\u{0}","ordered":[null,1]}),
        }],
        InputState::Present {
            value: serde_json::Value::Null,
        },
        revision,
        AdmissionKind::Ordinary,
    )
    .unwrap()
}
#[tokio::test]
async fn public_typed_loop_preserves_media_provenance_context_and_closed_replay() {
    let call = ToolCallId::new();
    let provider = Arc::new(FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call.clone(),
                name: "echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call.clone(),
                text: "{\"value\":42}".into(),
            },
            StreamDelta::ToolCallDone { call_id: call },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ]));
    let effects = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(MemoryStore::new());
    let selection = Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    };
    let agent = build_agent(
        store.clone(),
        provider.clone(),
        effects.clone(),
        Arc::new(StaticPolicy::new(PermissionDecision::Allow)),
        AgentConfig::new(selection.clone()),
    );
    let session = SessionId::new();
    let execution = ExecutionContext::new(
        SessionContext::new(session.clone(), "typed-workspace", "/typed"),
        selection,
    );
    let original = input(&session, SessionRevision::INITIAL);
    let options = options(&agent);
    let Admission::Started { receipt, handle } = agent
        .prompt_input_keyed_in(&execution, original.clone(), options.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh typed input must start")
    };
    handle.done().await.unwrap();
    assert_eq!(receipt.semantic_digest_version, 2);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_request_input(&requests, &original, &session);
    let retained = store
        .execution_binding(&session, &receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.capsule.version, 3);
    assert_eq!(retained.capsule.input_envelope, Some(original.clone()));
    let messages = store.list_all_messages(&session).await.unwrap();
    let revision = store.session_revision(&session).await.unwrap();
    let events = store.list_events(&session, None, 1000).await.unwrap();
    let mut stale = options.clone();
    stale.key = AdmissionKey::new("stale-snapshot").unwrap();
    assert!(matches!(
        agent
            .prompt_input_keyed_in(&execution, original.clone(), stale, None)
            .await,
        Err(crabber::RuntimeError::Store(
            StoreError::AdmissionRevisionConflict
        ))
    ));
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    assert_eq!(store.list_all_messages(&session).await.unwrap(), messages);
    agent.close_extensions().await.unwrap();
    assert!(
        matches!(agent.prompt_input_keyed_in(&execution, original.clone(), options.clone(), None).await.unwrap(), Admission::Replayed(actual) if actual == receipt)
    );
    assert!(
        matches!(agent.recover_input_admission_in(&execution, original, options, None).await.unwrap(), Admission::Replayed(actual) if actual == receipt)
    );
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    assert_eq!(
        store.list_events(&session, None, 1000).await.unwrap(),
        events
    );
}

fn assert_request_input(
    requests: &[crabber::providers::ModelRequest],
    original: &InputEnvelope,
    session: &SessionId,
) {
    for request in requests {
        assert_eq!(request.context, original.context());
        assert_eq!(
            request.messages[0].id.0,
            format!("provided-user-{}", session.0)
        );
        assert_eq!(
            request.messages[0].parts[0].id.0,
            format!("provided-caption-{}", session.0)
        );
        assert_eq!(
            request.messages[0].parts[1].content,
            ContentBlock::MediaContent {
                kind: MediaKind::Image,
                mime_type: "image/png".into(),
                source: MediaSource::File {
                    file_id: "opaque-file".into()
                }
            }
        );
        assert_eq!(
            request.messages[1].parent_id.as_ref().unwrap(),
            &request.messages[0].id
        );
        assert_eq!(request.messages[1].role, Role::Assistant);
    }
}

fn options(agent: &Agent) -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("typed-loop").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: agent.behavior_fingerprint().clone(),
    }
}

fn build_agent(
    store: Arc<MemoryStore>,
    provider: Arc<FakeProvider>,
    effects: Arc<AtomicUsize>,
    policy: Arc<dyn crabber::runtime::PermissionPolicy>,
    config: AgentConfig,
) -> Agent {
    Agent::builder()
        .behavior_version("typed-input-loop.v1")
        .store(store)
        .provider(provider)
        .config(config)
        .policy(policy)
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "echo".into(),
                description: "echo".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(CountedEcho(effects)),
        }))
        .build()
        .unwrap()
}

struct Pause;
impl crabber::runtime::PermissionPolicy for Pause {
    fn decide(&self, _: &ToolInfo, _: &serde_json::Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(
        &self,
        _: &ToolInfo,
        _: &serde_json::Value,
    ) -> crabber::runtime::InterruptPolicy {
        crabber::runtime::InterruptPolicy::Pause
    }
}
#[tokio::test]
async fn fresh_agent_resumes_original_typed_input_and_checkpoint_under_changed_defaults() {
    let call = ToolCallId::new();
    let provider = Arc::new(FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call.clone(),
                name: "echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone { call_id: call },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("resumed".into()),
            StreamDelta::Completed,
        ],
    ]));
    let store = Arc::new(MemoryStore::new());
    let effects = Arc::new(AtomicUsize::new(0));
    let selection = Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    };
    let agent = build_agent(
        store.clone(),
        provider.clone(),
        effects.clone(),
        Arc::new(Pause),
        AgentConfig::new(selection.clone()),
    );
    let session = SessionId::new();
    let context = ExecutionContext::new(
        SessionContext::new(session.clone(), "original-workspace", "/original"),
        selection,
    );
    let original = input(&session, SessionRevision::INITIAL);
    let Admission::Started { handle, .. } = agent
        .prompt_input_keyed_in(&context, original.clone(), options(&agent), None)
        .await
        .unwrap()
    else {
        panic!("fresh typed admission must start")
    };
    let paused = handle.done().await.unwrap();
    assert_eq!(paused.status, crabber::core::RunStatus::Paused);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let saved = store
        .get_run(&paused.run_id)
        .await
        .unwrap()
        .unwrap()
        .checkpoint
        .unwrap();
    assert_eq!(saved["request"]["version"], 2);
    assert_eq!(saved["request"]["input_digest"].as_str().unwrap().len(), 64);
    assert!(saved["request"].get("input_envelope").is_none());
    agent.close_extensions().await.unwrap();
    let mut defaults = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "new-default-model".into(),
    });
    defaults.workspace_id = "new-default-workspace".into();
    defaults.directory = "/new-default".into();
    defaults.system_prompt = Some("new default system prompt".into());
    defaults.max_output_tokens = Some(1);
    let fresh = build_agent(
        store.clone(),
        provider.clone(),
        effects.clone(),
        Arc::new(Pause),
        defaults,
    );
    assert_eq!(
        fresh.resume(&paused.run_id).await.unwrap().status,
        crabber::core::RunStatus::Completed
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_request_input(&requests, &original, &session);
    assert!(
        requests
            .iter()
            .all(|request| request.selection.model_id == "scripted"
                && request.max_tokens.is_none()
                && request.system.is_none())
    );
    assert_eq!(
        store
            .execution_binding(&session, &paused.run_id)
            .await
            .unwrap()
            .unwrap()
            .capsule
            .input_envelope,
        Some(original)
    );
}
