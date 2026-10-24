use crabber::{
    Admission, AdmissionKey, AdmissionOptions, Agent, AgentConfig, ExecutionContext, FakeProvider,
    InputFingerprint, ModelControls, ReasoningEffort, RuntimeError, Selection, SessionContext,
    SessionId, StreamDelta,
    session::{MemoryStore, StoreError},
};
use std::sync::Arc;
fn selection() -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: "model".into(),
    }
}
fn controls(effort: ReasoningEffort) -> ModelControls {
    ModelControls::default().reasoning_effort(effort)
}
fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("key").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}
fn provider(count: usize) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(
        (0..count)
            .map(|_| {
                vec![
                    StreamDelta::TextDelta("done".into()),
                    StreamDelta::Completed,
                ]
            })
            .collect(),
    ))
}
fn agent(provider: Arc<FakeProvider>, config: AgentConfig) -> Agent {
    Agent::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(Arc::new(MemoryStore::new()))
        .provider(provider)
        .config(config)
        .build()
        .unwrap()
}
#[tokio::test]
async fn agent_default_is_captured() {
    for effort in [None, Some(ReasoningEffort::High)] {
        let provider = provider(1);
        let mut config = AgentConfig::new(selection());
        if let Some(effort) = effort {
            config = config.model_controls(controls(effort));
        }
        let agent = agent(provider.clone(), config);
        agent
            .prompt(None, "hello")
            .await
            .unwrap()
            .done()
            .await
            .unwrap();
        assert_eq!(provider.requests()[0].reasoning_effort, effort);
    }
}
#[tokio::test]
async fn contexts_select_effort_and_changed_keyed_retry_conflicts() {
    let provider = provider(4);
    let agent = agent(provider.clone(), AgentConfig::new(selection()));
    let context = ExecutionContext::new(
        SessionContext::new(SessionId::new(), "default", "."),
        selection(),
    )
    .with_model_controls(controls(ReasoningEffort::Low));
    let high = context
        .clone()
        .with_model_controls(controls(ReasoningEffort::High));
    assert_ne!(context, high);
    let Admission::Started { handle, .. } = agent
        .prompt_keyed_in(&context, "hello", options(), None)
        .await
        .unwrap()
    else {
        panic!("new admission")
    };
    handle.done().await.unwrap();
    assert!(matches!(
        agent
            .prompt_keyed_in(&context, "hello", options(), None)
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
    assert_eq!(provider.requests().len(), 1);
    assert!(matches!(
        agent.prompt_keyed_in(&high, "hello", options(), None).await,
        Err(RuntimeError::Store(StoreError::AdmissionConflict))
    ));
    for context in [&context, &high] {
        agent
            .prompt_in(context, "next", None)
            .await
            .unwrap()
            .done()
            .await
            .unwrap();
    }
    let efforts: Vec<_> = provider
        .requests()
        .iter()
        .map(|r| r.reasoning_effort)
        .collect();
    assert_eq!(
        efforts,
        vec![
            Some(ReasoningEffort::Low),
            Some(ReasoningEffort::Low),
            Some(ReasoningEffort::High)
        ]
    );
}
#[tokio::test]
async fn stored_selection_defaults_keep_context_controls() {
    use crabber::{
        core::{ControlId, SessionRevision},
        session::{ManageSession, SessionChange, SessionDefaults, Store},
    };
    let store = Arc::new(MemoryStore::new());
    let session = SessionId::new();
    store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "default".into(),
                directory: ".".into(),
                title: "defaults".into(),
                defaults: SessionDefaults {
                    provider_id: Some("fake".into()),
                    model_id: Some("stored".into()),
                },
            },
        })
        .await
        .unwrap();
    let provider = provider(2);
    let agent = Agent::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(store)
        .provider(provider.clone())
        .config(AgentConfig::new(selection()).model_controls(controls(ReasoningEffort::High)))
        .build()
        .unwrap();
    let context = ExecutionContext::new(SessionContext::new(session, "default", "."), selection())
        .with_model_controls(controls(ReasoningEffort::Medium))
        .with_session_defaults();
    let Admission::Started { handle, .. } = agent
        .prompt_keyed_in(&context, "hello", options(), None)
        .await
        .unwrap()
    else {
        panic!("new")
    };
    handle.done().await.unwrap();
    assert!(matches!(
        agent
            .prompt_keyed_in(&context, "hello", options(), None)
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
    let changed = context
        .clone()
        .with_model_controls(controls(ReasoningEffort::Low));
    assert!(matches!(
        agent
            .prompt_keyed_in(&changed, "hello", options(), None)
            .await,
        Err(RuntimeError::Store(StoreError::AdmissionConflict))
    ));
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(provider.requests()[0].selection.model_id, "stored");
    assert_eq!(
        provider.requests()[0].reasoning_effort,
        Some(ReasoningEffort::Medium)
    );
}
