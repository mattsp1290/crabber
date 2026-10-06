#![cfg(feature = "opencode-go")]
use crabber::{
    Agent, AgentConfig, EventKind, EventRecord, Observer, OperationKind, OperationalObservation,
    Selection, TerminalReason,
    core::RunStatus,
    providers::{HttpAdapter, HttpResolver, Protocol},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct ModelOperations(Mutex<Vec<TerminalReason>>);
impl Observer for ModelOperations {
    fn emit(&self, _: &EventRecord) {}
    fn operational_completed(&self, observation: &OperationalObservation) {
        if matches!(observation.kind, OperationKind::Model { .. }) {
            self.0.lock().unwrap().push(observation.reason);
        }
    }
}
#[tokio::test]
#[ignore = "requires an authorized OPENCODE_GO_API_KEY and live provider access"]
async fn live_deepseek_v4_flash_accepts_tool_free_chat_completions_through_facade() {
    assert!(
        std::env::var("OPENCODE_GO_API_KEY").is_ok(),
        "set OPENCODE_GO_API_KEY before running the live acceptance test"
    );
    let observer = Arc::new(ModelOperations::default());
    let adapter = HttpAdapter::opencode_go(Protocol::ChatCompletions)
        .try_with_user_agent_product("crabber-live-check/0.1")
        .unwrap();
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(HttpResolver::new().with_adapter(adapter)))
        .observer(observer.clone())
        .config(
            AgentConfig::new(Selection {
                provider_id: "opencode-go".into(),
                model_id: "deepseek-v4-flash".into(),
            })
            .max_output_tokens(4096),
        )
        .build()
        .unwrap();
    let (saw_text, result) = tokio::time::timeout(Duration::from_secs(120), async {
        let mut handle = agent
            .prompt(None, "Reply with the single word OK.")
            .await
            .unwrap();
        let mut events = handle.events();
        let mut saw_text = false;
        while let Some(event) = events.recv().await.unwrap() {
            saw_text |= matches!(event.kind, EventKind::TextDelta);
        }
        (saw_text, handle.done().await.unwrap())
    })
    .await
    .expect("live acceptance timed out");
    assert!(saw_text, "provider returned no text delta");
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(*observer.0.lock().unwrap(), [TerminalReason::Success]);
}
