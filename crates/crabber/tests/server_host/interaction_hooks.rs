//! Ordinary native tools cannot park a worker on the removed live human hooks.
use super::*;
use crabber::core::{AuthorityRequirement, InteractionDescription, InteractionKind};
use tokio::time::{Duration, timeout};

struct OrdinaryQuestion;
#[async_trait]
impl JsonToolExecutor for OrdinaryQuestion {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::MissingCapability("tool context"))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let question = InteractionDescription::new(
            "choice",
            InteractionKind::UserInput,
            "PRIVATE_QUESTION",
            json!({"type":"string"}),
            AuthorityRequirement::new("host", "v1").unwrap(),
            None,
        )
        .unwrap();
        match context.request_interaction(&question) {
            Err(error) => Err(error.into()),
            Ok(impossible) => match impossible {},
        }
    }
}

#[tokio::test]
async fn unsupported_mid_call_question_returns_finite_error_without_waiting() {
    let store = Arc::new(MemoryStore::new());
    let provider = FakeProvider::scripted(vec![call("ordinary-question"), done()]);
    let agent = Agent::builder()
        .behavior_version("ordinary-question.v1")
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection("scripted")))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "identity".into(),
                description: "ordinary question".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(OrdinaryQuestion),
        }))
        .build()
        .unwrap();
    let handle = agent.prompt(None, "work").await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(
        timeout(Duration::from_secs(3), handle.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
    let history = store.list_all_messages(&session).await.unwrap();
    let encoded = serde_json::to_string(&history).unwrap();
    assert!(!encoded.contains("PRIVATE_QUESTION"));
    assert!(encoded.contains("durable interaction requires a resumable tool phase"));
    assert_eq!(provider.requests().len(), 2);
    let calls = &history
        .iter()
        .find(|message| message.role == crabber::core::Role::Tool)
        .unwrap()
        .parts;
    assert!(matches!(
        &calls[0].content,
        crabber::core::ContentBlock::ToolResult { is_error: true, .. }
    ));
}
