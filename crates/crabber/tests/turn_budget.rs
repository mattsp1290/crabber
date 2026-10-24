use async_trait::async_trait;
use crabber::{
    Agent, AgentBuilder, AgentConfig, BuildError, ExtensionError, FakeProvider, JsonToolExecutor,
    RuntimeError, Selection, StreamDelta, ToolDefinition,
    core::{RunStatus, ToolCallId, ToolInfo},
};
use serde_json::{Value, json};
use std::sync::Arc;

struct Echo;

#[async_trait]
impl JsonToolExecutor for Echo {
    async fn execute(&self, input: Value) -> Result<Value, ExtensionError> {
        Ok(input)
    }
}

fn builder(provider: Arc<FakeProvider>) -> AgentBuilder {
    Agent::builder()
        .behavior_version("turn-budget-test")
        .provider(provider)
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "echo".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(Echo),
        }))
}

fn provider() -> Arc<FakeProvider> {
    let call_id = ToolCallId::new();
    Arc::new(FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call_id.clone(),
                name: "echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call_id.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone { call_id },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ]))
}

#[tokio::test]
async fn public_turn_budget_bounds_model_tool_loop() {
    for max_turns in [None, Some(1), Some(2)] {
        let provider = provider();
        let mut builder = builder(provider.clone());
        if let Some(max_turns) = max_turns {
            builder = builder.max_turns(max_turns);
        }
        let agent = builder.build().unwrap();
        let result = agent
            .prompt(None, "echo then finish")
            .await
            .unwrap()
            .done()
            .await;
        if max_turns == Some(1) {
            assert!(matches!(result, Err(RuntimeError::TurnLimit)));
            assert_eq!(provider.requests().len(), 1);
        } else {
            assert_eq!(result.unwrap().status, RunStatus::Completed);
            assert_eq!(provider.requests().len(), 2);
        }
    }
}

#[test]
fn zero_turn_budget_is_rejected_at_build() {
    assert!(matches!(
        builder(provider()).max_turns(0).build(),
        Err(BuildError::Runtime(RuntimeError::InvalidConfiguration(_)))
    ));
}
