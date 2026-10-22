use async_trait::async_trait;
use crabber::JsonToolExecutor;
use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection, StaticPolicy, StreamDelta,
    core::{RunStatus, ToolCallId, ToolInfo},
    extension::{
        Extension, ExtensionError, ModelRequestError, Point, PromptSection, Registrar, Scope,
        ToolDefinition, TransformOutput,
    },
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use crabber_extensions::tool_result_redactor::{
    Limits as RedactorLimits, Options as RedactorOptions, ToolResultRedactor,
};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

const SECRET: &str = "ghp_0123456789abcdef";
fn redactor() -> Arc<ToolResultRedactor> {
    Arc::new(
        ToolResultRedactor::new(RedactorOptions {
            order: -100,
            excluded_tools: vec![],
            additional_patterns: vec![],
            limits: RedactorLimits {
                max_field_bytes: 4096,
                max_total_bytes: 16384,
                max_depth: 16,
                max_nodes: 100,
                max_matches_per_field: 16,
                max_patterns: 4,
                max_pattern_bytes: 256,
                max_in_flight: 4,
            },
        })
        .unwrap(),
    )
}
fn config(root: &str) -> AgentConfig {
    let mut c = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    c.workspace_id = "workspace".into();
    c.directory = root.into();
    c.system_prompt = Some("base".into());
    c
}
fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
struct SyntheticTool {
    rewrite: Option<PathBuf>,
    failed: bool,
}
#[async_trait]
impl JsonToolExecutor for SyntheticTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        if let Some(file) = &self.rewrite {
            std::fs::write(file, "second-payload").unwrap();
        }
        if self.failed {
            Err(ExtensionError::Tool(SECRET.into()))
        } else {
            Ok(json!({"output":SECRET,"ok":42}))
        }
    }
}
fn tool(failed: bool, rewrite: Option<PathBuf>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: "synthetic".into(),
            description: "Synthetic test".into(),
            parameters: json!({"type":"object"}),
            retry_safe: false,
            required_permissions: vec![],
        },
        executor: Arc::new(SyntheticTool { failed, rewrite }),
    })
}
fn call(name: &str) -> Vec<StreamDelta> {
    let id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: name.into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: "{}".into(),
        },
        StreamDelta::ToolCallDone { call_id: id },
        StreamDelta::Completed,
    ]
}
struct Reducer;
#[async_trait]
impl Extension for Reducer {
    fn id(&self) -> &str {
        "fixture/reducer"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixture".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        r.on_result_transform(
            1_000_000,
            "fixture/reducer",
            Arc::new(|_, _| {
                Box::pin(async {
                    Ok(TransformOutput::marked_error(
                        crabber::core::ToolOutput::from_json(&json!({"reduced":SECRET})).unwrap(),
                    ))
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test]
async fn final_redaction_protects_durable_results_events_and_next_provider_after_reducer() {
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![call("synthetic"), done()]));
    let agent = agent_support::builder()
        .store(store.clone())
        .provider(provider.clone())
        .config(config("/workspace"))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(tool(false, None))
        .shared_extension(redactor(), Scope::Global)
        .shared_extension(Arc::new(Reducer), Scope::Global)
        .build()
        .unwrap();
    let run = agent.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let next = format!("{:?}", requests[1]);
    assert!(!next.contains(SECRET) && next.contains("[REDACTED]"));
    let messages = store.list_all_messages(&session).await.unwrap();
    let events = store.list_events(&session, None, 100).await.unwrap();
    assert!(!format!("{messages:?}{events:?}").contains(SECRET));
    assert!(format!("{messages:?}{events:?}").contains("[REDACTED]"));
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session,
            limits: SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 10000,
                encoded_bytes: 100000,
            },
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("snapshot")
    };
    assert_eq!(page.tool_calls.len(), 1);
    let result = page.tool_calls[0].result.as_ref().unwrap();
    assert_eq!(result.status, crabber::core::ToolResultStatus::Failed);
    assert!(!format!("{result:?}").contains(SECRET));
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn error_and_unknown_tool_paths_remain_errors_after_redaction() {
    for (failed, name) in [(true, "synthetic"), (false, SECRET)] {
        let store = Arc::new(MemoryStore::new());
        let provider = Arc::new(FakeProvider::scripted(vec![call(name), done()]));
        let agent = Agent::builder()
            .behavior_version("crabber-extensions-tests.v1")
            .store(store.clone())
            .provider(provider.clone())
            .config(config("/workspace"))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .shared_tool(tool(failed, None))
            .shared_extension(redactor(), Scope::Global)
            .build()
            .unwrap();
        let run = agent.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let messages = store.list_all_messages(&session).await.unwrap();
        let results: Vec<_> = messages
            .iter()
            .flat_map(|m| &m.parts)
            .filter_map(|p| match &p.content {
                crabber::core::ContentBlock::ToolResult {
                    content, is_error, ..
                } => Some((content, is_error)),
                _ => None,
            })
            .collect();
        assert_eq!(results.len(), 1);
        assert!(*results[0].1);
        assert!(!format!("{:?}", results[0].0).contains(SECRET));
        // Unknown tool names also occur in arguments/call metadata; the redactor
        // protects result payloads, not that separate durable input identity.
        agent.close_extensions().await.unwrap();
    }
}
#[path = "runtime/instructions.rs"]
mod instructions;

#[path = "support/agent.rs"]
mod agent_support;
