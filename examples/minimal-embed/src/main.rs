use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, EventKind, ExtensionError, FakeProvider, PermissionDecision, Selection,
    StaticPolicy, StreamDelta, ToolDefinition, ToolExecutor,
    core::{ToolCallId, ToolInfo},
};
use serde_json::{Value, json};
use std::{
    error::Error,
    io::{self, Write},
    sync::Arc,
};

struct EchoTool;

#[async_trait]
impl ToolExecutor for EchoTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        Ok(arguments)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "crabber {} (git {})",
        env!("CARGO_PKG_VERSION"),
        env!("CRABBER_GIT_SHA")
    );

    // crabber:glue-start
    let call_id = ToolCallId::new();
    let provider = FakeProvider::scripted(vec![
        vec![
            StreamDelta::TextDelta("Checking tool...".into()),
            StreamDelta::ToolCallStart {
                call_id: call_id.clone(),
                name: "echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call_id.clone(),
                text: r#"{"text":"hello"}"#.into(),
            },
            StreamDelta::ToolCallDone { call_id },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("Done.".into()),
            StreamDelta::Completed,
        ],
    ]);
    let tool = ToolDefinition {
        info: ToolInfo {
            name: "echo".into(),
            description: "Returns its arguments".into(),
            parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(EchoTool),
    };
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(provider))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .tool(Arc::new(tool))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()?;
    let mut run = agent.prompt(None, "Use the echo tool").await?;
    let mut events = run.events();
    loop {
        let event = events.recv().await?;
        match event.kind {
            EventKind::TextDelta => {
                print!("{}", event.payload["text"].as_str().unwrap_or_default());
                io::stdout().flush()?;
            }
            EventKind::ToolCallSettled => println!("\ntool call settled"),
            EventKind::RunSettled => {
                println!();
                break;
            }
            _ => {}
        }
    }
    run.done().await?;
    // crabber:glue-end
    Ok(())
}
