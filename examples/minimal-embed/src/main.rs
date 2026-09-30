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
    let (provider, model, protocol) = cli_options()?;
    let selection = Selection {
        provider_id: provider.clone(),
        model_id: model,
    };
    let resolver: Arc<dyn crabber::providers::Resolver> = if provider == "fake" {
        Arc::new(fake_provider())
    } else {
        let mut providers = crabber::providers::HttpResolver::from_env();
        if provider == "opencode-go" {
            providers =
                providers.with_adapter(crabber::providers::HttpAdapter::opencode_go(protocol));
        }
        Arc::new(providers)
    };

    // crabber:glue-start
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
        .provider(resolver)
        .config(AgentConfig::new(selection))
        .tool(Arc::new(tool))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()?;
    let mut run = agent.prompt(None, "Use the echo tool").await?;
    let mut events = run.events();
    loop {
        let Some(event) = events.recv().await? else {
            break;
        };
        match event.kind {
            EventKind::TextDelta => {
                print!("{}", event.payload["text"].as_str().unwrap_or_default());
                io::stdout().flush()?;
            }
            EventKind::ToolCallSettled => println!("\ntool call settled"),
            _ => {}
        }
    }
    println!();
    run.done().await?;
    // crabber:glue-end
    Ok(())
}

fn fake_provider() -> FakeProvider {
    let call_id = ToolCallId::new();
    FakeProvider::scripted(vec![
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
    ])
}

fn cli_options() -> Result<(String, String, crabber::providers::Protocol), Box<dyn Error>> {
    let mut provider = "fake".to_owned();
    let mut model = None;
    let mut protocol = crabber::providers::Protocol::Responses;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or("option needs a value")?;
        match flag.as_str() {
            "--provider" => provider = value,
            "--model" => model = Some(value),
            "--protocol" => {
                protocol = match value.as_str() {
                    "responses" => crabber::providers::Protocol::Responses,
                    "messages" => crabber::providers::Protocol::Messages,
                    "chat-completions" => crabber::providers::Protocol::ChatCompletions,
                    _ => return Err("invalid protocol".into()),
                }
            }
            _ => return Err(format!("unknown option: {flag}").into()),
        }
    }
    let model = model.unwrap_or_else(|| {
        if provider == "fake" {
            "scripted".into()
        } else {
            String::new()
        }
    });
    if model.is_empty() {
        return Err("--model is required for real providers".into());
    }
    Ok((provider, model, protocol))
}
