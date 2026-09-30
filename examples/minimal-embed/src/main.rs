use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, EventKind, ExtensionError, FakeProvider, PermissionDecision, Selection,
    StaticPolicy, StreamDelta, ToolDefinition, ToolExecutor,
    core::{RunId, RunStatus, ToolCallId, ToolInfo},
    session::{MemoryStore, Store},
};
use serde_json::{Value, json};
use std::{
    error::Error,
    io::{self, Write},
    sync::Arc,
};

struct EchoTool {
    delay: bool,
}

#[async_trait]
impl ToolExecutor for EchoTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        if self.delay {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
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
    let CliOptions {
        provider,
        model,
        protocol,
        interrupt_after_first_delta,
        resume_id,
    } = cli_options()?;
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

    let store: Arc<dyn Store> = Arc::new(MemoryStore::new());

    // crabber:glue-start
    let tool = ToolDefinition {
        info: ToolInfo {
            name: "echo".into(),
            description: "Returns its arguments".into(),
            parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(EchoTool {
            delay: interrupt_after_first_delta,
        }),
    };
    let agent = Agent::builder()
        .store(Arc::clone(&store))
        .provider(resolver)
        .config(AgentConfig::new(selection))
        .tool(Arc::new(tool))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()?;
    if let Some(run_id) = resume_id {
        resume_run(&agent, &store, &run_id).await?;
        return Ok(());
    }
    let mut run = agent.prompt(None, "Use the echo tool").await?;
    let run_id = run.run_id().clone();
    let mut events = run.events();
    let mut interrupted = false;
    loop {
        let Some(event) = events.recv().await? else {
            break;
        };
        match event.kind {
            EventKind::TextDelta => {
                print!("{}", event.payload["text"].as_str().unwrap_or_default());
                io::stdout().flush()?;
                if interrupt_after_first_delta && !interrupted {
                    run.interrupt();
                    interrupted = true;
                }
            }
            EventKind::ToolCallSettled => println!("\ntool call settled"),
            _ => {}
        }
    }
    println!();
    let result = run.done().await?;
    if interrupted {
        println!("interrupted run {}: {:?}", run_id, result.status);
        resume_run(&agent, &store, &run_id).await?;
    }
    // crabber:glue-end
    Ok(())
}

async fn resume_run(
    agent: &Agent,
    store: &Arc<dyn Store>,
    run_id: &RunId,
) -> Result<(), Box<dyn Error>> {
    let original = store
        .get_run(run_id)
        .await?
        .ok_or("run not found in configured store")?;
    if original.status == RunStatus::Interrupted {
        let continuation = agent
            .prompt(Some(original.session_id), "Continue after interruption")
            .await?;
        let new_run_id = continuation.run_id().clone();
        let result = continuation.done().await?;
        println!(
            "resumed from {} as {}: {:?}",
            run_id, new_run_id, result.status
        );
    } else if original.status == RunStatus::Paused || original.status == RunStatus::Running {
        let result = agent.resume(run_id).await?;
        println!("resumed run {}: {:?}", run_id, result.status);
    } else {
        return Err(format!("run {} is already {:?}", run_id, original.status).into());
    }
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

struct CliOptions {
    provider: String,
    model: String,
    protocol: crabber::providers::Protocol,
    interrupt_after_first_delta: bool,
    resume_id: Option<RunId>,
}

fn cli_options() -> Result<CliOptions, Box<dyn Error>> {
    let mut provider = "fake".to_owned();
    let mut model = None;
    let mut protocol = crabber::providers::Protocol::Responses;
    let mut interrupt_after_first_delta = false;
    let mut resume_id = None;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--interrupt-after-first-delta" {
            interrupt_after_first_delta = true;
            continue;
        }
        let value = args.next().ok_or("option needs a value")?;
        match flag.as_str() {
            "--provider" => provider = value,
            "--model" => model = Some(value),
            "--resume" => resume_id = Some(RunId::from(value)),
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
    Ok(CliOptions {
        provider,
        model,
        protocol,
        interrupt_after_first_delta,
        resume_id,
    })
}
