use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, EventKind, ExtensionError, FakeProvider, PermissionDecision, Selection,
    StaticPolicy, StreamDelta, ToolDefinition, ToolExecutor,
    core::{EventRecord, RunId, RunStatus, SessionId, ToolCallId, ToolInfo},
    extension::{Extension, GuardDecision, Registrar, Scope, ToolGuard},
    session::{MemoryStore, PostgresStore, Store},
    wasm::{InstanceMode, Limits, ModuleConfig},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

struct EchoTool {
    delay: bool,
}

struct NativeGuard {
    name: &'static str,
}
impl ToolGuard for NativeGuard {
    fn id(&self) -> &'static str {
        "minimal-native-guard"
    }
    fn check(&self, name: &str, arguments: &Value) -> GuardDecision {
        if name == self.name && arguments["text"] == "forbidden" {
            println!("native guard denied {name}");
            GuardDecision::Deny
        } else {
            GuardDecision::Abstain
        }
    }
}

struct NativeExtension {
    name: &'static str,
    delay: bool,
}
#[async_trait]
impl Extension for NativeExtension {
    fn id(&self) -> &'static str {
        "minimal-native"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        format!("{}:{}", self.name, self.delay)
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: self.name.into(), description: "Echoes arguments; the native guard denies text forbidden".into(),
                parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}),
                retry_safe: true, required_permissions: vec![],
            },
            executor: Arc::new(EchoTool { delay: self.delay }),
        }));
        registrar.guard(Arc::new(NativeGuard { name: self.name }));
        Ok(())
    }
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
        store_kind,
        extension,
        wasm,
    } = cli_options()?;
    let selection = Selection {
        provider_id: provider.clone(),
        model_id: model,
    };
    let wasm_enabled = wasm.is_some();
    let resolver: Arc<dyn crabber::providers::Resolver> = if provider == "fake" {
        Arc::new(fake_provider(wasm_enabled))
    } else {
        let mut providers = crabber::providers::HttpResolver::from_env();
        if provider == "opencode-go" {
            providers =
                providers.with_adapter(crabber::providers::HttpAdapter::opencode_go(protocol));
        }
        Arc::new(providers)
    };

    let store = create_store(&store_kind).await?;
    println!("store={store_kind} provider={provider}");
    let prompt_text = if wasm_enabled {
        "Use both tools in order. First call native-echo with {\"text\":\"forbidden\"}; its guard denial is expected. Then call echo with {\"text\":\"hello\"} even though the first call was denied. After both tool results, reply Done."
    } else {
        "Use the echo tool"
    };
    // crabber:glue-start
    let mut builder = Agent::builder()
        .store(Arc::clone(&store))
        .provider(resolver)
        .config(AgentConfig::new(selection))
        .extension(
            Arc::new(NativeExtension {
                name: if wasm_enabled { "native-echo" } else { "echo" },
                delay: interrupt_after_first_delta,
            }),
            Scope::Global,
        )
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)));
    if extension != "native" {
        return Err("--extension must be native".into());
    }
    if let Some(path) = wasm {
        builder = builder.wasm_extension(wasm_module(&path)?);
    }
    let agent = builder.build()?;
    if let Some(run_id) = resume_id {
        resume_run(&agent, &store, &run_id).await?;
        return Ok(());
    }
    let mut run = agent.prompt(None, prompt_text).await?;
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
            EventKind::ToolCallSettled => show_tool_settlement(&event, wasm_enabled),
            _ => {}
        }
    }
    println!();
    let result = run.done().await?;
    println!(
        "run={} session={} status={:?}",
        result.run_id, result.session_id, result.status
    );
    show_stored_session(&store, &result.session_id).await?;
    if interrupted {
        println!("interrupted run {}: {:?}", run_id, result.status);
        if store_kind == "memory" {
            resume_run(&agent, &store, &run_id).await?;
        }
    }
    // crabber:glue-end
    Ok(())
}

async fn create_store(store_kind: &str) -> Result<Arc<dyn Store>, Box<dyn Error>> {
    match store_kind {
        "memory" => Ok(Arc::new(MemoryStore::new())),
        "postgres" => {
            let url = std::env::var("CRABBER_POSTGRES_URL")?;
            PostgresStore::migrate(&url).await?;
            Ok(Arc::new(PostgresStore::connect(&url).await?))
        }
        _ => Err("--store must be memory or postgres".into()),
    }
}

fn show_tool_settlement(event: &EventRecord, wasm_enabled: bool) {
    let name = event.payload["name"].as_str().unwrap_or_default();
    let status = event.payload["status"].as_str().unwrap_or_default();
    if wasm_enabled && name == "echo" {
        println!("\nWASM tool {name} settled: {status}");
    } else {
        println!("\nnative tool {name} settled: {status}");
    }
    println!("tool call settled");
}

async fn show_stored_session(store: &Arc<dyn Store>, id: &SessionId) -> Result<(), Box<dyn Error>> {
    let stored = store
        .get_session(id)
        .await?
        .ok_or("session missing from store")?;
    let messages = store.list_messages(&stored.id, None).await?;
    println!("listed session={} messages={}", stored.id, messages.len());
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

fn fake_provider(wasm_enabled: bool) -> FakeProvider {
    let mut first = vec![StreamDelta::TextDelta("Checking tool...".into())];
    append_call(
        &mut first,
        if wasm_enabled { "native-echo" } else { "echo" },
        "forbidden",
    );
    append_call(&mut first, "echo", "hello");
    first.push(StreamDelta::Completed);
    FakeProvider::scripted(vec![
        first,
        vec![
            StreamDelta::TextDelta("Done.".into()),
            StreamDelta::Completed,
        ],
    ])
}

fn append_call(script: &mut Vec<StreamDelta>, name: &str, text: &str) {
    let call_id = ToolCallId::new();
    script.push(StreamDelta::ToolCallStart {
        call_id: call_id.clone(),
        name: name.into(),
    });
    script.push(StreamDelta::ToolCallArgsDelta {
        call_id: call_id.clone(),
        text: json!({"text":text}).to_string(),
    });
    script.push(StreamDelta::ToolCallDone { call_id });
}

struct CliOptions {
    provider: String,
    model: String,
    protocol: crabber::providers::Protocol,
    interrupt_after_first_delta: bool,
    resume_id: Option<RunId>,
    store_kind: String,
    extension: String,
    wasm: Option<PathBuf>,
}

fn wasm_module(path: &Path) -> Result<ModuleConfig, Box<dyn Error>> {
    let path = path.canonicalize()?;
    let root = path
        .parent()
        .ok_or("WASM path needs a parent directory")?
        .to_path_buf();
    let name = path
        .file_stem()
        .ok_or("WASM path needs a file name")?
        .to_string_lossy()
        .replace('_', "-");
    Ok(ModuleConfig {
        name,
        expected_sha256: Sha256::digest(std::fs::read(&path)?).into(),
        path,
        allowed_root: root,
        config_json: "{}".into(),
        limits: Limits::default(),
        instance_mode: InstanceMode::PerCall,
    })
}

fn cli_options() -> Result<CliOptions, Box<dyn Error>> {
    let mut provider = "fake".to_owned();
    let mut model = None;
    let mut protocol = crabber::providers::Protocol::Responses;
    let mut interrupt_after_first_delta = false;
    let mut resume_id = None;
    let mut store_kind = "memory".to_owned();
    let mut extension = "native".to_owned();
    let mut wasm = None;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--interrupt-after-first-delta" || flag == "--interrupt" {
            interrupt_after_first_delta = true;
            continue;
        }
        let value = args.next().ok_or("option needs a value")?;
        match flag.as_str() {
            "--provider" => provider = value,
            "--store" => store_kind = value,
            "--extension" => extension = value,
            "--wasm" => wasm = Some(PathBuf::from(value)),
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
        store_kind,
        extension,
        wasm,
    })
}
