use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, ToolDefinition, ToolExecutor,
    core::{ToolCallId, ToolInfo},
    extension::{
        Callback, EventPublished, Extension, GuardDecision, Point, PromptSection, Registrar, Scope,
        ToolGuard, ToolResultTransform,
    },
};
use serde_json::{Value, json};
use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct Shell;
#[async_trait]
impl ToolExecutor for Shell {
    async fn execute(&self, input: Value) -> Result<Value, ExtensionError> {
        println!("runtime tool executed: {}", input["command"]);
        Ok(json!({"output":"ok","secret":"private-value"}))
    }
}
struct DenyDelete;
impl ToolGuard for DenyDelete {
    fn id(&self) -> &'static str {
        "deny-recursive-delete"
    }
    fn check(&self, _name: &str, input: &Value) -> GuardDecision {
        if input["command"]
            .as_str()
            .is_some_and(|s| s.contains("rm -rf"))
        {
            println!("guard denied rm -rf");
            GuardDecision::Deny
        } else {
            GuardDecision::Abstain
        }
    }
}
struct NativeExtension {
    observed: Arc<AtomicUsize>,
}
#[async_trait]
impl Extension for NativeExtension {
    fn id(&self) -> &'static str {
        "example/native"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "demo".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        r.tool(Arc::new(ToolDefinition { info: ToolInfo { name:"shell".into(), description:"Runs a demo command".into(), parameters:json!({"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}), retry_safe:true, required_permissions:vec![] }, executor:Arc::new(Shell) }));
        r.prompt(Arc::new(PromptSection {
            name: "native-guidance".into(),
            order: 0,
            text: "Use the shell tool carefully.".into(),
        }));
        r.guard(Arc::new(DenyDelete));
        let redact: Callback = Arc::new(|mut value| {
            Box::pin(async move {
                if let Some(secret) = value
                    .get_mut("result")
                    .and_then(|result| result.get_mut("secret"))
                {
                    *secret = Value::String("[REDACTED]".into());
                }
                println!("redacted tool result");
                Ok(value)
            })
        });
        r.on_transform(ToolResultTransform::ID, 0, "redact", redact);
        let observed = Arc::clone(&self.observed);
        r.on_notify(
            EventPublished::ID,
            0,
            "observe",
            Arc::new(move |value| {
                let observed = Arc::clone(&observed);
                Box::pin(async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    println!("observed event: {}", value["kind"]);
                    Ok(Value::Null)
                })
            }),
        );
        Ok(())
    }
}
fn call(id: ToolCallId, command: &str) -> Vec<StreamDelta> {
    vec![
        StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: "shell".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: json!({"command":command}).to_string(),
        },
        StreamDelta::ToolCallDone { call_id: id },
    ]
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut script = call(ToolCallId::new(), "rm -rf /tmp/example");
    script.extend(call(ToolCallId::new(), "echo hello"));
    script.push(StreamDelta::Completed);
    let provider = FakeProvider::scripted(vec![
        script,
        vec![
            StreamDelta::TextDelta("Done".into()),
            StreamDelta::Completed,
        ],
    ]);
    let observed = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .extension(
            Arc::new(NativeExtension {
                observed: Arc::clone(&observed),
            }),
            Scope::Global,
        )
        .build()?;
    let run = agent.prompt(None, "Try both commands").await?;
    run.done().await?;
    let requests = provider.requests();
    let first = &requests[0];
    assert!(first.tools.iter().any(|tool| tool.name == "shell"));
    println!("runtime tool registered");
    assert!(
        first
            .system
            .as_deref()
            .unwrap_or_default()
            .contains("Use the shell tool carefully.")
    );
    println!("prompt section in request");
    let second = serde_json::to_string(&requests[1].messages)?;
    assert!(second.contains("[REDACTED]") && !second.contains("private-value"));
    println!("redacted result in request");
    assert!(observed.load(Ordering::SeqCst) > 0);
    println!("observed {} events", observed.load(Ordering::SeqCst));
    Ok(())
}
