use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection, StaticPolicy, StreamDelta,
    core::ToolCallId,
    wasm::{InstanceMode, Limits, ModuleConfig},
};
use sha2::{Digest, Sha256};
use std::{error::Error, path::PathBuf, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../target/wasm32-wasip2/release")
        .canonicalize()?;
    let path = root.join("external_guest.wasm");
    let hash = Sha256::digest(std::fs::read(&path)?).into();
    let call_id = ToolCallId::new();
    let provider = FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call_id.clone(),
                name: "echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call_id.clone(),
                text: r#"{"message":"hello"}"#.into(),
            },
            StreamDelta::ToolCallDone { call_id },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("Done".into()),
            StreamDelta::Completed,
        ],
    ]);
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(provider))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .wasm_extension(ModuleConfig {
            name: "echo-tool".into(),
            path,
            allowed_root: root,
            expected_sha256: hash,
            config_json: "{}".into(),
            limits: Limits::default(),
            instance_mode: InstanceMode::PerCall,
        })
        .build()?;
    let result = agent.prompt(None, "Echo a message").await?.done().await?;
    println!("WASM extension run: {:?}", result.status);
    Ok(())
}
