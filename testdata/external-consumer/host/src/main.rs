#[path = "../../../../examples/operational-telemetry/src/journey.rs"]
mod operational;
mod admission_receipts;
mod bounded_snapshots;
mod custom_provider;
mod trace_context;
mod ag_ui;
#[path = "../../../../examples/host-trace/src/export.rs"]
mod export;
#[path = "../../../../examples/host-trace/src/journey.rs"]
mod journey;

use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection, StaticPolicy, StreamDelta,
    core::ToolCallId,
    wasm::{InstanceMode, Limits, ModuleConfig},
};
use sha2::{Digest, Sha256};
use std::{error::Error, path::PathBuf, sync::Arc};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    custom_provider::run()?;
    ag_ui::run().await?;
    operational::run().await;
    admission_receipts::run().await?;
    bounded_snapshots::run().await?;
    trace_context::run().await?;
    journey::journey().await;
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target"));
    let root = target.join("wasm32-wasip2/release").canonicalize()?;
    let module = |name: &str, binary: &str| -> Result<ModuleConfig, Box<dyn Error>> {
        let path = root.join(binary);
        let hash = Sha256::digest(std::fs::read(&path)?).into();
        Ok(ModuleConfig {
            name: name.into(), path, allowed_root: root.clone(), expected_sha256: hash,
            config_json: "{}".into(), limits: Limits::default(), instance_mode: InstanceMode::PerCall,
        })
    };
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
        .wasm_extension(module("echo-tool", "external_guest.wasm")?)
        .wasm_extension(module("external-controls", "external_controls.wasm")?)
        .build()?;
    let result = agent.prompt(None, "Echo a message").await?.done().await?;
    println!("WASM extension run: {:?}", result.status);
    Ok(())
}
