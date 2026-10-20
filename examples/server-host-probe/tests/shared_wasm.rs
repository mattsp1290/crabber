//! Public shared WASM registration executes the actual local component.
#![cfg(feature = "wasm")]
use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection, StaticPolicy, StreamDelta,
    core::{ContentBlock, RunStatus, ToolCallId, ToolOutput},
    session::{MemoryStore, Store},
    wasm::{InstanceMode, Limits, ModuleConfig},
};
use serde_json::json;
use server_host_probe::Error;
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};

fn component() -> Result<ModuleConfig, Error> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/wasm/target/wasm32-wasip2/release")
        .canonicalize()?;
    let path = root.join("echo_tool.wasm");
    Ok(ModuleConfig {
        name: "echo-tool".into(),
        expected_sha256: Sha256::digest(std::fs::read(&path)?).into(),
        path,
        allowed_root: root,
        config_json: "{}".into(),
        limits: Limits::default(),
        instance_mode: InstanceMode::PerCall,
    })
}

#[tokio::test]
async fn shared_wasm_tool_reaches_storage_and_next_model_request() -> Result<(), Error> {
    let id = ToolCallId::new();
    let arguments = json!({"message":"public component result"});
    let provider = FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: id.clone(),
                name: "echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: id.clone(),
                text: arguments.to_string(),
            },
            StreamDelta::ToolCallDone {
                call_id: id.clone(),
            },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ]);
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version("wasm-public-probe.v1")
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_wasm_extension(component()?)
        .build()?;
    let handle = agent.prompt(None, "execute the shared component").await?;
    let session = handle.session_id().clone();
    assert_eq!(handle.done().await?.status, RunStatus::Completed);
    let expected = ToolOutput::from_json(&arguments)?.to_content_blocks();
    let messages = store.list_all_messages(&session).await?;
    assert_eq!(messages.iter().flat_map(|message| &message.parts).filter(|part|
        matches!(&part.content, ContentBlock::ToolResult { call_id, content, is_error: false }
            if call_id == &id && content == &expected)).count(), 1);
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].tools.iter().any(|tool| tool.name == "echo"));
    assert_eq!(requests[1].messages.iter().flat_map(|message| &message.parts).filter(|part|
        matches!(&part.content, ContentBlock::ToolResult { call_id, content, is_error: false }
            if call_id == &id && content == &expected)).count(), 1);
    agent.close_extensions().await?;
    Ok(())
}
