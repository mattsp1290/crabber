//! Exercises the public API with serde_json/preserve_order unified by a consumer.
use crabber::{
    Admission, AdmissionKey, AdmissionOptions, Agent, AgentConfig, ExtensionError, FakeProvider,
    InputFingerprint, RuntimeError, Selection, SessionId, StreamDelta, ToolDefinition,
    ToolExecutor,
    core::{CoreError, ToolInfo},
    extension::{Extension, Registrar, Scope},
    session::MemoryStore,
};
use serde_json::Value;
use std::{error::Error, sync::Arc};

// Reorder both root and nested keys, including objects nested inside arrays.
const SCHEMA: &str = r#"{"type":"object","properties":{"mode":{"type":"string","enum":["a","b"]}},"allOf":[{"type":"object","minProperties":0}]}"#;
const REORDERED_SCHEMA: &str = r#"{"allOf":[{"minProperties":0,"type":"object"}],"properties":{"mode":{"enum":["a","b"],"type":"string"}},"type":"object"}"#;
const CONFIG: &str = r#"{"mode":"safe","nested":{"a":1,"b":2},"steps":[{"x":1,"y":2}]}"#;
const REORDERED_CONFIG: &str = r#"{"steps":[{"y":2,"x":1}],"nested":{"b":2,"a":1},"mode":"safe"}"#;

struct UnusedTool;
#[async_trait::async_trait]
impl ToolExecutor for UnusedTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("text-only receipt regression must not execute tools")
    }
}

struct SchemaExtension {
    tool: Arc<ToolDefinition>,
    config: String,
}
#[async_trait::async_trait]
impl Extension for SchemaExtension {
    fn id(&self) -> &str {
        "schema-extension"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn config_hash(&self) -> String {
        self.config.clone()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(self.tool.clone());
        Ok(())
    }
}

fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("external-receipt").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}

fn agent(
    store: Arc<MemoryStore>,
    provider: Arc<FakeProvider>,
    schema: &str,
    registry_config: Option<&str>,
) -> Result<Agent, Box<dyn Error>> {
    let tool = Arc::new(ToolDefinition {
        info: ToolInfo {
            name: "schema-tool".into(),
            description: "unused".into(),
            parameters: serde_json::from_str(schema)?,
            retry_safe: false,
            required_permissions: vec![],
        },
        executor: Arc::new(UnusedTool),
    });
    let mut builder = Agent::builder()
        .store(store)
        .provider(provider)
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }));
    builder = if let Some(config) = registry_config {
        builder.extension(
            Arc::new(SchemaExtension {
                tool,
                config: config.into(),
            }),
            Scope::Global,
        )
    } else {
        builder.tool(tool)
    };
    Ok(builder.build()?)
}

pub async fn run() -> Result<(), Box<dyn Error>> {
    // A feature-unification guard: this test must not accidentally run with sorted maps.
    let left: Value = serde_json::from_str(SCHEMA)?;
    let right: Value = serde_json::from_str(REORDERED_SCHEMA)?;
    assert_ne!(left.to_string(), right.to_string());
    for registry in [false, true] {
        let store = Arc::new(MemoryStore::new());
        let provider = Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ]]));
        let session = SessionId::new();
        let first = agent(
            store.clone(),
            provider.clone(),
            SCHEMA,
            registry.then_some(CONFIG),
        )?
        .prompt_keyed(session.clone(), "hello", options())
        .await?;
        let Admission::Started { receipt, handle } = first else {
            panic!("first admission must start")
        };
        handle.done().await?;
        let replay_agent = agent(
            store.clone(),
            provider.clone(),
            REORDERED_SCHEMA,
            registry.then_some(REORDERED_CONFIG),
        )?;
        let replay = replay_agent
            .prompt_keyed(session.clone(), "hello", options())
            .await?;
        assert!(matches!(replay, Admission::Replayed(_)));
        assert_eq!(replay.receipt(), &receipt);
        assert_eq!(
            replay_agent
                .lookup_admission(&session, &options().key)
                .await?,
            Some(receipt)
        );
        // Canonicalization must preserve changed scalar values and ordered arrays.
        for changed in [
            SCHEMA.replace("string", "number"),
            SCHEMA.replace(r#"["a","b"]"#, r#"["b","a"]"#),
        ] {
            let outcome = agent(
                store.clone(),
                provider.clone(),
                &changed,
                registry.then_some(CONFIG),
            )?
            .prompt_keyed(session.clone(), "hello", options())
            .await;
            assert!(matches!(
                outcome,
                Err(RuntimeError::Store(CoreError::AdmissionConflict))
            ));
        }
        if registry {
            let changed = CONFIG.replace("safe", "different");
            let outcome = agent(store, provider.clone(), SCHEMA, Some(&changed))?
                .prompt_keyed(session, "hello", options())
                .await;
            assert!(matches!(
                outcome,
                Err(RuntimeError::Store(CoreError::AdmissionConflict))
            ));
        }
        assert_eq!(provider.requests().len(), 1);
    }
    println!("Admission receipt canonicalization: static + registry preserve_order replay passed");
    Ok(())
}
