use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, ToolDefinition, ToolExecutor,
    core::{
        ContentBlock, MediaKind, MediaSource, MessageContent, ToolCallId, ToolInfo, ToolOutput,
        ToolResultStatus,
    },
    extension::{
        Extension, Point, Registrar, Scope, ToolExecute, TransformOutput,
        parse_tool_output_envelope, tool_output_envelope,
    },
    session::{MemoryStore, Store},
};
use serde_json::{Value, json};
use std::sync::Arc;

fn content(caption: &str) -> Vec<MessageContent> {
    vec![
        MessageContent::Text {
            text: caption.into(),
        },
        MessageContent::Media {
            kind: MediaKind::Image,
            mime_type: "image/png".into(),
            source: MediaSource::File {
                file_id: "opaque-image".into(),
            },
        },
        MessageContent::Text {
            text: "trailing text".into(),
        },
    ]
}
struct MixedTool(bool);
#[async_trait]
impl ToolExecutor for MixedTool {
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        Ok(ToolOutput::new(content("PRIVATE_CAPTION"), self.0).unwrap())
    }
}
struct Transforms(bool);
#[async_trait]
impl Extension for Transforms {
    fn id(&self) -> &'static str {
        "typed-output-transform"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        self.0.to_string()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.on_around(
            ToolExecute::ID,
            0,
            "around",
            Arc::new(|arguments, next| {
                Box::pin(async move {
                    let output = parse_tool_output_envelope(next.call(arguments).await?)?;
                    assert_eq!(output.content(), content("PRIVATE_CAPTION"));
                    // A wrapper cannot clear the terminal's declared error bit.
                    Ok(tool_output_envelope(
                        ToolOutput::new(output.content().to_vec(), false).unwrap(),
                    ))
                })
            }),
        );
        let mark_error = self.0;
        registrar.on_result_transform(
            0,
            "ordinary",
            Arc::new(move |context, output| {
                Box::pin(async move {
                    assert_eq!(output.content(), content("PRIVATE_CAPTION"));
                    assert_eq!(context.is_error(), output.is_error());
                    Ok(TransformOutput {
                        result: output,
                        mark_error,
                    })
                })
            }),
        );
        registrar.on_final_redaction(
            0,
            "redact",
            Arc::new(|_, output| {
                Box::pin(async move {
                    assert_eq!(output.content(), content("PRIVATE_CAPTION"));
                    // Clear the local bit deliberately: the driver must retain earlier errors.
                    Ok(TransformOutput::new(
                        ToolOutput::new(content("public caption"), false).unwrap(),
                    ))
                })
            }),
        );
        Ok(())
    }
}
fn provider(call: &ToolCallId) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call.clone(),
                name: "mixed".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone {
                call_id: call.clone(),
            },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ]))
}
#[tokio::test]
async fn public_mixed_output_remains_typed_and_error_escalation_is_monotonic() {
    for (declared_error, transform_error) in [(false, false), (true, false), (false, true)] {
        run_mixed_output(
            Arc::new(MemoryStore::new()),
            declared_error,
            transform_error,
        )
        .await;
    }
}
async fn run_mixed_output(store: Arc<dyn Store>, declared_error: bool, transform_error: bool) {
    let call = ToolCallId::new();
    let provider = provider(&call);
    let agent = Agent::builder()
        .behavior_version("typed-output-loop.v1")
        .store(store.clone())
        .provider(provider.clone())
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "mixed".into(),
                description: "mixed result".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(MixedTool(declared_error)),
        }))
        .shared_extension(Arc::new(Transforms(transform_error)), Scope::Global)
        .build()
        .unwrap();
    let handle = agent.prompt(None, "show image").await.unwrap();
    let session = handle.session_id().clone();
    handle.done().await.unwrap();
    assert_output(
        &*store,
        &session,
        &call,
        &provider,
        declared_error || transform_error,
    )
    .await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_persists_typed_output_through_the_public_loop() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("typed-output.sqlite");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    let store = Arc::new(crabber::session::SqliteStore::connect(&path).await.unwrap());
    run_mixed_output(store, true, false).await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_persists_typed_output_through_the_public_loop() {
    let url = match std::env::var("CRABBER_TEST_POSTGRES_URL") {
        Ok(url) => url,
        Err(std::env::VarError::NotPresent)
            if std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref() == Ok("1") =>
        {
            panic!("CRABBER_TEST_POSTGRES_URL is required")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("CRABBER_TEST_POSTGRES_URL must be valid Unicode")
        }
    };
    crabber::session::PostgresStore::migrate(&url)
        .await
        .unwrap();
    let store = Arc::new(
        crabber::session::PostgresStore::connect(&url)
            .await
            .unwrap(),
    );
    run_mixed_output(store, true, false).await;
}
async fn assert_output(
    store: &dyn Store,
    session: &crabber::core::SessionId,
    call: &ToolCallId,
    provider: &FakeProvider,
    is_error: bool,
) {
    let messages = store.list_all_messages(session).await.unwrap();
    let record = messages
        .iter()
        .flat_map(|message| &message.parts)
        .find_map(|part| match &part.content {
            ContentBlock::ToolResult {
                call_id,
                content,
                is_error,
            } if call_id == call => Some((content.clone(), *is_error)),
            _ => None,
        })
        .unwrap();
    let expected = ToolOutput::new(content("public caption"), is_error)
        .unwrap()
        .to_content_blocks();
    assert_eq!(record, (expected.clone(), is_error));
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages.iter().flat_map(|message| &message.parts).any(|part| matches!(&part.content,
        ContentBlock::ToolResult { call_id, content, is_error: actual } if call_id == call && content == &expected && *actual == is_error)));
    let crabber::session::SnapshotOutcome::Page(page) = store
        .snapshot(crabber::session::SnapshotRequest {
            session_id: session.clone(),
            limits: crabber::session::SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 1000,
                text_bytes: 1 << 20,
                encoded_bytes: 1 << 22,
            },
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("fixture must fit a single snapshot")
    };
    let calls = page.tool_calls;
    let result = calls
        .iter()
        .find(|record| &record.id == call)
        .unwrap()
        .result
        .as_ref()
        .unwrap();
    assert_eq!(result.content, expected);
    assert_eq!(
        result.status,
        if is_error {
            ToolResultStatus::Failed
        } else {
            ToolResultStatus::Completed
        }
    );
    let events = store.list_events(session, None, 1000).await.unwrap();
    let settled = events
        .iter()
        .find(|event| event.kind == crabber::core::EventKind::ToolCallSettled)
        .unwrap();
    let source: crabber::core::event_payload::CallSettled<'_> =
        serde_json::from_value(settled.payload.clone()).unwrap();
    assert_eq!(source.call_id, *call);
    assert_eq!(*source.content, expected);
    assert_eq!(source.is_error, is_error);
    let projected = crabber_agui::public_tool_content(&result.content, is_error, false);
    assert_eq!(
        projected["content"][1],
        serde_json::to_value(&expected[1]).unwrap()
    );
    assert_eq!(projected["is_error"], is_error);
    for artifact in [
        serde_json::to_string(&messages).unwrap(),
        serde_json::to_string(&requests[1].messages).unwrap(),
        projected.to_string(),
    ] {
        assert!(!artifact.contains("PRIVATE_CAPTION"));
    }
}

#[cfg(feature = "wasm")]
#[tokio::test]
async fn public_wasm_mixed_completion_uses_the_same_typed_path() {
    use sha2::{Digest, Sha256};
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/wasm/target/wasm32-wasip2/release")
        .canonicalize()
        .expect("cargo xtask build-fixtures");
    let path = root.join("typed_output_tool.wasm");
    let config = crabber::wasm::ModuleConfig {
        name: "typed-output-tool".into(),
        expected_sha256: Sha256::digest(std::fs::read(&path).unwrap()).into(),
        path,
        allowed_root: root,
        config_json: "{}".into(),
        limits: crabber::wasm::Limits::default(),
        instance_mode: crabber::wasm::InstanceMode::PerCall,
    };
    let call = ToolCallId::new();
    let provider = provider(&call);
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version("typed-wasm-output-loop.v1")
        .store(store.clone())
        .provider(provider.clone())
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_wasm_extension(config)
        .shared_extension(Arc::new(Transforms(false)), Scope::Global)
        .build()
        .unwrap();
    let handle = agent.prompt(None, "show image").await.unwrap();
    let session = handle.session_id().clone();
    handle.done().await.unwrap();
    assert_output(&*store, &session, &call, &provider, false).await;
}
