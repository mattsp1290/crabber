//! Credential-free SQLite run; environment configuration enables exports.
mod fixture;
use crabber::{
    Agent, AgentConfig, PermissionDecision, Selection, StaticPolicy, ToolDefinition,
    core::ToolInfo,
    obs::{Telemetry, TelemetryConfig},
    session::SqliteStore,
};
use std::{error::Error, sync::Arc};
use tracing::Instrument as _;
use tracing_subscriber::{EnvFilter, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut config = TelemetryConfig::from_env()?;
    if let Ok(marker) = std::env::var("CRABBER_OBS_VERIFY_MARKER") {
        if let Some(datadog) = config.datadog.as_mut() {
            datadog.tags.push(format!("verify:{marker}"));
        }
        config.resource_tags.push(("verify".into(), marker));
    }
    let telemetry = Telemetry::new(config)?;
    let stderr = (std::env::var("CRABBER_LOG_STDERR").as_deref() == Ok("1")).then(|| {
        tracing_subscriber::fmt::layer()
            .json()
            .with_writer(std::io::stderr)
            .with_filter(EnvFilter::from_default_env())
    });
    tracing_subscriber::registry()
        .with(telemetry.layer())
        .with(stderr)
        .init();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("session.sqlite");
    SqliteStore::migrate(&path).await?;
    let mut builder = Agent::builder()
        .behavior_version("observability-example.v1")
        .sqlite(&path)
        .await?
        .provider(Arc::new(fixture::Provider::new()))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "datadog-demo".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "echo".into(),
                description: "Echo input".into(),
                parameters: serde_json::json!({"type":"object", "properties":{"text":{"type":"string"}}, "required":["text"]}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(fixture::Echo),
        }));
    if let Some(observer) = telemetry.exporter() {
        builder = builder.datadog_observer(observer);
    }
    let agent = builder.build()?;
    let result = async {
        agent
            .prompt_with_context(
                None,
                "demonstrate observability",
                Telemetry::current_trace_context(),
            )
            .await?
            .done()
            .await
    }
    .instrument(tracing::info_span!("host.request"))
    .await?;
    assert_eq!(result.status, crabber::core::RunStatus::Completed);
    println!("run_id={} status={:?}", result.run_id, result.status);
    telemetry.flush().await?;
    telemetry.shutdown().await?;
    Ok(())
}
