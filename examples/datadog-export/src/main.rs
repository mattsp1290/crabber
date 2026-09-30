//! A credential-free fake-provider run; set `DD_API_KEY` to export.
use crabber::{Agent, AgentConfig, FakeProvider, Selection, StreamDelta};
use std::{error::Error, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let provider = FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("hello".into()),
        StreamDelta::Completed,
    ]]);
    let mut builder = Agent::builder()
        .memory()
        .provider(Arc::new(provider))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "datadog-demo".into(),
        }));
    if let Some(mut config) = crabber_obs::DatadogConfig::from_env() {
        if let Ok(marker) = std::env::var("CRABBER_OBS_VERIFY_MARKER") {
            config.tags.push(format!("verify:{marker}"));
        }
        builder = builder.datadog(config);
    }
    let agent = builder.build()?;
    let run = agent.prompt(None, "demonstrate observability").await?;
    let result = run.done().await?;
    agent.flush().await?;
    println!("run_id={} status={:?}", result.run_id, result.status);
    agent.shutdown().await?;
    Ok(())
}
