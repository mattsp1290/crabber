use crabber::{Agent, AgentConfig, FakeProvider, Selection, StreamDelta, TraceContext, obs::{DatadogConfig, DatadogObserver}};
use std::{error::Error, sync::Arc};
pub async fn run() -> Result<(),Box<dyn Error>> {
    // Compile public exporter/context APIs outside the workspace, without credentials.
    let _config = DatadogConfig::from_lookup(&|_| Err(std::env::VarError::NotPresent));
    let _constructor: fn(&DatadogConfig) -> DatadogObserver = DatadogObserver::new;
    let agent = Agent::builder().memory().provider(Arc::new(FakeProvider::scripted(vec![vec![StreamDelta::TextDelta("safe".into()),StreamDelta::Completed]]))).config(AgentConfig::new(Selection {provider_id:"fake".into(),model_id:"scripted".into()})).build()?;
    let context = TraceContext::new("1234567890abcdef1234567890abcdef","1234567890abcdef")?;
    agent.prompt_with_context(None,"safe",Some(context)).await?.done().await?;
    Ok(())
}
