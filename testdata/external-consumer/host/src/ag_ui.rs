use std::{error::Error, sync::Arc};
use crabber::{Agent, AgentConfig, FakeProvider, Selection, StreamDelta};
use crabber_agui::{Completion, ProjectionConfig, Projector, encode_sse, ag_ui_core::event::Event};

pub async fn run() -> Result<(), Box<dyn Error>> {
    let agent = Agent::builder().memory().provider(Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("external host".into()), StreamDelta::Completed,
    ]])))
        .config(AgentConfig::new(Selection { provider_id:"fake".into(), model_id:"scripted".into() })).build()?;
    let mut handle = agent.prompt(None, "hello").await?;
    let config = ProjectionConfig::default();
    let mut projector = Projector::new(handle.session_id().clone(), handle.run_id().clone(), "external-thread".into(), "external-run".into(), config.clone())?;
    let mut receiver = handle.events();
    let mut text = String::new();
    while let Some(record) = receiver.recv().await? {
        for event in projector.push(&record)? {
            let frame = encode_sse(&event, config.max_event_bytes)?;
            assert!(frame.starts_with(b"data: "));
            if let Event::TextMessageContent(event) = event { text.push_str(&event.delta); }
        }
    }
    handle.done().await?;
    assert_eq!(text, "external host");
    assert!(matches!(projector.finish(Completion::Completed)?.as_slice(), [Event::RunFinished(_)]));
    println!("external AG-UI public API journey passed");
    Ok(())
}
