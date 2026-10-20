#![cfg(all(feature = "otlp", feature = "sqlite"))]
//! Public host recipe exports real scoped SQLite work in the attempt's tree.
#[path = "../../../examples/operational-telemetry/src/otlp_receiver.rs"]
mod receiver;
use crabber::{
    Agent, AgentConfig, FakeProvider, Selection, StreamDelta,
    obs::{ALLOWED_ATTRIBUTES, OtlpConfig, OtlpProtocol, Telemetry, TelemetryConfig},
    session::SqliteStore,
};
use std::{fmt::Write as _, sync::Arc, time::Duration};
use tracing::Instrument as _;
use tracing_subscriber::prelude::*;
#[tokio::test]
async fn sqlite_run_exports_scoped_store_child_under_host_request() {
    let receiver = receiver::Receiver::new();
    let mut config =
        TelemetryConfig::from_lookup(&|_| Err(std::env::VarError::NotPresent)).unwrap();
    config.otlp = Some(OtlpConfig {
        traces_url: format!("{}/v1/traces", receiver.endpoint),
        protocol: OtlpProtocol::HttpProtobuf,
        timeout: Duration::from_secs(2),
        headers: vec![],
    });
    let telemetry = Telemetry::new(config).unwrap();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(telemetry.layer()));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("trace.sqlite");
    SqliteStore::migrate(&path).await.unwrap();
    let agent = Agent::builder()
        .behavior_version("public-telemetry-fixture.v1")
        .sqlite(&path)
        .await
        .unwrap()
        .provider(Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("PROMPT_SECRET".into()),
            StreamDelta::Completed,
        ]])))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .build()
        .unwrap();
    let (result, context) = async {
        let context = Telemetry::current_trace_context().expect("host has a real OTel context");
        let result = agent
            .prompt_with_context(None, "PROMPT_SECRET", Some(context.clone()))
            .await
            .unwrap()
            .done()
            .await
            .unwrap();
        (result, context)
    }
    .instrument(tracing::info_span!("host.request"))
    .await;
    assert_eq!(result.status, crabber::core::RunStatus::Completed);
    telemetry.flush().await.unwrap();
    telemetry.shutdown().await.unwrap();
    let spans = receiver.spans();
    let host = spans
        .iter()
        .find(|span| span.name == "host.request")
        .unwrap();
    let run = spans
        .iter()
        .find(|span| span.name == "crabber.run")
        .unwrap();
    assert_eq!(run.parent_span_id, host.span_id);
    assert_eq!(run.trace_id, host.trace_id);
    let trace_hex = host
        .trace_id
        .iter()
        .fold(String::new(), |mut output, byte| {
            write!(&mut output, "{byte:02x}").unwrap();
            output
        });
    assert_eq!(trace_hex, context.trace_id());
    assert!(
        spans
            .iter()
            .any(|span| span.name == "crabber.store.transaction"
                && span.parent_span_id == run.span_id
                && span.trace_id == run.trace_id)
    );
    for span in spans
        .iter()
        .filter(|span| span.name.starts_with("crabber."))
    {
        for attr in &span.attributes {
            assert!(
                ALLOWED_ATTRIBUTES.contains(&attr.key.as_str()),
                "unexpected field {}",
                attr.key
            );
            assert!(!format!("{:?}", attr.value).contains("PROMPT_SECRET"));
        }
    }
}
