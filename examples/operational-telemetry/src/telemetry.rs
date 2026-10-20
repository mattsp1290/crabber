//! Additional real-subscriber proof; existing exact-clock fixtures run first.
use super::{
    Agent, AgentConfig, Arc, Intake, PermissionDecision, Selection, StaticPolicy, ToolDefinition,
    ToolInfo, json,
};
#[path = "../../datadog-export/src/fixture.rs"]
mod fixture;
#[path = "otlp_receiver.rs"]
mod otlp_receiver;
use crabber::obs::{ALLOWED_ATTRIBUTES, OtlpConfig, OtlpProtocol, Telemetry, TelemetryConfig};
use opentelemetry_proto::tonic::{common::v1::any_value::Value, trace::v1::Span};
use std::time::Duration;
use tracing::Instrument as _;
use tracing_subscriber::prelude::*;
fn field<'a>(span: &'a Span, key: &str) -> Option<&'a str> {
    span.attributes
        .iter()
        .find(|attr| attr.key == key)
        .and_then(|attr| attr.value.as_ref())
        .and_then(|value| {
            if let Some(Value::StringValue(value)) = &value.value {
                Some(value.as_str())
            } else {
                None
            }
        })
}
const MODEL_CALLS: usize = 3; // Failed attempt, successful tool turn, final text turn.
const TOOL_CALLS: usize = 1;
const EXTENSION_MOUNTS: usize = 1;
const EXTENSION_CLOSES: usize = 1;
const EXTENSION_ID: &str = "telemetry/echo";

struct EchoExtension(Arc<ToolDefinition>);
#[async_trait::async_trait]
impl crabber::extension::Extension for EchoExtension {
    fn id(&self) -> &'static str {
        EXTENSION_ID
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        crabber::session::admission_config_hash(&json!(&self.0.info))
    }
    async fn install(
        &self,
        registrar: &mut crabber::extension::Registrar,
    ) -> Result<(), crabber::ExtensionError> {
        registrar.tool(self.0.clone());
        Ok(())
    }
}

pub async fn run() {
    let receiver = otlp_receiver::Receiver::new();
    let intake = Intake::new().await;
    let mut config =
        TelemetryConfig::from_lookup(&|_| Err(std::env::VarError::NotPresent)).unwrap();
    let mut datadog = intake.config();
    datadog.metric_dimensions.extensions = vec![EXTENSION_ID.into()];
    config.datadog = Some(datadog);
    config.otlp = Some(OtlpConfig {
        traces_url: format!("{}/v1/traces", receiver.endpoint),
        protocol: OtlpProtocol::HttpProtobuf,
        timeout: Duration::from_secs(2),
        headers: vec![],
    });
    let telemetry = Telemetry::new(config).unwrap();
    tracing_subscriber::registry()
        .with(telemetry.layer())
        .init();
    let agent = build_agent(&telemetry);
    let result = async {
        agent
            .prompt_with_context(None, "PROMPT_SECRET", Telemetry::current_trace_context())
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
    }
    .instrument(tracing::info_span!("host.request"))
    .await;
    assert_eq!(result.status, crabber::core::RunStatus::Completed);
    agent.close_extensions().await.unwrap();
    telemetry.flush().await.unwrap();
    telemetry.shutdown().await.unwrap();
    let spans = receiver.spans();
    let (count, bound) = verify_tree(&spans);
    let requests = intake.requests.lock().unwrap().clone();
    verify_payloads(&requests);
    println!(
        "apm=offline-proven spans={count} bound={bound} layer_metrics=verified layer_logs=verified"
    );
}

fn build_agent(telemetry: &Telemetry) -> Agent {
    Agent::builder()
        .behavior_version("telemetry-example.v1")
        .memory()
        .provider(Arc::new(fixture::Provider::new()))
        .datadog_observer(telemetry.exporter().unwrap())
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(
            Arc::new(EchoExtension(Arc::new(ToolDefinition {
                info: ToolInfo {
                    name: "echo".into(),
                    description: "telemetry fixture".into(),
                    parameters: json!({"type":"object"}),
                    retry_safe: true,
                    required_permissions: vec![],
                },
                executor: Arc::new(fixture::Echo),
            }))),
            crabber::Scope::Global,
        )
        .build()
        .unwrap()
}

fn verify_tree(spans: &[Span]) -> (usize, usize) {
    let host = spans
        .iter()
        .find(|span| span.name == "host.request")
        .unwrap();
    let run = spans
        .iter()
        .find(|span| span.name == "crabber.run")
        .unwrap();
    assert_eq!(run.parent_span_id, host.span_id);
    let admission = spans
        .iter()
        .find(|span| span.name == "crabber.admission")
        .unwrap();
    assert_eq!(admission.parent_span_id, host.span_id);
    assert_eq!(field(admission, "outcome"), Some("started"));
    let models: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "crabber.model_call")
        .collect();
    assert_eq!(models.len(), MODEL_CALLS);
    for model in &models {
        assert_eq!(model.parent_span_id, run.span_id);
    }
    let tools: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "crabber.tool_call")
        .collect();
    assert_eq!(tools.len(), TOOL_CALLS);
    assert_eq!(tools[0].parent_span_id, run.span_id);
    let mounts: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "crabber.extension.mount")
        .collect();
    assert_eq!(mounts.len(), EXTENSION_MOUNTS);
    assert!(
        mounts
            .iter()
            .any(|span| field(span, "extension.id") == Some(EXTENSION_ID))
    );
    assert!(
        spans
            .iter()
            .all(|span| span.name != "crabber.provider.request"
                && span.name != "crabber.store.transaction")
    );
    for span in spans
        .iter()
        .filter(|span| span.name.starts_with("crabber."))
    {
        for attr in &span.attributes {
            assert!(ALLOWED_ATTRIBUTES.contains(&attr.key.as_str()));
            let value = format!("{:?}", attr.value);
            for secret in [
                "SECRET response credential header",
                "PROMPT_SECRET",
                "SECRET argument",
                "TOOL_ARGUMENT_SECRET",
                "SECRET result",
            ] {
                assert!(!value.contains(secret));
            }
        }
    }
    // Catalog: admission + run + scripted models/tools + mount/close, margin 4.
    let bound = 1 + 1 + MODEL_CALLS + TOOL_CALLS + EXTENSION_MOUNTS + EXTENSION_CLOSES + 4;
    let count = spans
        .iter()
        .filter(|span| span.name.starts_with("crabber."))
        .count();
    assert!(
        count <= bound,
        "span count {count} exceeds catalog bound {bound}"
    );
    (count, bound)
}

fn verify_payloads(requests: &[super::Request]) {
    for request in requests.iter().filter(|request| request.accepted) {
        let body = request.body.to_string();
        for sentinel in [
            "SECRET response credential header",
            "PROMPT_SECRET",
            "TOOL_ARGUMENT_SECRET",
        ] {
            assert!(
                !body.contains(sentinel),
                "content leaked into {}",
                request.path
            );
        }
    }

    let distributions: Vec<_> = requests
        .iter()
        .filter(|request| request.accepted && request.path == "/api/v1/distribution_points")
        .flat_map(|request| request.body["series"].as_array().unwrap())
        .collect();
    for name in [
        "crabber.extension.mount_ms",
        "crabber.extension.close_ms",
        "crabber.admission.elapsed_ms",
    ] {
        let metrics: Vec<_> = distributions
            .iter()
            .filter(|series| series["metric"] == name)
            .collect();
        assert_ne!(
            metrics,
            [] as [&&serde_json::Value; 0],
            "missing distribution {name}"
        );
        for metric in metrics {
            let points = metric["points"].as_array().unwrap();
            assert_ne!(points.as_slice(), [] as [serde_json::Value; 0]);
            for point in points {
                let samples = point[1].as_array().unwrap();
                assert_ne!(samples.as_slice(), [] as [serde_json::Value; 0]);
                assert!(
                    samples
                        .iter()
                        .all(|value| value.as_f64().is_some_and(f64::is_finite))
                );
            }
        }
    }
    let logs: Vec<_> = requests
        .iter()
        .filter(|request| request.accepted && request.path == "/api/v2/logs")
        .flat_map(|request| request.body.as_array().unwrap())
        .filter(|log| log["crabber.source"] == "layer")
        .collect();
    assert_ne!(logs, [] as [&serde_json::Value; 0]);
    assert!(
        logs.iter()
            .any(|log| log["error.kind"] == "provider_server")
    );
    for log in logs {
        assert!(
            ![
                "run admitted",
                "run started",
                "run settled",
                "tool settled",
                "permission decided"
            ]
            .contains(&log["message"].as_str().unwrap())
        );
        assert!(
            !log.to_string()
                .contains("SECRET response credential header")
        );
    }
}
