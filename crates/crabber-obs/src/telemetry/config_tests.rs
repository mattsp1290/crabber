use super::*;
use std::env::VarError;
fn config(vars: &[(&str, &str)]) -> Result<TelemetryConfig, TelemetryError> {
    TelemetryConfig::from_lookup(&|name| {
        vars.iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).to_owned())
            .ok_or(VarError::NotPresent)
    })
}
#[test]
fn telemetry_config_environment_defaults_and_endpoints() {
    let empty = config(&[]).unwrap();
    assert!(empty.datadog.is_none() && empty.otlp.is_none());
    let base = config(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://h:4318/")]).unwrap();
    assert_eq!(base.otlp.unwrap().traces_url, "http://h:4318/v1/traces");
    let explicit = config(&[
        ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://base"),
        ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://trace/custom"),
    ])
    .unwrap();
    assert_eq!(explicit.otlp.unwrap().traces_url, "http://trace/custom");
    for value in [
        "https://h",
        "ftp://h",
        "not an endpoint",
        "http://user:PRIVATE@h",
    ] {
        assert!(matches!(
            config(&[("OTEL_EXPORTER_OTLP_ENDPOINT", value)]),
            Err(TelemetryError::InvalidEndpoint)
        ));
    }
    assert!(matches!(
        config(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://h"),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
        ]),
        Err(TelemetryError::UnsupportedProtocol)
    ));
}
#[test]
fn telemetry_config_secrets_are_redacted() {
    let dd = config(&[("DD_API_KEY", "PRIVATE_KEY")]).unwrap();
    assert!(dd.datadog.is_some() && dd.otlp.is_none());
    let full = config(&[
        ("DD_API_KEY", "PRIVATE_KEY"),
        (
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "http://h/ENDPOINT_SECRET?token=QUERY_SECRET",
        ),
        ("OTEL_EXPORTER_OTLP_HEADERS", "authorization=PRIVATE_HEADER"),
    ])
    .unwrap();
    let debug = format!("{full:?}");
    assert!(!debug.contains("PRIVATE_KEY") && !debug.contains("PRIVATE_HEADER"));
    assert!(!debug.contains("ENDPOINT_SECRET") && !debug.contains("QUERY_SECRET"));
}
#[tokio::test]
async fn telemetry_config_empty_telemetry_is_inert() {
    use tracing_subscriber::prelude::*;
    let mut config = config(&[]).unwrap();
    config.signals.metrics = false;
    let telemetry = Telemetry::new(config).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.layer());
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!("host event");
    });
    assert!(telemetry.exporter().is_none());
    assert_eq!(telemetry.health(), TelemetryHealth::default());
    telemetry.flush().await.unwrap();
    telemetry.shutdown().await.unwrap();
    telemetry.shutdown().await.unwrap();
}
#[test]
fn telemetry_config_construction_requires_runtime() {
    assert!(matches!(
        Telemetry::new(config(&[]).unwrap()),
        Err(TelemetryError::Runtime)
    ));
}
