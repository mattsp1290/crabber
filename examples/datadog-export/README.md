# Datadog traces, metrics and logs

Run the complete fixture without credentials or an Agent:

```sh
env -u DD_API_KEY -u OTEL_EXPORTER_OTLP_ENDPOINT -u OTEL_EXPORTER_OTLP_TRACES_ENDPOINT cargo run -p datadog-export
```

The fixture migrates its own temporary SQLite database, retries one scripted
provider failure, executes a native `echo` tool and completes a text response.
It prints `run_id=… status=Completed`. No configured application database is used.
The provider's failure contains a secret sentinel that must never appear in
stderr or exported signals; offline loopback assertions live in
[operational-telemetry](../operational-telemetry/README.md).

To send APM spans, enable the Agent's HTTP OTLP receiver with
`DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT=0.0.0.0:4318`, then set
`OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318` in the application.
`DD_API_KEY` enables agentless LLM Observability, metrics and logs; `DD_SITE`
selects the account site. `DD_SERVICE`, `DD_ENV` and `DD_VERSION` identify the
service. Credential values stay in the environment.

The host installs `Telemetry::layer()` once in a global subscriber. Inside
`host.request`, it passes `Telemetry::current_trace_context()` to
`prompt_with_context` so APM, LLM Observability and logs share the host identity.
Optional `CRABBER_LOG_STDERR=1` adds JSON stderr formatting, filtered separately
with `RUST_LOG`; this filter never suppresses telemetry. Optional
`CRABBER_OBS_VERIFY_MARKER` adds the same verification tag to both intake and
OTLP resource attributes.

After completion the host calls `telemetry.flush()` and `telemetry.shutdown()`.
The telemetry bundle owns exporter shutdown; the example shares its observer
with the agent and does not shut that observer down independently.

`cargo xtask verify-datadog` runs this fixture and checks APM spans, LLM spans,
metrics and linked layer logs. The manual gate also needs `DD_APP_KEY` with the
appropriate read scopes. Run it from a clean committed checkout. Offline
fixtures prove the wire contract; live ingestion, linkage and percentiles remain
unverified until the corresponding live gate is recorded. See the
[host observability guide](../../docs/observability.md).
