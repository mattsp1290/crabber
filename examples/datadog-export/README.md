# Datadog LLM export

For the credential-free path, remove inherited opt-in configuration:

```sh
env -u DD_API_KEY cargo run -p datadog-export
```

A FakeProvider emits `hello`. The example waits for completion, calls
`agent.flush()`, prints the run ID/status and calls `agent.shutdown()`.
Its Datadog feature exports only when `DD_API_KEY` is nonempty; `DD_SITE` selects
the site and defaults to `datadoghq.com`. Optional `CRABBER_OBS_VERIFY_MARKER`
adds a verification tag. The exporter does not read `DD_APP_KEY`.

Providing an API key enables external writes and is a manual live-service step.
Do not inherit it into CI or an offline walkthrough. No live ingestion or
percentile verification is claimed here; that remains `crabber-jeyl`.

This example demonstrates LLM spans/metrics/logs. For run/model/tool/first-token
operational distributions, bounded safe queues and offline payload assertions,
use [operational-telemetry](../operational-telemetry/README.md). See
[exporter adoption](../../crates/crabber-obs/README.md) and [source](src/main.rs).
