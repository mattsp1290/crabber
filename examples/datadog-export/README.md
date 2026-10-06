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

For searchable visibility, use `cargo xtask verify-datadog` with environment-only
`DD_SITE`, `DD_API_KEY` and `DD_APP_KEY` in the same account. The application key
needs LLM span, metric and log read permissions. The gate generates a unique
`verify` marker and requires linked agent/workflow/LLM spans, finite marker-filtered
run/model elapsed and model first-token distributions, run count and safe logs.
This fake fixture emits no tool timing sample. Intake acceptance alone does not
pass. Fixed padded UTC bounds and one 120-second polling deadline apply; absent
signals or read-access failures exit nonzero. Evidence prints only safe status,
counts and identities. Run against a clean committed checkout so the source SHA
identifies the tested code. Local tests cannot satisfy the live acceptance gate.
