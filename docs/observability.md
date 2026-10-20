# Observability

Crabber instruments execution with `tracing`. Hosts own subscriber and exporter
initialization: no library crate installs a global subscriber. The facade's
`otlp` Cargo feature includes `datadog` and enables the `obs::Telemetry` bundle.

| Signal | Observer alone (`datadog`) | With `Telemetry::layer()` |
| --- | --- | --- |
| LLM Observability | Agentless HTTP spans | Same shared observer |
| Existing operational metrics | Agentless HTTP series and distributions | Same measurements, no duplicate run/model/tool distributions |
| Lifecycle logs | Four agentless HTTP messages | Same four messages |
| APM traces | None | OTLP/HTTP to a local Datadog Agent or collector |
| I/O metrics and diagnostics | None | Span/event layers through agentless HTTP intake |

Without `DD_API_KEY` and OTLP endpoint configuration, the bundle exports
nothing. Instrumentation without a subscriber is inert. Tests and examples run
without credentials. `Telemetry::new` and `from_env` must run inside a Tokio
runtime; construction from synchronous `main` returns a finite runtime error.

## Agent and host setup

Enable the Agent's OTLP HTTP listener with
`DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT=0.0.0.0:4318`. Configure the
application with `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318`. This
release supports HTTP protobuf (default) and HTTP JSON. It rejects HTTPS
endpoints and gRPC. Keep the Agent listener on a trusted local network.

Construct the bundle inside Tokio, install `tracing_subscriber::registry()`
with `telemetry.layer()`, and pass `telemetry.exporter()` to
`AgentBuilder::datadog_observer` when present. The observer is shared between
the bundle and agent, so there is one bounded queue and worker.

Install the subscriber once globally for multithreaded Tokio programs.
Thread-local `set_default` is suitable for current-thread tests only. Attach
host formatting filters with `fmt_layer.with_filter(...)`. A global filter
layer can starve telemetry; use per-layer filtering. The trace layer exports
host spans of any target so request parentage survives.

A request follows this pattern after the agent is built with the shared observer:

```rust,ignore
use tracing::Instrument;
use tracing_subscriber::prelude::*;
let telemetry = crabber::obs::Telemetry::from_env()?;
tracing_subscriber::registry().with(telemetry.layer()).init();
// Pass telemetry.exporter(), when present, to AgentBuilder::datadog_observer.
let result = async {
    let context = crabber::obs::Telemetry::current_trace_context();
    agent.prompt_with_context(None, "example input", context).await?.done().await
}.instrument(tracing::info_span!("host.request")).await?;
telemetry.flush().await?;
telemetry.shutdown().await?;
```

The complete runnable recipe is
[`datadog-export`](../examples/datadog-export/README.md).

## Configuration

| Environment variable | Meaning |
| --- | --- |
| `DD_API_KEY` | Enables existing agentless LLM/metrics/logs export when nonempty |
| `DD_SITE` | Datadog site; default `datadoghq.com` |
| `DD_SERVICE` | Service name; takes precedence over `OTEL_SERVICE_NAME` |
| `OTEL_SERVICE_NAME` | APM service fallback; final default `crabber` |
| `DD_ENV`, `DD_VERSION` | Optional environment/version resource attributes and intake tags |
| `DD_LLMOBS_ML_APP` | LLM Observability application identity |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Base HTTP endpoint; normalized `/v1/traces` suffix |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Complete traces URL, used verbatim; overrides base endpoint |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` (default) or `http/json` |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | Export timeout in milliseconds; default 10000 |
| `OTEL_EXPORTER_OTLP_HEADERS` | Comma-separated `key=value` pairs; never printed by configuration Debug |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | Standard SDK sampler configuration; default parent-based always-on |

`TelemetryConfig.signals` enables traces, metrics and logs independently through
programmatic booleans. There is no runtime kill-switch environment variable.
`LogsPolicy` selects the minimum level (INFO by default); guest text is off by
default and must be explicitly enabled. `resource_tags` adds sanitized
host-supplied resource attributes. Do not put credentials or content in tags
or host-defined identities.

Resources carry `service.name`, `service.version`, and
`deployment.environment.name`. The deprecated `deployment.environment` is
also emitted for older Agents. The optional OTLP dependency uses reqwest 0.13
alongside the existing 0.12 HTTP clients. The HTTP-only OTLP feature adds no
TLS provider; existing clients continue using ring.

## Span catalog

All spans have explicit field lists. Finite `outcome` and `error.kind` fields
classify results without exporting error messages. Failed outcomes set OTel
error status. No span is created per stream delta or heartbeat tick.

| Span | Boundary and fields |
| --- | --- |
| `crabber.run`, `crabber.resume` | Execution attempt; session/run/attempt IDs, outcome/reason, optional host trace/span/predecessor IDs |
| `crabber.admission` | Admission transaction; session ID, keyed, outcome |
| `crabber.model_call` | Each turn/compaction retry; provider/model IDs, purpose, retry attempt, token counts, outcome/reason |
| `crabber.tool_call` | Tool execution; tool name/call ID, permission, outcome |
| `crabber.approval.suspend` | Durable approval suspension; calls, outcome |
| `crabber.approval.continue` | Decision application on a later attempt; outcome |
| `crabber.store.transaction` | Transaction lifetime; database, finite scope, outcome; client kind |
| `crabber.store.connect`, `crabber.store.migrate` | Store initialization; database, schema version on migration; client kind |
| `crabber.provider.request` | HTTP send until response headers; provider, protocol, HTTP status, refresh retry; client kind |
| `crabber.provider.models` | Model discovery; provider, outcome; client kind |
| `crabber.extension.mount`, `crabber.extension.close` | Install and bounded close; extension ID, scope, outcome, leases/pending tasks on timeout |

Scoped store transactions are INFO; unscoped ones are DEBUG. `MemoryStore`
performs no I/O and emits no store spans. Instrumented scopes are `session`,
`run`, `lease`, `admission`, `events`, `snapshot`, `approval`,
`tool_continuation`, `queue`, `verify`, and `unscoped`. The transaction outcome
is written once, so rollback after a failed commit retains `commit_failed`.
Statement SQL, DSNs and file paths are never attributes.

Admission and the attempt are siblings beneath the caller's request span.
The caller span is captured before spawning the admitted attempt; model,
tool, provider and scoped transaction spans inherit the attempt. The registry
keeps the host parent alive until children close. Its duration ends at its
last exit even though export waits for child closure. Approval waits cross
attempts/processes and have no duration span.

## Metrics

Existing observer metrics remain unchanged:
`crabber.run.count`, `crabber.run.duration_ms`, `crabber.model.calls`,
`crabber.model.latency_ms`, `crabber.model.tokens.input`,
`crabber.model.tokens.output`, `crabber.tool.calls`,
`crabber.permission.decisions`, `crabber.wasm.calls`,
`crabber.run.elapsed_ms`, `crabber.model.elapsed_ms`,
`crabber.tool.elapsed_ms`, `crabber.model.first_token_ms`, and exporter
health `crabber.export.batches`, `crabber.export.dropped`.

| Distribution (milliseconds) | Counter | Tags |
| --- | --- | --- |
| `crabber.store.transaction_ms` | `crabber.store.transactions` | db, scope, outcome, error_kind on begin/commit failure |
| `crabber.store.connect_ms`, `crabber.store.migrate_ms` | — | db, outcome |
| `crabber.provider.response_start_ms` | `crabber.provider.requests` | provider, protocol, status_class, retried, error_kind |
| `crabber.extension.mount_ms` | — | extension, scope, outcome |
| `crabber.extension.close_ms` | `crabber.extension.close_timeouts` on timeout | extension, outcome |
| `crabber.admission.elapsed_ms` | `crabber.admission.outcomes` | outcome, keyed |
| — | `crabber.approval.suspensions`, `crabber.approval.continuations` | outcome |
| — | `crabber.lease.lost` | none; counted at heartbeat only |
| — | `crabber.lease.renew_failures` | error_kind |
| — | `crabber.events.lagged` | none; counter value is skipped event count |
| — | `crabber.model.retries` | error_kind, compaction_requested |

All metrics also carry configured service/env/version and sanitized host tags.
Session/run/attempt/call/trace/span IDs never become metric dimensions.
Provider/model/tool/extension names use explicit allowlists capped at 32
entries, with unknown values mapped to `overflow`. Unknown finite values map
to `other`. Database is `sqlite`/`postgresql`; status class is
`2xx`/`4xx`/`5xx`/`transport`/`other`; retried/keyed/compaction_requested are
booleans. Store outcomes are `begin_failed`, `committed`, `commit_failed`,
`rolled_back`, `abandoned`; SQLx error classes are `database`, `io`,
`pool_timeout`, `pool_closed`, `protocol`, `other`. Admission outcomes are
`started`, `replayed`, `conflict`, `rejected`, `unknown`. Extension scopes are
`global`, `workspace`, `session`; mount outcomes are `mounted`,
`install_failed`, `install_panicked`, `registry_closed`; close outcomes are
`closed`, `timed_out`, `self_close`.

The complete layer vocabulary is closed at compile time. Every value outside
these sets becomes `other`:

| Tag | Values |
| --- | --- |
| `outcome` | `begin_failed`, `committed`, `commit_failed`, `rolled_back`, `abandoned`, `completed`, `failed`, `connected`, `migrated`, `mounted`, `closed`, `succeeded`, `timed_out`, `install_failed`, `install_panicked`, `registry_closed`, `self_close`, `started`, `replayed`, `conflict`, `rejected`, `unknown`, `suspended`, `approved`, `denied`, `cancelled`, `expired`, `transport_error`, `ok`, `success` |
| `protocol` | `openai`, `anthropic`, `openai_compatible`, `openai_responses`, `chat_completions`, `responses`, `messages`, `sse`, `http` |
| `error_kind` | `auth`, `context_overflow`, `invalid`, `canceled`, `provider_auth`, `provider_context_overflow`, `provider_invalid`, `provider_canceled`, `limit`, `admission_conflict`, `admission_revision_conflict`, `admission_unsupported`, `admission_recovery_required`, `snapshot_unsupported`, `host_unsupported`, `session_identity_mismatch`, `session_deleted`, `busy`, `pending_input`, `store_limit`, `store_admission_conflict`, `store_admission_revision_conflict`, `store_admission_unsupported`, `store_admission_recovery_required`, `store_snapshot_unsupported`, `store_host_unsupported`, `store_session_identity_mismatch`, `store_session_deleted`, `store_busy`, `store_pending_input`, `database`, `io`, `pool_timeout`, `pool_closed`, `protocol`, `other`, `lease_lost`, `transport`, `rate_limited`, `unauthorized`, `authentication`, `authorization`, `invalid_request`, `server`, `timeout`, `cancelled`, `context_length`, `unknown`, `validation`, `conflict`, `not_found`, `unavailable`, `provider_server`, `provider_rate_limited`, `provider_transport`, `provider_timeout`, `provider_authentication`, `provider_invalid_request`, `provider_context_length`, `provider_other`, `store_conflict`, `store_not_found`, `store_validation`, `extension_plan`, `install_failed`, `install_panicked` |
| `purpose` (observer) | `turn`, `compaction` |
| `reason` (observer) | `success`, `provider_error`, `tool_error`, `cancelled`, `lease_lost`, `paused`, `runtime_error` |

Span-derived elapsed uses real `Instant`, independently of the runtime's
injectable measurement clock. It never duplicates observer run/model/tool
distributions. Provider response-start latency measures headers, not body
consumption. See [operational measurements](operational-observation.md).

## Logs and redaction

The observer exports four lifecycle messages: `run admitted`, `run settled`,
`tool settled`, `permission decided`. Runtime lifecycle events remain visible
to host formatting under `crabber::runtime::lifecycle`, but the logs layer
excludes them and drops duplicate lifecycle messages.

The logs layer exports static allowlisted diagnostics for retries, lease
renewal/loss, event lag, approval suspension/recovery, provider rejection and
credential refresh, migrations, extension install failure and close timeout.
Logs carry `crabber.source:layer`, `ddsource:crabber`, finite `error.kind`,
and bounded allowlisted fields. Unknown messages become `unlisted message`;
unknown fields are dropped. At most 32 fields and 256 bytes per string are
retained. The exporter target `crabber_obs` is excluded to prevent recursion.

Prompt text, reasoning, arguments/results, headers, URLs, DSNs, paths and
error Display strings never enter library telemetry. An export-time attribute
allowlist scrubs unexpected attributes on `crabber.*` spans. Host spans pass
through unchanged; hosts own their content policy. An xtask AST check enforces
static message literals in the instrumented library crates. Guest logs use
`crabber::wasm::guest` and the static message `guest log`; `guest.text` is
exported only with explicit opt-in and remains bounded to 256 bytes.

## Correlation

Open a host `host.request` span and instrument the request future. Inside it,
call `Telemetry::current_trace_context()` and pass that context to
`prompt_with_context` or `prompt_keyed_in`. This links LLM Observability and
layer logs to the APM request trace. Without the recipe, APM spans still have
parentage but LLM/log export has only session/run identity.

For incoming W3C headers, extract an OTel context in the host and apply
`OpenTelemetrySpanExt::set_parent` to the host request span before entering
it. Crabber does not adopt remote parents from its host context attributes.

Layer logs carry both OpenTelemetry `trace_id` (32 lowercase hex digits) /
`span_id` (16 lowercase hex digits), and the existing `dd.trace_id` /
`dd.span_id` convention. These identify the supplied host request span,
not the innermost library span. Configure a Trace ID remapper on `trace_id`
if required by the account; live verification reports which convention links.

## Lifecycle and health

Call `telemetry.flush().await` and `telemetry.shutdown().await` before the
Tokio runtime exits. Failed OTLP batches are discarded by the SDK; the bundle retains a finite failure flag so flush/shutdown cannot acknowledge delivery after such loss. Shutdown is idempotent and tolerates an agent observer
that was already stopped. With a shared observer, prefer only bundle shutdown:
`Agent::shutdown` retains its original raw worker-stopped behavior.

Drop aborts the agentless worker, accounts outstanding records as dropped,
and requests bounded provider cleanup on a detached OS thread. It does not
block the dropping task. Explicit shutdown is required for delivery.

`Telemetry::health()` exposes spans_seen, events_seen, metrics_enqueued,
metrics_dropped, logs_enqueued, logs_dropped, messages_unlisted,
attributes_scrubbed. `spans_seen` counts span callbacks in the metrics and logs layers; a span
observed by both contributes twice. It remains zero for OTLP-only configuration,
when both agentless layers are disabled, or when no agentless exporter exists.
With those layers configured, start with `spans_seen == 0` when diagnosing a
missing subscriber/layer. The existing `ExportHealth` remains the worker/queue health
source. OTLP batch drops are currently exposed only by SDK internal logs at
shutdown, outside the Crabber log-export target filter.

## Verification and alternatives

`cargo xtask check` runs credential-free loopback HTTP intake and OTLP
protobuf fixtures, isolated feature builds, static diagnostics validation,
and the external consumer. `cargo run -p operational-telemetry -- --check`
checks signal relationships and a catalog-derived span bound.

These checks establish **offline-proven** behavior. They do not establish
Datadog visibility. With a running OTLP-enabled Agent and `DD_SITE`,
`DD_API_KEY`, `DD_APP_KEY`, and `OTEL_EXPORTER_OTLP_ENDPOINT`, run
`cargo xtask verify-datadog`. The application key requires read access for
LLM Observability, metrics, logs and APM (`apm_read`). The gate must find the
marker-scoped APM tree (including SQLite transactions), seven metric signals,
a layer log and a matching trace identity before reporting `visibility=passed`.
Only then record a live verification row with date, commit, site and linkage
convention in the [exporter README](../crates/crabber-obs/README.md).

OTLP metrics/logs are outside this bundle; hosts can install
`opentelemetry-appender-tracing` for an alternative logs pipeline.
`datadog-opentelemetry` is another host integration option. This implementation
keeps the existing agentless transports and adds OTLP only for APM traces.
Dashboards, monitors, percentile tuning and pool gauges remain follow-up work.
