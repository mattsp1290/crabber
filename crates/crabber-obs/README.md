# Crabber Datadog export

Enable the `crabber/datadog` feature and call `AgentBuilder::datadog_from_env()`. Set `DD_API_KEY` and optionally `DD_SITE`, `DD_SERVICE`, `DD_ENV`, `DD_VERSION`, and `DD_LLMOBS_ML_APP`. `DD_SITE` defaults to `datadoghq.com`. Exports use the LLM Observability span intake with plain JSON, and the metrics v2 and logs v2 intakes with gzip JSON. `DD_APP_KEY` is only read by `cargo xtask verify-datadog` for ingestion queries.

The original intake acceptance expected gzip for all three signals. In the US3 live gate, LLM Obs rejected the gzip request with HTTP 400 and accepted plain JSON with HTTP 202. The current [HTTP API reference](https://docs.datadoghq.com/llm_observability/instrument/api/) documents JSON for span intake. Metrics and logs continue to use gzip.

The observer allowlists lifecycle fields before queuing. Prompt and completion text, tool arguments and results, reasoning, and headers are never exported. The bounded queue drops observations when full and reports `crabber.export.dropped`. Call `agent.flush().await` before process exit, or `agent.shutdown().await` to flush and stop the worker.

Run the credential-free example with `cargo run -p datadog-export`. The live gate is `DD_SITE=... DD_API_KEY=... DD_APP_KEY=... cargo xtask verify-datadog`; it submits a marker-tagged fake run, waits for successful flush/shutdown, then requires searchable linked LLM spans, operational distributions, run count and logs within a single two-minute deadline. See the searchable visibility gate below.

## Live verification

| Date | Candidate | Site | Intake | Metrics | Logs | Span visibility |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-09-30 02:15 UTC | `42fbb96` | `us3.datadoghq.com` | HTTP 202 | Found (query HTTP 200, 1 series) | Found | Later confirmed by direct query |
| 2026-09-30 03:09 UTC | `653571c` | `us3.datadoghq.com` | HTTP 202 | Found (query HTTP 200, status ok, 1 series) | Found | Agent, workflow, and LLM spans found by direct query |
| 2026-09-30 03:18 UTC | `ceb0924` | `us3.datadoghq.com` | HTTP 202 | Found (query HTTP 200, status ok, 1 series) | Found | Agent, workflow, and LLM spans found by direct query |

The `ceb0924` live gate exited 0 with marker `crabber-1790738310-13253`. The user authorized direct verification with the authenticated `pup` CLI. `pup llm-obs spans search --query 'verify:crabber-1790738310-13253' --from 1h --summary` found linked agent, workflow, and LLM spans on trace `9245911478003124504`; the agent and workflow each covered 1.691 ms. This verifies span visibility through Datadog's read API; a browser UI click was not performed. Earlier gates on `653571c` and `42fbb96` also exited 0 before the final chunk-retry and failed-model observation fixes.

References: [LLM Observability HTTP API](https://docs.datadoghq.com/llm_observability/instrument/api/), [Submit metrics](https://docs.datadoghq.com/api/latest/metrics/submit-metrics/), [Send logs](https://docs.datadoghq.com/api/latest/logs/send-logs/), [Search logs](https://docs.datadoghq.com/api/latest/logs/search-logs-post/).

## Context and native recovery-link transport (2026-10-01)

The exporter now sends plain JSON raw envelopes to
`https://llmobs-intake.{DD_SITE}/api/v2/llmobs`, the native transport used by the
[official Datadog Python SDK at pinned revision 57aff59616e141dbf16cf92ac868a78b37ef7e1a](https://github.com/DataDog/dd-trace-py/blob/57aff59616e141dbf16cf92ac868a78b37ef7e1a/ddtrace/llmobs/_writer.py).
Each batch is `[{"_dd.stage":"raw","_dd.tracer_version":"crabber-<version>",
"event_type":"span","spans":[event]}, ...]`; it uses Crabber's own version identity.
Metrics/logs keep their existing gzip v2 origins. Local `api_origin` overrides
both span and metric origins. Allow-list/firewall users must adopt the new span
origin/path; mock intakes must flatten each envelope’s singleton `spans` array. Batches split
the outer raw-envelope array, exactly matching the first-party writer’s shape.
Accepted chunks are not replayed after later chunk failure. Native v2 follows
the official SDK success rule (any 2xx); successful submission alone is never
linked-product evidence.

The [current HTTP API reference](https://docs.datadoghq.com/llm_observability/instrument/api/)
documents top-level `apm_trace_id` on the public v1 API, but does not document
native `span_links` there. The exporter therefore uses the supported SDK v2
transport rather than assuming v1 accepts native links. The
[official SDK event mapping](https://github.com/DataDog/dd-trace-py/blob/57aff59616e141dbf16cf92ac868a78b37ef7e1a/ddtrace/llmobs/_llmobs.py)
emits `_dd.apm_trace_id`, `_dd.trace_id` and `_dd.span_id` for host APM association,
separately from internal LLM `trace_id`, `span_id`, and `parent_id`. The
[official OTel mapping](https://docs.datadoghq.com/llm_observability/instrument/otel_instrumentation/)
also distinguishes host correlation from LLM parentage and maps native links.
Both documentation pages were fetched successfully on 2026-10-01; v2 native
transport is supported by the pinned first-party implementation, not described
as the public v1 contract.

Validated host trace IDs are converted numerically: at most u64 becomes decimal;
larger values use all 32 lowercase hexadecimal digits. Host span IDs become
unsigned decimal. Logs use `dd.trace_id`/`dd.span_id` with the same conversions.
Internal LLM trace IDs use 32 hexadecimal digits, span/parent IDs use unsigned
64-bit decimal, and root parent is `undefined`. Internal attempt identity always includes the runtime observation-attempt UUID,
even when a host reuses its context. `linked_to_attempt` supplies the specific
prior observation UUID for native lineage. Older `linked_to` host-only metadata
remains readable, but does not claim a native LLM predecessor. Host APM
span IDs are association only. Context-free exports omit host correlation.
The closed admission anchors and loss/recovery delivery limits are described in
[the assembled example](../../examples/host-trace/README.md); they are causal
parents, not elapsed run-duration measurements.

Queued records own validated context and runtime attempt identity independently
of raw event payload. Tool spans use real runtime `call_id`/`name` fields. Status
is restricted to `ok`/`error`; paused admission remains `ok`. Rejection diagnostics
contain a finite signal/status/category and never response-body text. Transport
errors do not expose endpoint URLs through Display or Debug. Live results listed
above are historical v1 runs, not verification of this revision's context/link
transport. Current full correlation is **UNVERIFIED** until the assembled live
procedure returns actual linked LLM/APM/log evidence.

Operational timing observations use `POST /api/v1/distribution_points`, with
`{"series":[{"metric":"crabber.run.elapsed_ms","points":[[unix_seconds,[sample]]],"tags":[]}]}`
and supported `Content-Encoding: deflate`. Each eligible run/model/tool observation
produces one `crabber.run.elapsed_ms`, `crabber.model.elapsed_ms`, or
`crabber.tool.elapsed_ms` sample. A model observation additionally produces one
`crabber.model.first_token_ms` sample only when its typed first-token value is
present, including failed or cancelled calls that produced text. All values are
milliseconds. Repeated calls remain individual samples; gauges such as
`crabber.run.duration_ms` and `crabber.model.latency_ms` remain separate v2 series.
Model purpose is `turn` or `compaction`; reason uses the runtime's finite taxonomy.
Enable percentile aggregations for these distribution names in Datadog to query
percentiles. Local mock acceptance does not establish live ingestion or percentile
visibility; those remain unverified.

The distribution endpoint/body and deflate encoding follow the
[Datadog distribution API](https://docs.datadoghq.com/api/latest/metrics/submit-distribution-points/).
`max_payload_bytes` is our conservative uncompressed JSON bound, not a documented
distribution intake limit. HTTP 413 splits batches into smaller chunks; a single
oversized sample returns a safe error. Native LLM spans remain uncompressed and
logs/v2 metrics remain gzip encoded.

`DatadogConfig::metric_dimensions` supplies provider/model/tool allowlists. Each
list honors its first 32 names. Sanitization inspects only the first 100
input characters, then retains allowed ASCII characters; invalid prefixes never
cause an unbounded scan. Unknown or empty names
map to `overflow`; this applies to typed distributions and existing series.
Truncation alone is not the cardinality policy. Configure known identities such
as `fake`, `demo`, and `echo` for fixtures. Metric tags never include runtime
session/run/attempt/call/trace/span IDs. Host-controlled static tags remain the
host's responsibility. Native span identities/context and safe logs are preserved.
Adding this config field is a source API change: update explicit struct literals
with `metric_dimensions: MetricDimensions::default()` or host allowlists.
`ExportError::MetricIntakeErrors(count)` becomes `IntakeErrors { signal, count }`
to distinguish distribution and v2 metrics acknowledgements; `IntakeResponse`
reports invalid or oversized acknowledgements without their contents.

Read `DatadogObserver::health()` or, with the facade `datadog` feature,
`Agent::export_health()` without network activity. The facade returns `None` when
export is not configured (including `datadog_from_env` without `DD_API_KEY`). The
method is unavailable when the feature is disabled. `accepted` counts observations
reserved and submitted to the channel, not samples or deliveries; one model observation can yield
two samples. `dropped` counts enqueue rejections,
observations shed while a failed batch is retained, and outstanding observations
at worker termination. Accepted and dropped can therefore overlap. Both counters
are cumulative and never reset; `crabber.export.dropped` is now a cumulative gauge.
`retries` counts actual additional HTTP attempts, including the first request
when resuming a failed batch; failures before HTTP do not increment it;
`failures` counts failed HTTP submissions (including 413 and intake error arrays).
Queue/pending depths are current observation counts, bounded by channel capacity
(clamped to 1..4096) and batch size (1..1000). Fields use atomic reads; snapshots
may straddle concurrent progress. Queue depth is capped at the configured
channel bound during a concurrent receive transition; controls are excluded. Last success is Unix seconds of completion of a
whole nonempty batch across spans, metrics, distributions and logs, `None` before
that point; empty flushes and partial-stage acceptance do not update it.

Emission never waits for intake or a bookkeeping lock. It reserves channel
capacity before incrementing record counters and sending; concurrent producers
with available capacity do not shed records for bookkeeping contention. At worker
termination, dropped is derived from rejected plus accepted minus fully delivered
records, so submissions racing termination are accounted without counter
underflow. A failed batch remains bounded; additional received records are
shed, accounted locally. Successful chunks/stages are removed before progressing
and are not replayed after a later known rejection. Response loss is inherently
at-least-once: a request accepted remotely but lost locally can be retried.
Diagnostics expose safe signal/status/category/counts, never URLs or response
text; acknowledgement bodies are bounded to 64 KiB.

`flush` and `shutdown` each use one overall `timeout` budget (including control
queueing and acknowledgement). A timed-out flush leaves the worker live and
pending progress available for retry/recovery. Shutdown exits after its export
attempt even if rejected; if its control cannot finish by the deadline, it aborts
the worker. Cancellation unwinds the worker and records remaining observations
as dropped, then reports `WorkerStatus::Stopped`. This guarantees eventual
termination even with offline/slow intake, once the async runtime schedules
cancellation. Health remains readable after shutdown; later emits are rejected.

## Public HTTP transport for hosts without native lineage

Set `config.llmobs_transport = LlmObsTransport::PublicV1` to export spans to
`https://api.{DD_SITE}/api/intake/llm-obs/v1/trace/spans`, using the
[documented LLM Observability HTTP API](https://docs.datadoghq.com/llm_observability/instrument/api/).
The request contains `data.type = "span"` and `data.attributes` with `ml_app`,
`spans`, and sanitized configured tags. Public spans carry explicit
`apm_trace_id` only when a host supplied correlation context. Native SDK fields
are excluded from this wire format. An observation with native `span_links`
fails with `ExportError::UnsupportedSpanLinks`; choose `NativeV2` for that host.
The existing `NativeV2` default and native link transport are preserved.

Configured tags now propagate to duration distributions as well as lifecycle
counters, logs, and spans, using the same sanitization and 120-character bound.

On 2026-10-06 UTC, the crabber-channels disposable consumer verified a real
`opencode-go` / `deepseek-v4-flash` run through `PublicV1` on the local
`fix/channels-telemetry-gate` branch. A marker query through authenticated pup
returned agent, workflow, and LLM spans, one `crabber.model.elapsed_ms` metric
series, and two runtime logs. Native v2 intake acknowledged the comparison
probes but marker searches stayed empty; this does not establish native intake
visibility. No prompt or completion capture was enabled.

Validation: all 24 observability crate tests pass, including public wire shape,
APM mapping, refusal of unsupported native links, payload split preservation,
and configured tags on every distribution. Clippy passes with the existing
`crabber-session` `duration_suboptimal_units` warning allowed. The default
workspace gate was attempted but is blocked by the pinned Wasmtime 49.0.1
requiring Rust 1.96 while the installed stable toolchain is 1.95; this local
telemetry change does not update that unrelated dependency or suppress it.

## Searchable visibility gate

`cargo xtask verify-datadog` is an explicit live command requiring `DD_SITE`,
`DD_API_KEY`, and `DD_APP_KEY` with metric, log, and LLM span read access in the
same account/site as export. It launches a fresh marker-tagged fake run and waits
for flush/shutdown before reading. A successful flush proves transport acceptance;
HTTP acceptance alone does not prove searchable visibility.

The gate searches [LLM span events](https://docs.datadoghq.com/llm_observability/investigate/export_api/)
with structured `verify` tags and fixed RFC3339 bounds (start minus ten minutes,
end plus one minute), following cursors. Returned span attributes must prove
matching service/application/marker tags, a common trace and agent → workflow →
LLM parents. Resource IDs and intake `meta.kind` are not read span identities.
The same marker must yield finite run-count, run/model elapsed and model
first-token points through the [v1 metric query API](https://docs.datadoghq.com/api/latest/metrics/query-timeseries-points/),
and safe runtime logs. Metric request bounds use seconds; response point
and interval units use milliseconds. A rollup bucket overlapping the fixed
padded window is accepted even if its start precedes the run.

One 120-second deadline covers all reads, request time and sleeps. Responses are
bounded to 1 MiB each; failed permissions/schema requests fail immediately. Any
missing required signal gives a nonzero exit. Safe evidence includes site, marker,
committed source SHA, UTC bounds, statuses/counts and returned trace/span IDs;
response bodies and content are never printed. The default native span transport
is preserved; a native intake visibility failure is reported as a failure.
Offline tests and `cargo xtask check` do not read credentials or prove live results.

Host `DatadogConfig::tags` use the same ASCII filtering and 120-surviving-character
bound for timing distributions and event tags. Empty entries and duplicates keep
the established behavior. Tags are added once when a fresh request is built;
retained split/retry bodies are reused. Hosts own static-tag cardinality; runtime
provider/model/tool dimensions still have their independent finite bounds.

The earlier consumer report above belongs to PR #12's `0355b83` correction and is
historical evidence, not verification of this gate. Closing `crabber-9ghg` also
requires a channels WP0 re-probe against the exact verified correction revision.
