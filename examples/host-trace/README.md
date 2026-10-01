Run `cargo run -p host-trace -- memory` without credentials. It observes a fake
provider, two parallel tools, explicit 128-bit host context and facade broadcasts,
then persists an example queue envelope and runs paused admission, a fresh Agent
resume and duplicate delivery against the same MemoryStore. MemoryStore is volatile:
it demonstrates behavior parity, not persistence across operating system processes.

For the durable independent-process journey, set `CRABBER_TEST_POSTGRES_URL` to a
disposable PostgreSQL database, then run:

```sh
CRABBER_REQUIRE_POSTGRES=1 cargo run -p host-trace --features postgres -- postgres
CRABBER_REQUIRE_POSTGRES=1 cargo test -p crabber --features datadog,postgres --test trace_context --test admission_receipts
```

Run these on a clean committed checkout. The parent and each fresh worker print
that full source Git SHA, backend/schema (schema3 for PostgreSQL), fake provider,
session/run IDs, current and predecessor trace identities and callback counts.
They omit the DSN, payload and private paths. Workers are separate OS processes;
PostgreSQL retains the run and receipt. Missing PostgreSQL configuration fails
required tests instead of silently skipping. The example leaves only randomly
identified demo sessions in the disposable database; its temporary queue is removed.

Construct `TraceContext::new(trace_hex, span_hex)` before calling the facade's
`prompt_with_context` or `prompt_keyed_with_context`, or the runtime's
`start_with_context` or `start_keyed_with_context`. Their final argument is
`Option<TraceContext>`; existing context-free methods continue to work. Trace IDs
must have 16 or 32 hexadecimal digits; span IDs must have 16. Zero, other widths,
nonhex identity, unknown and duplicate serialized fields are rejected. Debug and
validation errors redact input. No baggage, prompts, arguments, credentials or
paths belong in context. Request content belongs in the host's separately secured
queue envelope, never in its correlation metadata or diagnostic output.

Add host observers with `Agent::builder().observer(Arc::new(observer))`. Repeated
calls compose observers. Override `emit_with_context` and
`model_completed_with_context` to inspect the explicit identity. Existing custom
observers implementing `emit` and optionally `model_completed` remain compatible:
the new methods default to those callbacks. Model completion is a separate observer
callback, not an additional facade broadcast. Host callbacks should be fast and
must not panic; callbacks execute synchronously on the run's tasks. Hosts own
subscriber and exporter initialization; no global subscriber is installed here.

Each execution owns immutable context across lifecycle, model and parallel tool
tasks. Concurrent sessions do not share context. Identity is transport metadata
excluded from the semantic admission fingerprint: retrying a keyed prompt with
different context returns its retained receipt without executing or changing it.
Queue acceptance is host durable acceptance; Crabber keyed admission happens in
the worker. The demo fsyncs queue content and its directory before acceptance.
This is a single-envelope demonstration, not an outbox, FIFO, cross-database
transaction or Agentcraft queue policy.

The host must retain stable session, admission key, input and behavior fingerprints,
request semantics, approved context, and queue ownership independently of process
lifetime. Validate the serialized context before admission. Retry the same keyed
request after ambiguous admission; lookup can be absent while another admission is
still committing. A retained receipt grants no execution handle. Do not execute
again merely because a queue message is delivered twice. Reconcile the receipt,
then choose explicitly whether to resume an eligible unfinished run. A live lease
or competing reclaim remains a conflict under the existing Store fence checks.

Use `resume_with_context(run_id, current_context)` on Agent or Orchestrator for a
fresh current attempt. Use `recover_with_context(|run| current_context_for(run))`
for a per-run selection; the callback is invoked only for runs whose lease appears
expired, and actual claim can still lose a race. The existing `resume`/`recover`
methods deliberately select no context, so old context-free records stay usable.
Paused calls resume persisted input. Expired pending calls run only when retry-safe;
expired running calls are interrupted to avoid repeating ambiguous side effects.
The execution identity, retained receipt and session remain unchanged; the lease
claim token changes and stale owners remain fenced.

Build a new current identity with `current.linked_to(&prior)?`. This rejects the
same numeric trace, including a 64-bit trace and its zero-padded 128-bit alias.
Only one predecessor trace/span pair is retained: linking to a context that already
has a predecessor strips the earlier link. Serde accepts this bounded identity and
rejects nested links. The prior pair is correlation only, never lease authority.
An exporter can derive a prior attempt's LLM root from the unchanged event run ID
and that predecessor pair. Host span identity is APM correlation; it is not an
internal LLM parent. Datadog payload mapping is handled in the next milestone slice.

Crabber Store remains at schema3. Correlation lives in host durable queue/journal
records; there is no Store migration or persistence requirement for custom Stores.
The example fsyncs its current attempt journal before its designated resume worker
claims the run. Production hosts must coordinate journal writes with their worker
claim policy, record the actual winning attempt, and preserve enough prior identity
to recover after loss before/after admission. A candidate that loses a lease race
must not replace the journal's winning attempt. No recursive baggage or attempt
history belongs in `TraceContext`; keep any required audit history in the host.

Acceptance evidence maps to `trace_context` (Memory/PG paused resume, fresh process,
validated bounded link, original schema3 rows/receipts and snapshot continuation),
`admission_receipts::process` (durable context, precommit loss, lost commit reply,
contention, replay, genuine worker kill during a running tool, fencing), and runtime
`recovery_selects_current_attempt_per_run_without_ambient_contamination` (two runs,
distinct new contexts). Existing runtime recovery tests retain context-free pending,
running and paused behavior. Automated tests need no provider or Datadog credentials.

## Captured exports and adoption

Run the assembled credential-free journey at the checked-out revision:

```sh
cargo run --quiet -p host-trace -- memory
env CRABBER_REQUIRE_POSTGRES=1 cargo test -p crabber --features datadog,postgres --test trace_context -- --nocapture
cargo run --quiet -p host-trace --features postgres -- postgres
```

PostgreSQL requires `CRABBER_TEST_POSTGRES_URL` pointing to a disposable database.
The required test mode fails when the variable is missing. Workers are separate
OS processes. The example prints the full source SHA, backend/schema, fake
provider, session/run, current/predecessor trace IDs, APM span, internal LLM root,
and assertion counts. It checks real HTTP bodies generated by the exporter; it
never prints their contents, private paths, DSNs, or credentials. Queue and
attempt journals retain only validated public correlation identity alongside the
host's explicit request envelope. Captures are temporary and removed on success.

`TraceContext::new` accepts nonzero trace IDs as 16 or 32 hexadecimal digits and
span IDs as 16 hexadecimal digits. `linked_to` carries only one predecessor and
requires a numerically different trace. `prompt_with_context`, keyed admission,
`resume_with_context`, and `recover_with_context` take explicit per-attempt
metadata. A keyed replay cannot replace a receipt or rerun execution. The host
owns API validation, authentication, durable queue admission, worker claims,
attempt journal durability, and subscriber/exporter initialization. Identity
never grants admission or lease authority. The demo queue is a single designated
worker fixture, not a production queue or cross-database transaction design.

Existing `Observer` implementors need no change: new contextual and attempt
callbacks default to the legacy callbacks. Context-aware composition should
forward `emit_in_attempt` and `model_completed_in_attempt` with the same runtime
attempt ID, Legacy wrappers forwarding only contextual callbacks preserve host identity,
but custom exporter bridges must forward attempt callbacks to preserve distinct
internal graphs and native lineage.
The facade broadcaster forwards each callback once to host observers and the
optional exporter. `crabber::obs` exposes direct exporter construction for hosts
that initialize their own subscriber. No global subscriber is installed. No `Store` trait or storage migration is required for trace context; context-free
records remain supported. PostgreSQL remains schema3. The host envelope and
attempt journal carry correlation metadata separately from execution storage.

An attempt's closed `crabber.attempt.admission` agent and
`crabber.workflow.admission` workflow anchors are emitted on `RunStarted` or
`RunResumed`, before work. Their duration is one nanosecond and describes causal
admission, not elapsed execution latency. Models and tools refer to the workflow
anchor; their durations and existing run-duration metrics retain their meaning.
Closed parents can have children that finish later, as allowed by the
[OpenTelemetry End specification](https://opentelemetry.io/docs/specs/otel/trace/api/#end).
Anchors are exported once; this design requires no same-ID update or indefinite
reopening. With context, internal LLM identity is deterministic from session,
run, and the fresh runtime observation attempt ID. The ID separates graphs even
when host context is absent or reused on resume. `linked_to_attempt` adds the
prior callback’s canonical nonzero observation-attempt UUID to the bounded link.
The host retains it in its attempt journal and discards older links. `linked_to`
without that ID remains usable prior host correlation metadata, but cannot
identify an LLM attempt and does not fabricate a native LLM link. Native recovery
links point to the previous LLM admission root, never the unrelated host APM span.

The killed-worker test flushes the original anchor/model export before killing
its process, waits for real lease expiry, and proves a fresh worker emits an
interrupted attempt linked to the captured original root. Nonblocking export
can lose buffered observations on abrupt termination. A host needing guaranteed
telemetry delivery must synchronize intake or supply its own durable observation
policy; an absent predecessor cannot be claimed visible in Datadog.

## Request-to-assertion evidence

| Request | Runnable assertion |
| --- | --- |
| Host callbacks, broadcasts, spawned models and parallel tools | `host_observes_model_parallel_tools_and_lifecycle_without_global_subscriber`; the barrier proves two tools execute concurrently; captured tool span IDs are unique |
| 64/full128/None isolation | `concurrent_contexts_and_context_free_execution_are_isolated` checks generated intake bodies per session and host association |
| Same/absent host identity across resume | `context_free_and_reused_host_context_resume_have_distinct_closed_graphs` proves fresh LLM IDs and no invented host links |
| Invalid context before mutation and keyed replay | `malformed_transport_context_is_rejected_before_admission`, `replay_ignores_replacement_context_without_receipt_or_execution_mutation` |
| Durable API handoff, fresh worker, retained schema3 data | `postgres_fresh_workers_preserve_schema3_receipts_sessions_and_cursors` with `CRABBER_REQUIRE_POSTGRES=1` |
| Paused recovery with native predecessor link | `durable_memory_paused_resume_and_duplicate_delivery` and required PostgreSQL fresh-worker test compare link IDs with actual prior captured roots |
| Process loss and valid lineage without reopening | `killed_worker_recovery_links_to_an_already_captured_closed_anchor` checks prior model/anchor presence, fresh trace/link, fencing and interrupted outcome |
| Prompt, output, argument, path and credential sentinel redaction | Real journey's mock HTTP decoder checks every signal; `all_intake_and_transport_diagnostics_hide_response_and_endpoint_secrets` checks all intake diagnostics and transport Display/Debug |
| Bounded queue, overflow, split and retry, flush/shutdown | `cargo test -p crabber-obs`; accepted chunks are retained across retries, failed shutdown remains retryable |
| External public adoption and required feature coverage | `cargo xtask check`, independent `testdata/external-consumer/check.sh`, CI PostgreSQL feature gate |

## Opt-in live correlation verification

Live linked LLM/APM/log evidence for this revision is **UNVERIFIED**: no approved
`DD_SITE`, `DD_API_KEY`, `DD_APP_KEY`, or host APM instrumentation was supplied.
A mock capture or intake HTTP 202 does not prove linked Datadog products.

In a host-managed Python environment install the pinned official SDK and configure
an existing host Datadog Agent using `DD_TRACE_AGENT_URL`, and provide approved
`DD_SITE`, `DD_API_KEY`, and `DD_APP_KEY` through the environment. From the pinned,
clean checkout run:

```sh
python -m pip install "git+https://github.com/DataDog/dd-trace-py.git@57aff59616e141dbf16cf92ac868a78b37ef7e1a"
DD_TRACE_128_BIT_TRACEID_GENERATION_ENABLED=true python examples/host-trace/live.py
```

The driver creates real host admission and recovery APM spans with independent
traces and a unique `verify` marker, passes their actual identities to the Rust
journey, sends the same spans/logs to Datadog alongside mock assertions, finishes
host spans, and flushes the existing host tracer. It prints only source SHA,
marker and approved IDs. The marker is also included in exported LLM tags and
logs. It intentionally reports correlation UNVERIFIED after submission.

Using the site's authenticated Datadog UI or approved read client, search LLM
Observability and logs for `verify:<printed-marker>` and APM Trace Explorer for
`@verify:<printed-marker>`. Allow ingestion time; record the source SHA and
marker with the actual returned IDs/counts. Verify all agent/workflow/model/tool
spans, their internal parent IDs, two distinct attempts and the native recovery
link to the original LLM root. Open each LLM span's associated APM trace and check
it is the driver's actual admission/recovery trace; verify the logs' `dd.trace_id`
and `dd.span_id` match that host trace/span (full128 hexadecimal trace above u64,
span decimal). Check the marker-scoped APM spans exist and have the pinned
`source_sha`. Record returned evidence or UI URLs for all three products. A
missing product, association, or native link remains an unverified/failed live
check even if every intake request succeeds. Never paste credential values.
