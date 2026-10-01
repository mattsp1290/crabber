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
