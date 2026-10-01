# Host-local operational observations

Register `Arc<dyn Observer>` with `AgentBuilder::observer`. Multiple observers
receive each callback once alongside the existing facade event broadcasts and
optional Datadog observer. Implement `operational_completed` for safe measurements,
or `operational_completed_in_attempt` to retain the execution attempt and optional
host `TraceContext`. The contextual default delegates to the plain callback.
Callbacks run inline and must remain bounded and nonblocking: enqueue a bounded
copy and return, rather than performing network I/O. Avoid panics in callbacks.

```rust,ignore
use crabber::{Observer, OperationalObservation};
struct Host;
impl Observer for Host {
    fn emit(&self, _: &crabber::EventRecord) {}
    fn operational_completed(&self, sample: &OperationalObservation) {
        // Submit sample.elapsed.as_secs_f64() and, if present, first_token.
        // Choose bounded tags from sample.kind and sample.reason.
    }
}
// Agent::builder().observer(std::sync::Arc::new(Host)) ...
```

These observations describe a local execution attempt. They never claim that a
run or call settled durably. In particular, `LeaseLost` is emitted locally once
when an attempt loses ownership; it does not emit a durable `RunSettled`, settle
unfinished calls, or invoke run settlement through the lost fence. A replacement
owner remains authoritative. When a store rejects an operation before heartbeat
notices a takeover, the runtime checks retained claim identity/lease expiry before
failure cleanup. Verified loss produces the same local LeaseLost run sample and
skips subsequent cleanup/settlement. The original returned `RuntimeError::Store`
is preserved; an ordinary Conflict with a live retained fence remains RuntimeError.
A paused run intentionally releases its lease; unchanged paused ownership is
not classified as loss. A failed verification read cannot assert loss. Store failures in model/tool setup
are RuntimeError, while provider acquisition failures and tool-body failures retain
their own classifications. Existing execution-store fencing remains in effect
for all other writes. Admission rejection and keyed replay do not execute and
produce no operational run observation.

| Reason | Meaning |
| --- | --- |
| `Success` | Model stream completed with valid content, tool returned a successful result, or run returned Completed. |
| `ProviderError` | Model acquisition or consumption failed (including invalid stream protocol). |
| `ToolError` | Tool preparation/validation, permission denial, execution, or result transformation failed. A later successful model/run does not erase this call observation. |
| `Cancelled` | Explicit cancellation stopped an in-flight model/tool or the run returned Interrupted. |
| `LeaseLost` | This execution attempt stopped because lease ownership was lost. |
| `Paused` | The run checkpointed and returned Paused; this is an attempt boundary, not a durable terminal run status. |
| `RuntimeError` | Other execution failures, such as a store/extension failure or turn limit. |

Pausing ends the active run measurement. Staged calls which have not executed
produce no tool duration; cancelling an executing call produces Cancelled. Resume and recovery create fresh attempt identities and run
measurements after a successful lease claim. Time between attempts is omitted.
Interrupted calls reconciled from a predecessor are represented by existing
durable events; they do not fabricate a tool execution duration for work this
host did not perform. Abandoning a paused/crashed run without resume/recovery
produces no execution observation; dropping a handle does not cancel its task.
Unexpected task/process termination cannot guarantee a final callback.
`RuntimeError` and `RunStatus` return semantics are unchanged.

`OperationalObservation` contains the session/run identity, finite operation kind
and reason, elapsed `std::time::Duration`, and an optional first-token duration.
It contains no error messages, token content, tool arguments/results, headers, or
credentials. Provider/model/tool names remain host-defined identities: exporters
must apply an allowlist or a finite cap with an overflow category before using
them as metric tags. Never tag metrics with session/run/attempt/trace/span/call
IDs. Existing text broadcasts intentionally still carry text; they are a
separate host-facing stream and must not be forwarded unredacted to telemetry.

Elapsed timing uses a monotonic clock unrelated to event timestamps and lease
wall time. `SystemMonotonicClock` uses `Instant`; wall-clock changes affect neither
latency nor first-token samples. `AgentBuilder::monotonic_clock` and
`OrchestratorBuilder::monotonic_clock` accept an injectable clock returning
`Duration` from any fixed origin. Regressing test/custom clocks saturate elapsed
to zero. Durations are nonnegative and finite; convert to seconds or milliseconds
explicitly at the consumer boundary.

Run timing starts as the admitted task begins, before acquiring/renewing its
execution store, and ends as execution/settlement returns. Resume timing starts
immediately after claiming its lease. Model timing starts before model callbacks,
extension/provider stream acquisition, and consumption; it ends at stream
completion/failure, before durable assistant-message commit. Compaction measures
each summary stream separately with `ModelPurpose::Compaction`; normal turns use
`ModelPurpose::Turn`. Tool timing starts before claiming its staged call, includes
permission/approval wait, tool body and result transformation, and ends before
durable tool-result settlement. Failed/cancelled calls remain eligible samples.
Queue/network delays in an exporter are outside these measurements.

First-token timing is recorded once at the first **nonempty TextDelta** of each
model call, including compaction. Empty text, reasoning, tool events and provider
state do not trigger it. Calls with no text omit the sample; a failed or cancelled
call that already emitted text retains its first-token sample. Neither the
measurement nor the callback carries that text.

This is an additive Observer API; existing implementors compile with default
methods, but custom observer adapters must forward the new callback to receive
measurements. The facade forwards it to composed observers with the same fresh
attempt/context identity as existing callbacks. No database schema or persisted
event-format migration is required. Legacy `model_completed` and durable
`RunSettled`/`ToolCallSettled` events remain available; adopt the typed callback for
complete operational samples, including provider startup failure and lease loss.
Legacy model/tool elapsed payloads now also use monotonic time. Do not double
count typed measurements and legacy elapsed events.

Credential-free evidence lives in `crabber-runtime` tests (`operational_*` plus
compaction cancellation tests) and `crabber/tests/operational_observation.rs`.
The latter proves a scripted provider/native tool turn through two composed
observers plus facade broadcasts and verifies context/attempt identity. Runtime
fixtures use an injected clock, setup/text notification barriers and paused Tokio
time. To drive lease loss via a facade host, provide a `MemoryStore::with_clock`
using `ManualClock`, wait for a provider/tool barrier, set its wall clock past
`Store::get_run(run_id).lease_until` (rather than guessing the admission time), call `Store::claim_expired_run`, and allow a heartbeat tick; retain the
replacement fence and assert there is no durable RunSettled. A custom Provider
can expose setup/consumption barriers for cancellation; `RunHandle::interrupt`
cancels either boundary without an exporter or credentials.

Datadog distributions and local exporter health are provided by the following
exporter slice; this runtime API alone does not promise network submission or
live percentile visibility.
