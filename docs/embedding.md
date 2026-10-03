# Embedding Crabber

The default facade has no provider credentials, database, WASM, observability or
AG-UI dependency. Begin with `Agent::builder().memory()` and a FakeProvider;
subscribe to live events, then await the run's durable completion. Enable only
capabilities your host uses.

| Capability | Code/guide | Runnable command / feature |
| --- | --- | --- |
| Memory + fake provider | [minimal embedding](../examples/minimal-embed/src/main.rs) | `cargo run -p minimal-embed` |
| PostgreSQL persistence | [session store](../crates/crabber-session/README.md) | `cargo run -p minimal-embed -- --store postgres`; set `CRABBER_POSTGRES_URL`, dedicated PostgreSQL 14+; facade `postgres` |
| Real providers | [provider usage](../crates/crabber-providers/README.md) | facade `anthropic`, `openai`, `codex`, `opencode-go`; manual service configuration |
| Fenced abandonment | [host protocol](fenced-abandon.md) | `cargo run -p fenced-abandon`; memory default, manual PostgreSQL mode |
| Native extensions | [native guide](../examples/native-extension/README.md) | `cargo run -p native-extension` |
| Workspace context for native extensions | [contract](#workspace-context), [external-consumer probe](../examples/workspace-context-probe/README.md) | `cargo test -p workspace-context-probe` |
| WASM extensions | [WASM guide](../examples/wasm-extension/README.md) | `cargo xtask build-fixtures`, then `cargo run -p wasm-extension`; facade `wasm` |
| Codex browser login | [login guide](../examples/codex-login/README.md) | `cargo run -p codex-login -- status`; manual local credential access |
| LLM observability | [Datadog guide](../examples/datadog-export/README.md) | `env -u DD_API_KEY cargo run -p datadog-export`; facade `datadog` |
| Operational distributions | [operational guide](../examples/operational-telemetry/README.md) | `cargo run -p operational-telemetry -- --check` |
| AG-UI SSE | [projection contract](ag-ui.md), [host guide](../examples/agui-sse/README.md) | `cargo run -p agui-sse -- --check`; separate opt-in `crabber-agui` crate |

[Admission receipts](admission-receipts.md) describe ambiguous-response retries
without starting duplicate executors. [Bounded snapshots](bounded-snapshots.md)
provide bounded all-history reads. [Host tracing](../examples/host-trace/README.md)
covers explicit context and composed observers; [operational observation](operational-observation.md)
describes finite run/model/tool reasons and monotonic durations. Use those
contracts when adopting their corresponding public APIs.

`cargo xtask check` builds local WASM fixtures, checks formatting/Clippy, runs
workspace tests, offline host journeys and the external public-API consumer,
and enforces minimal-embed's 60-line marked glue limit. It requires no credentials
or external accounts. Real PostgreSQL/provider setup, login/logout and live
Datadog ingestion are manual external-service steps. Do not run Codex credential
commands in CI against a user's real store. Never let inherited Datadog keys
turn an offline example check into an external write. Generated WASM stays local.

## Workspace context

Native extensions can read the workspace identity of the session a run belongs
to. The values are **routing data, never authorization**: use them to pick a
root or an owner record, and keep trust decisions, process policy and
credentials in the host.

### Public API

| Surface | API |
| --- | --- |
| Type | `crabber::extension::WorkspaceContext` with `workspace_id() -> Option<&str>` and `directory() -> Option<&str>` |
| Tool | `ToolContext::workspace() -> &WorkspaceContext`, read-only, available in `ToolExecutor::execute_with_context` |
| Dynamic prompt contribution | the `ContextAssemble` transform payload carries `workspace_id` and `workspace_directory` (`WorkspaceContext::WORKSPACE_ID_KEY`, `WorkspaceContext::DIRECTORY_KEY`); each is a JSON string, or `null` when unavailable |
| Constructor | `ToolContext::new` takes a required `WorkspaceContext` after `host`. Only code that builds a `ToolContext` itself is affected; `ToolExecutor::execute` and `execute_with_context` are unchanged |

`None` / `null` is the explicit unavailable value. An extension must handle it
and cannot read a default.

Values are exposed verbatim. A directory may be relative, in which case it
resolves against the working directory of whichever process runs the tool.
`AgentConfig::new` defaults to workspace ID `"default"` and directory `"."`;
those are persisted with the session and exposed as available values. Hosts
that route on workspace identity should set both explicitly.

### Source and guarantees

The persisted `Session` record (`workspace_id`, `directory`) is the only source,
on every path. The request saved in a run checkpoint and the request retained in
a keyed-admission capsule are never used to populate extension context; they
are only compared with the session.

- **Missing.** A field is missing only when the persisted session holds an
  empty string for it. The run does not fail; that field is unavailable. The
  runtime never substitutes `AgentConfig` values, the process working
  directory or model-supplied tool arguments. A `Running` run reclaimed after
  its lease expired has no checkpoint request, and that is not missing
  context: it sees exactly what an uninterrupted run sees.
- **Invalid.** A stored session that cannot be decoded, or compared values
  that differ, fail closed with a typed error before any executor or
  `ContextAssemble` handler runs. A difference is
  `StoreError::SessionIdentityMismatch`.
- **Comparison.** Exact string equality, per field. There is no lexical
  normalization, no symlink or filesystem resolution and no check that the
  directory exists; `/srv/a` and `/srv/a/` differ. An empty string is a value
  like any other, so empty against non-empty is a mismatch in either
  direction. Availability is decided only after every comparison passes.
- **Handlers cannot change the values.** The runtime re-asserts both keys
  after every `ContextAssemble` handler, so a handler that overwrites or
  removes them affects neither later handlers nor the tool context. A handler
  that returns anything other than a JSON object fails the turn with
  `ExtensionError::Rejected`.
- **Same turn, same values.** The tool context and the `ContextAssemble`
  payload of a run come from one read of the session. `ContextAssemble` runs
  once per turn, including turns after resume or recovery; contributions are
  re-invoked, not replayed.
- **Plan fingerprint.** Workspace values are not fingerprint inputs. The
  fingerprint covers registered component identities (and, for a static plan,
  its static prompt text); two sessions in different workspaces with the same
  components have the same fingerprint. Handler output is neither
  fingerprinted nor persisted, so output that varies with the workspace does
  not affect receipts or replay.

### Path matrix

| Path | Comparison | On conflict |
| --- | --- | --- |
| `prompt` creating a session | none; the request's values are persisted | n/a |
| `prompt` into an existing session | host-presented: `AgentConfig` workspace ID and directory against the session, inside the admission transaction | `SessionIdentityMismatch`; no run is created |
| `prompt_keyed` | host-presented, inside the admission transaction, before the receipt lookup | `SessionIdentityMismatch`; no run, no replay |
| `resume`, `resume_with_context` of a paused run | stored-record: the checkpoint request against the session, before plan acquisition, the claim and any pending tool | `SessionIdentityMismatch`; no plan, fence or lease is taken and the run keeps its status |
| `resume` of a reclaimed `Running` run with no checkpoint request | none | n/a |
| `recover`, `recover_with_context` | as `resume`, per run | the run is skipped, listed in the returned `RecoverReport`, and the sweep continues |
| `recover_admission` | host-presented, before the receipt lookup and before any replay | `SessionIdentityMismatch`; no claim, no replayed receipt |

A run rejected on `resume` stays unfinished and is rejected again on every
later attempt; it does not heal itself, and its session stays busy. `recover`
returns a `RecoverReport`: the runs it recovered, plus a `SkippedRun` with the
reason for each expired run it left unfinished: an identity mismatch, a session that is
missing or cannot be decoded, a checkpoint request without identity
(`Validation`), a live competing claim or an unstarted keyed admission. Dispose
of a skipped run through [fenced abandonment](fenced-abandon.md).

### Unsupported context

WASM guests do not receive workspace context. The WIT contract and the
guest-visible `turn-metadata` record are unchanged (its `workspace-id` field
stays the empty string), and no guest rebuild is needed. Exposing it to guests
is a separate change with its own WIT version decision.

### Schema, adoption and rollback

There is no schema change and no migration: sessions already store both fields,
and the schema version stays 5. Adopt by upgrading the crate and, only if you
construct `ToolContext` yourself, passing the new argument. Roll back by
reverting to the previous revision. Neither direction needs a data migration.

Upgrade hazard: earlier revisions let an unkeyed `prompt` into an existing
session present a different workspace ID or directory, and a run admitted that
way saved the different values in its pause checkpoint. After the upgrade such a
paused run is rejected on `resume`, skipped by `recover` and keeps its session
busy. Before upgrading, let paused runs finish or settle them; after upgrading,
find any in the `skipped` list of `recover` and `abandon` them once their lease
expires.

One behavior change comes with adoption: an unkeyed `prompt` into an existing
session now rejects a different workspace ID or directory, as keyed admission
already did. A host that reuses a session must present the identity it was
created with. Sessions created with an empty workspace ID or directory keep
working and expose the unavailable value.

## Tool result transform context

Status: **design gate for epic `crabber-miia` (request `crabber-r-u7l3`)**. This
section is the contract the implementation slices build against. Nothing in it
is implemented at the commit that introduces it; line references are to
`a36b639`. Where this section and a slice description disagree, this section
wins.

The `crabber/tool/result-transform` point changes from an untyped
`{"result","is_error"}` waterfall into a typed chain: every handler receives
the authoritative, read-only call context, and `result` is the only value a
handler can change. It is a clean break (D9): no compatibility shim, no feature
flag.

### Decisions

| Decision | Text | Status | Covered by |
| --- | --- | --- | --- |
| D1 Transform layers | `ToolPipeline::transform_result` stays a host-owned pre-stage. It runs first, only on executed output (class `Succeeded`), with the same read-only context. The registry-mounted result chain is the single guaranteed final layer on all five outcome paths, and protected-persistence guarantees attach to that chain only. The pre-stage runs inside the run-token `select!` around `permit_and_execute` and receives the D5 child token in its context. Its output is never an accepted value: cancellation during the pre-stage settles `Interrupted` with the fixed runtime text. A pre-stage `Err` or panic follows D2 (handler id `crabber/tool-pipeline`) and the extension chain is then skipped, because nothing tool-authored is left to protect. Cleanup the pre-stage owns goes through a host-supplied `CleanupOwner` (see Public API), because a `ToolPipeline` has no mount. | confirmed | TBD (coverage audit) |
| D2 Transform failure | If a handler returns `Err`, panics, returns a malformed or non-envelope value (JSON, WASM), returns context that differs from the authoritative context, or its mount starts closing before or while it runs, and cancellation has not been observed, the call settles `Failed` with the fixed runtime text naming the handler id. Remaining handlers, including final redactors, are skipped. Neither the original output, nor any intermediate value, nor handler-authored error text is persisted. This replaces `Err(error.to_string())` and `unwrap_or_else(\|\| output.clone())` (`orchestrator.rs:3210-3222`). | confirmed | TBD (coverage audit) |
| D3 Input availability | Input is the tagged `ToolInput`. Success, execution error and permission denial: `Normalized`, the post-`ToolPrepare` `record.arguments` exactly as given to guards and policy. Unknown tool: `Raw`, the provider arguments, not normalized and not validated. Preparation error: `Unavailable { reason: PrepareFailed }`. Input is never reconstructed, reparsed or inferred from result text, and `ToolPrepare` is never rerun. The tool name is the provider-requested `call.name` with `resolved: bool`, false only for class `UnknownTool`. | confirmed | TBD (coverage audit) |
| D4 Status | The class is the immutable `ToolOutcomeClass`. A handler returns `TransformOutput { result, mark_error }`. `mark_error` is the only way to escalate success to error; nothing turns an error into success and nothing changes the class. Persisted `is_error` is `class != Succeeded` or any handler (or the pre-stage) set `mark_error`. Native context is read-only by type; in the JSON and WASM envelopes a changed context is a D2 failure. The same rules apply to native, JSON, WASM and `ToolPipeline`. | confirmed | TBD (coverage audit) |
| D5 Cancellation plumbing | `execute_tool` creates one child token of the run `cancellation` per call and puts it in the typed context. It is never in a JSON payload. The driver selects on it around every handler, with the cancellation arm polled first, and the call settles `Interrupted`. The existing `select!` around `permit_and_execute` (`orchestrator.rs:3173`) becomes `biased` with cancellation first. Cancellation must be observed and durably settled within `INTERRUPT_SETTLEMENT_BOUND` (1s), asserted by tests against the named constant. WASM guests in the ordinary phase are interrupted by epoch interruption tied to the token; no opt-out is taken. `resume` and `recover` gain no caller-visible cancellation source (see Recorded answers). | confirmed | TBD (coverage audit) |
| D6 Fallback acceptance and final redaction | A value is accepted once a handler of either phase returns a valid output and the driver commits it before it observes cancellation. The seed is never an accepted value. Final redactors are registered with an explicit phase and always run after every ordinary handler, whatever their `order`. After cancellation, ordinary handlers are skipped; final redactors still run on the accepted value under one deadline and can see the cancelled token. With no accepted value, or when a final redactor fails or the deadline expires, the call settles `Interrupted` with the fixed runtime text. Because ordinary handlers are skipped after cancellation, an ordinary-phase redactor protects nothing once cancellation is observed: a redactor relied on for protection must be registered as a final redactor. | confirmed | TBD (coverage audit) |
| D5/D6 control flow | One chain driver iterates the handlers and keeps the latest accepted value in its own state, outside every callback future. It is implemented as a `Dispatcher` method in `crabber-extension` (handler mount data is crate-private there) and called by the runtime. On cancellation it drops only an in-flight ordinary handler's future; that handler's cleanup survives through D7. An in-flight final redactor is not dropped merely because cancellation was observed, provided a value had been accepted before it started. It is dropped when nothing was accepted (it was running on the seed), when the deadline expires, when its mount starts closing, or when a `Parallel` sibling's error drops the whole call; each of those ends in the fixed text. The driver keeps running and drives the remaining final redactors under `FINAL_REDACTION_DEADLINE` (500ms), one deadline measured from the instant cancellation was observed. Cancellation to durable settlement stays within the D5 bound. | confirmed | TBD (coverage audit) |
| Settlement precedence | Once the driver has observed cancellation the outcome is always `Interrupted` with the fixed runtime text, unless an accepted value then passes every remaining final redactor, at least one of which ran after cancellation or was in flight when it was observed; then only that redacted value is persisted, still with status `Interrupted`. With no final redactor mounted nothing tool-authored is persisted after cancellation. A failure, panic, malformed envelope, context mismatch, mount close or deadline expiry after cancellation also yields `Interrupted` with the fixed text; D2 `Failed` applies only when cancellation was not observed. A driver that returned before cancellation was not interrupted: its result is persisted normally and the run is interrupted at its next check. Race tests cover cancellation during an ordinary handler, between handlers, during a final redactor, and a final redactor failing, panicking or timing out after cancellation, plus cancellation with no final redactor mounted. | confirmed | TBD (coverage audit) |
| D7 Cleanup and mount close | Each mount owns a cleanup tracker, exposed through the context, so cleanup survives a dropped callback future. `MountHandle::close` deactivates the mount, sends the close signal (which also fails that mount's in-flight and later result-transform invocations per D2), then waits for plan leases and the tracker under one bound (default 5s, configurable). On expiry it returns `ExtensionError::MountCloseTimeout` naming the extension and calls the close observer. It never aborts a task. Cleanup that owns a child reacts to the close signal by killing it (`start_kill`, `kill_on_drop` as backstop) and keeps its permit until `wait()` returns. After a timeout the mount's detached close task remains as the process-lifetime reaper: it keeps waiting, so permits are held until children are reaped, then runs rollback and `shutdown`. `Registry::close_all` makes the registry terminal before it deactivates any mount (Mount close, below). | overridden: the default text made the reaper "owned by the runtime". `crabber-extension` cannot depend on `crabber-runtime`, and `MountHandle::close` already runs its work in a detached task (`registry.rs:415`), so that task is the reaper and `crabber-extension` owns it. The observation is delivered through a registry-level observer and a defaulted `Observer` method, not the runtime `OperationalObservation`, which requires a session and run. Everything else is confirmed. | TBD (coverage audit) |
| D8 Interruption test infrastructure | The real interruption test runs the real `Orchestrator` through the `crabber` facade in `crates/crabber/tests/`, with `MemoryStore`, in the `check` job. The fixture reduction child is source only, built at test time, modeled on `crates/crabber/tests/admission_support/{child,process}.rs`. The child writes a ready line on its pipe before the test interrupts. "Permit" is a reducer-owned semaphore permit per child, held by the cleanup task until `wait()` reaps it. The test asserts the permit count recovers only after reaping, the PID is gone, pipes are closed, both bounds hold against the named constants, mount close through `Agent::close_extensions` joins the cleanup, and the durable status is `Interrupted` with no unredacted output. | confirmed | TBD (coverage audit) |
| D9 ABI, schema and fingerprint | Clean break, no shims. The point ID stays `crabber/tool/result-transform`. `RESULT_TRANSFORM_CONTRACT_VERSION` enters `compute_fingerprint`, so every plan frozen under the old contract fails the strict check (`RuntimeError::PlanChanged`) instead of resuming. WASM guests get the new envelope through `crates/crabber-wasm/src/adapters.rs` with no WIT change; only guest fixture sources change and generated `fixtures/wasm/` binaries stay uncommitted. There is no durable schema change; one record-content change is recorded (unknown-tool arguments, Recorded answer 1). | confirmed | TBD (coverage audit) |
| D10 Recovery and replay | On `resume` and `recover`, an unfinished call takes one of two paths, decided from the stored record. **Fixed settlement:** a call found `Running`, and a `Pending` call that is not `retry_safe` on a run that is not `Paused`, is settled by `settle_interrupted_call` with status `Interrupted` and the fixed runtime text. No pre-stage, no transform, no final redactor runs, and nothing tool-authored is persisted. **Re-execution:** every other `Pending` call (`retry_safe`, or any `Pending` call of a `Paused` run) is re-claimed and run through `execute_tool`: executor, pre-stage and the full result chain, with the same durable call, session and run IDs and with class and input derived from the stored record only, never from in-memory substitutes. `ToolPrepare` is not rerun. | overridden: the default text said "transforms never run during recovery", which does not match the code. `resume_loaded` re-executes `Pending` calls through `execute_tool` and its whole chain (`orchestrator.rs:984-1024`). The decision is restated as the split above; the fixed path is unchanged. | TBD (coverage audit) |
| D11 Human gates | Slices that publish (merge to `main`, tag or release), reply on `bn request crabber-r-u7l3`, or record acceptance are labeled `human-gate` and are not executed by agents without explicit user approval. Agents may prepare drafts only: release notes and the response file under `$HOME/.agents/projects/crabber/responses/`. The external consumer probe is a standalone crate outside the workspace that depends on the published git revision, needs no credentials, and passes when `cargo test` exercises D3/D4 context binding and a reducer then final-redactor chain. | confirmed | TBD (coverage audit) |

### Public API

All items below are new or changed. Items in `crabber-extension` are reachable
as `crabber::extension::…`; items in `crabber-runtime` as `crabber::runtime::…`.

#### Types (`crabber-extension`, new module `result_transform`, re-exported from the crate root)

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum ToolInput {
    Normalized(serde_json::Value),
    Raw(serde_json::Value),
    Unavailable { reason: InputUnavailable },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputUnavailable {
    /// Preparation failed; there is no normalized input.
    PrepareFailed,
    /// The record holds no raw provider arguments for an unresolved tool.
    Unresolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolOutcomeClass {
    Succeeded,
    ExecutionFailed,
    PermissionDenied,
    UnknownTool,
    PrepareFailed,
}
impl ToolOutcomeClass {
    pub const fn is_error(self) -> bool; // false only for Succeeded
    pub const fn as_str(self) -> &'static str; // the JSON encoding below
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformPhase {
    Ordinary,
    FinalRedaction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransformOutput {
    pub result: serde_json::Value,
    pub mark_error: bool,
}
impl TransformOutput {
    pub fn new(result: serde_json::Value) -> Self; // mark_error: false
    pub fn marked_error(result: serde_json::Value) -> Self; // mark_error: true
}

/// What the runtime gets back from the chain driver.
#[derive(Debug, Clone, PartialEq)]
pub enum ToolResultOutcome {
    /// Every handler ran; cancellation was not observed.
    Completed { result: serde_json::Value, is_error: bool },
    /// D2. Carries only the id of the failing handler.
    Failed { handler: String },
    /// Cancellation was observed. `Some` only when an accepted value passed
    /// every remaining final redactor (settlement precedence).
    Interrupted { redacted: Option<serde_json::Value> },
}

pub type ResultTransformCallback = Arc<
    dyn Fn(ToolResultContext, serde_json::Value)
            -> BoxFuture<'static, Result<TransformOutput, ExtensionError>>
        + Send
        + Sync,
>;
```

`ToolResultContext` has private fields and no setters visible to a handler. It
is `Debug + Clone` and cheap to clone.

```rust
impl ToolResultContext {
    /// Built by the runtime (and by tests). Phase is `Ordinary`, the token is
    /// never cancelled and the tracker is detached until replaced.
    pub fn new(
        tool_name: String,
        resolved: bool,
        input: ToolInput,
        call_id: ToolCallId,
        session_id: SessionId,
        run_id: RunId,
        class: ToolOutcomeClass,
    ) -> Self;
    pub fn with_cancellation(self, cancellation: CancellationToken) -> Self;
    pub fn with_cleanup(self, cleanup: CleanupTracker) -> Self;

    pub fn tool_name(&self) -> &str;
    pub fn resolved(&self) -> bool;
    pub fn input(&self) -> &ToolInput;
    pub fn call_id(&self) -> &ToolCallId;
    pub fn session_id(&self) -> &SessionId;
    pub fn run_id(&self) -> &RunId;
    pub fn class(&self) -> ToolOutcomeClass;
    /// `class().is_error()`, or an earlier handler or the pre-stage set `mark_error`.
    pub fn is_error(&self) -> bool;
    pub fn phase(&self) -> TransformPhase;
    pub fn cancellation(&self) -> &CancellationToken;
    pub fn cleanup(&self) -> &CleanupTracker;
}
```

The constructor and the two `with_*` builders are public because the runtime
lives in another crate. They grant nothing: the runtime never reads a context
back from a native handler, and the driver replaces the phase, the effective
`is_error` and the tracker for every handler it invokes. Setting `phase` and
`is_error` is crate-private.

#### Cleanup tracker (`crabber-extension`, same module)

```rust
/// Handed to callbacks. Wraps `tokio_util::task::TaskTracker` and a close signal.
#[derive(Debug, Clone)]
pub struct CleanupTracker { /* private */ }
impl CleanupTracker {
    /// A tracker with no owner: its close signal never fires and nothing joins it.
    pub fn detached() -> Self;
    /// Spawns on the current Tokio runtime. The task is tracked until it
    /// finishes, whether or not the returned handle or the caller is dropped.
    pub fn spawn<F>(&self, task: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
    /// Resolves when the owner starts closing. `'static`, so it can move into a task.
    pub fn closing(&self) -> tokio_util::sync::WaitForCancellationFutureOwned;
    pub fn is_closing(&self) -> bool;
    /// Tracked tasks that have not finished.
    pub fn pending(&self) -> usize;
}

/// Owner side. A mount holds one; a host with a `ToolPipeline` may hold one.
#[derive(Debug)]
pub struct CleanupOwner { /* private */ }
impl CleanupOwner {
    pub fn new() -> Self; // also `Default`
    pub fn tracker(&self) -> CleanupTracker;
    /// Sends the close signal. Idempotent. Never aborts a task.
    pub fn close(&self);
    /// `close()`, then waits for every tracked task, at most `bound`.
    pub async fn join(&self, bound: Duration) -> Result<(), CleanupJoinTimeout>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupJoinTimeout { pub pending: usize }
```

A callback cannot close or join a tracker. `spawn` after the close signal still
runs and tracks the task, so cleanup started late is not lost.
`ToolResultContext::new` and a `Handler` that has not been mounted hold
`CleanupTracker::detached()`. `CleanupTracker` lives in `result_transform.rs`
and has a crate-private constructor from a `TaskTracker` and a close token;
`CleanupOwner` and `CleanupJoinTimeout` live in `registry.rs` and are built on
it.

#### Registration (`crabber-extension`, `Registrar`)

```rust
impl Registrar {
    /// Typed handler, phase `Ordinary`.
    pub fn on_result_transform(&mut self, order: i32, id: impl Into<String>, cb: ResultTransformCallback);
    /// Typed handler, phase `FinalRedaction`.
    pub fn on_final_redaction(&mut self, order: i32, id: impl Into<String>, cb: ResultTransformCallback);
    /// JSON envelope handler, phase `FinalRedaction`.
    pub fn on_final_redaction_json(&mut self, order: i32, id: impl Into<String>, cb: Callback);
}
/// The envelope adapter both JSON registrations use.
pub fn json_result_transform(cb: Callback) -> ResultTransformCallback;
```

- JSON `on_transform(ToolResultTransform::ID, order, id, cb)` **stays** and is
  not rejected at mount. It keeps storing an ordinary JSON callback; the driver
  applies `json_result_transform` to it and treats it as phase `Ordinary`. A
  JSON handler becomes a final redactor through `on_final_redaction_json`,
  which stores `json_result_transform(cb)` with phase `FinalRedaction`. JSON
  handlers receive neither the token nor the tracker; they are interrupted
  only by their future being dropped.
- **A redactor relied on for protection must be a final redactor**
  (`on_final_redaction` or `on_final_redaction_json`). Ordinary handlers are
  skipped once cancellation is observed, so when any final redactor is mounted
  the accepted value can be persisted without an ordinary-phase redactor having
  run. That covers every `on_transform` and `on_result_transform` handler and
  every WASM guest. The example redactors in `examples/native-extension` and
  `examples/agui-sse` are registered with `on_final_redaction`.
- `Handler` gains `pub(crate) phase: TransformPhase` and
  `pub(crate) cleanup: CleanupTracker` (default `CleanupTracker::detached()`),
  and `HandlerFn` gains `ResultTransform(ResultTransformCallback)`.
  `Registry::mount` stamps the mount's tracker onto each handler where it
  already stamps `mount_id` (`registry.rs:239-243`); the driver reads
  `handler.cleanup` for both the tracker it puts in the context and the mount's
  close signal.
- Chain order: every `Ordinary` handler in the existing sort order
  `(order, scope_rank, mount_seq, registration_seq)`, then every
  `FinalRedaction` handler in that same order. Phase dominates `order`.
- The handler component identity for this point becomes version
  `"{order}:{mount_seq}:{phase}"` with phase `ordinary` or `final_redaction`,
  so moving a redactor between phases changes the plan fingerprint.
- `Dispatcher::transform::<ToolResultTransform>` and
  `transform_pinned::<ToolResultTransform>` end up returning
  `Err(ExtensionError::Rejected(ToolResultTransform::ID))`, so the driver is
  the only way to run this point and no caller can bypass it silently. The
  rejection lands last (`crabber-8q7h`), after every in-tree caller has moved
  to the driver; until then the generic waterfall keeps its old behaviour so
  each intermediate slice passes the workspace tests.

#### Driver (`crabber-extension`, `Dispatcher`)

```rust
impl Dispatcher {
    pub async fn transform_tool_result(
        &self,
        context: ToolResultContext,
        seed: TransformOutput,
    ) -> ToolResultOutcome;
}
```

`context` carries the per-call child token (`with_cancellation`); the driver
selects on `context.cancellation()`. `seed` is the path's seed value with the
pre-stage's `mark_error` (false on the four paths that have no pre-stage). The
driver never returns `Err` and never panics.

On entry it checks the token once: if it is already cancelled it returns
`Interrupted { redacted: None }` without invoking anything, whether or not
handlers are mounted. Then, per handler, it:

1. Fails with `Failed { handler }` if the handler's mount is closing.
2. Clones the context with that handler's phase, `handler.cleanup` and the
   effective `is_error`, and invokes the callback inside `with_mount` and
   `catch_unwind`, in a `biased` select: cancellation first, then the mount
   close signal, then the callback.
3. Commits a valid `Ok(output)` as the accepted value and ORs `mark_error`.
4. On `Err`, panic or mount close: `Failed { handler }` if cancellation was not
   observed, otherwise `Interrupted { redacted: None }`.
5. On cancellation:
   - nothing accepted yet (the in-flight handler was running on the seed), or
     no final redactor mounted: drops the in-flight handler, of either phase,
     and returns `Interrupted { redacted: None }` at once. A sole final
     redactor in flight on the seed is this case;
   - a value was accepted and the in-flight handler is ordinary: drops it and
     runs every final redactor on the accepted value;
   - a value was accepted and the in-flight handler is a final redactor: lets
     it finish, then runs the final redactors after it.

   The final redactors run under `FINAL_REDACTION_DEADLINE`, measured from the
   instant cancellation was observed. The driver returns
   `Interrupted { redacted: Some(value) }` only if all of them return valid
   output in time; on deadline expiry it drops the one in flight and returns
   `Interrupted { redacted: None }`.

After the last handler's output is committed the driver returns `Completed`
without checking the token again; cancellation that arrives later was not
observed (settlement precedence). With no handlers and a token that is not
cancelled it returns
`Completed { result: seed.result, is_error: class.is_error() || seed.mark_error }`.

#### JSON envelope

Input to a JSON handler, and the required shape of its output:

```json
{
  "context": {
    "tool_name": "read_file",
    "resolved": true,
    "input": { "kind": "normalized", "value": { "path": "a.txt" } },
    "call_id": "<ToolCallId Display string>",
    "session_id": "<SessionId Display string>",
    "run_id": "<RunId Display string>",
    "class": "succeeded",
    "is_error": false,
    "phase": "ordinary"
  },
  "result": "<any JSON value>",
  "mark_error": false
}
```

| Field | Encoding |
| --- | --- |
| `context.input` | `{"kind":"normalized","value":V}`, `{"kind":"raw","value":V}`, or `{"kind":"unavailable","reason":R}` with `R` one of `"prepare_failed"`, `"unresolved"` |
| `context.class` | `"succeeded"`, `"execution_failed"`, `"permission_denied"`, `"unknown_tool"`, `"prepare_failed"` |
| `context.phase` | `"ordinary"`, `"final_redaction"` |
| `context.is_error` | boolean, the effective value before this handler |
| `result` | any JSON value, including `null` |
| `mark_error` | boolean; always `false` on input, required on output |

A JSON handler's reply is a D2 failure when any of these holds: the callback
returns `Err` or panics; the value is not a JSON object; `context`, `result` or
`mark_error` is missing; there is any other top-level key (a leftover
`is_error` write is one); `context` is not equal, as a `serde_json::Value`, to
the context that was sent; `mark_error` is not a JSON boolean. There is no
cancellation flag in the envelope.

```rust
pub fn result_envelope(context: &ToolResultContext, result: serde_json::Value) -> serde_json::Value;
pub fn parse_result_envelope(
    context: &ToolResultContext,
    reply: serde_json::Value,
) -> Result<TransformOutput, EnvelopeError>;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeError {
    NotAnObject,
    MissingField(&'static str),
    UnknownField,
    ContextChanged,
    MarkErrorNotBool,
}
```

#### Constants and fixed texts

| Item | Crate | Value |
| --- | --- | --- |
| `RESULT_TRANSFORM_CONTRACT_VERSION: u32` | `crabber-extension` | `2` (the untyped contract is version 1) |
| `FINAL_REDACTION_DEADLINE: Duration` | `crabber-extension` | 500ms; enforced by the driver |
| `DEFAULT_MOUNT_CLOSE_TIMEOUT: Duration` | `crabber-extension` | 5s; changed with `Registry::with_close_timeout` or `AgentBuilder::extension_close_timeout` |
| `INTERRUPT_SETTLEMENT_BOUND: Duration` | `crabber-runtime` | 1s from cancellation to durable call settlement; asserted by tests, not a runtime timer (the runtime cannot bound a store write) |
| `result_transform_failed_message(handler: &str) -> String` | `crabber-extension` | `result transform failed: <handler id>` and nothing else |
| `INTERRUPTED_RESULT_TEXT: &str` | `crabber-runtime` | `interrupted`, the text `settle_interrupted_call` writes today |
| `TOOL_PIPELINE_HANDLER_ID: &str` | `crabber-runtime` | `crabber/tool-pipeline`, the handler id of a pre-stage D2 failure |

`compute_fingerprint` keeps its signature. Before sorting it appends the
component `ComponentIdentity { id: "contract:crabber/tool/result-transform",
version: "2" }` (the constant, formatted), so both callers
(`StaticPlanProvider::new`, `Registry::acquire`) and every plan change, with or
without result-transform handlers. That is intended: the unknown-tool record
content changes for every plan. The two pinned hashes in
`fingerprint_of_fixed_components_is_pinned` (`plan.rs`) are re-pinned
deliberately in the same slice.

The handler id is the string an extension passed at registration. It is static
configuration, already a fingerprint input, and is the only non-runtime text in
a D2 message. The WASM adapter registers as `wasm-after-tool:<module name>` so
the message identifies the module.

#### Persisted forms

| Driver outcome | Call status | `is_error` | Content | Notifications |
| --- | --- | --- | --- | --- |
| `Completed`, `is_error` false | `Completed` | false | `serde_json::to_string(&result)` | `EventPublished`, `ToolSettled` |
| `Completed`, `is_error` true | `Failed` | true | `serde_json::to_string(&result)`; a structured error result is no longer stringified first | `EventPublished`, `ToolSettled` |
| `Failed { handler }` | `Failed` | true | `serde_json::to_string` of the string `result_transform_failed_message(handler)` | `EventPublished`, `ToolSettled` |
| `Interrupted { redacted: Some(v) }` | `Interrupted` | true | `serde_json::to_string(&v)` | `EventPublished`, `ToolSettled` |
| `Interrupted { redacted: None }`, and every fixed settlement | `Interrupted` | true | the bare text `interrupted` | `EventPublished` only |

The same content is the `ToolResult`, the tool message part, the
`ToolCallSettled` event and the next provider input; there is one value and it
is written once.

Seeds per path: `Succeeded` is the pre-stage output; `ExecutionFailed` is the
executor or `ToolExecute` error text as a JSON string; `PermissionDenied` is
`"permission denied"`; `UnknownTool` is `"unknown tool: <name>"`;
`PrepareFailed` is the preparation error text. Error seeds are tool- or
handler-authored and pass through the chain like any other value.

#### Pre-stage (`crabber-runtime`, `policy.rs`)

```rust
#[async_trait]
pub trait ToolPipeline: Send + Sync {
    async fn prepare(&self, tool: &ToolInfo, arguments: Value) -> Result<Value, String>;
    async fn transform_result(
        &self,
        context: &ToolResultContext,
        tool: &ToolInfo,
        result: Value,
    ) -> Result<TransformOutput, String>;
}
impl OrchestratorBuilder {
    /// Tracker handed to the pre-stage. Default: a detached tracker.
    pub fn pipeline_cleanup(self, tracker: CleanupTracker) -> Self;
}
```

The pre-stage context has class `Succeeded`, `Normalized` input, phase
`Ordinary`, the per-call child token and the pipeline tracker. `Err(text)` and
a panic are D2 failures; the text is discarded. The host that owns the
`CleanupOwner` joins it itself. The `crabber` facade does not expose
`ToolPipeline`, so this affects only hosts that build an `Orchestrator`
directly.

#### Mount close (`crabber-extension`, `crabber-runtime`, `crabber`)

```rust
// crabber-extension
pub enum ExtensionError {
    // ...existing variants...
    #[error("mount close timed out: {extension}")]
    MountCloseTimeout { extension: String },
    #[error("extension registry is closed")]
    RegistryClosed,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountCloseTimeout {
    pub extension: String, // Extension::id()
    pub bound: Duration,
    pub leases: usize,
    pub pending_tasks: usize,
}
pub type MountCloseObserver = Arc<dyn Fn(&MountCloseTimeout) + Send + Sync>;
impl Registry {
    pub fn with_close_timeout(self, bound: Duration) -> Self;
    pub fn with_close_observer(self, observer: MountCloseObserver) -> Self;
    /// `Err(ExtensionError::RegistryClosed)` once `close_all` has started.
    pub fn try_acquire(&self, session: &SessionId) -> Result<RunPlan, ExtensionError>;
}

// crabber-runtime
pub trait Observer: Send + Sync {
    // ...existing methods...
    fn mount_close_timed_out(&self, _timeout: &crabber_extension::MountCloseTimeout) {}
}

// crabber
impl AgentBuilder {
    pub fn extension_close_timeout(self, bound: Duration) -> Self;
}
impl Agent {
    pub async fn close_extensions(&self) -> Result<(), ExtensionError>;
}
```

`MountHandle::close` and `Registry::close_all` keep their signatures. Both
`Registry` settings are shared by clones and are set before the first mount.
A later `close` on a mount that timed out waits again under the bound and
returns `Ok` once the reaper has finished.

**`close_all` is terminal, and the terminal state comes first.**

1. Under the registry mutex that `acquire` takes, in one critical section,
   `close_all` sets a `closed` flag in `RegistryInner` and deactivates every
   mount. No plan can be acquired between the flag and any deactivation, so
   no run ever gets a plan that contains some mounts but not a closed one.
2. It sends the close signal to every mount.
3. It joins each mount in reverse mount order, each under its own bound, and
   returns the first error after attempting all of them (today it stops at the
   first).

From step 1 on, for every clone of the registry, `try_acquire` and
`RunPlanProvider::acquire_plan` return `Err(ExtensionError::RegistryClosed)`
and `mount` returns the same error. The runtime already maps an
`acquire_plan` error to `RuntimeError::Extension` on `prompt`, `resume` and
`recover` (`orchestrator.rs:923-927`, `:1588-1592`). `Registry::acquire` keeps
its signature and panics on a closed registry; it is `try_acquire` unwrapped.
The registry stays terminal when `close_all` returns `MountCloseTimeout`, and
there is no reopen.

For a host that calls `Registry::close_all` directly this means: the registry
is unusable afterwards and every later run attempt through an `Orchestrator`
using it fails with `RuntimeError::Extension`; build a new registry to
continue. Closing one mount with `MountHandle::close` is **not** terminal: new
plans simply omit that mount, so a host must not close a redactor's mount that
way while it still admits runs.

`Agent::close_extensions` calls `close_all` and adds no flag of its own. An
agent built without extensions has no registry: the call returns `Ok(())` and
the agent keeps working, since there is nothing to lose.

### Recorded answers

**1. Unknown-tool arguments in the durable record.** `stage_tool` writes
`{"$crabber_unknown_tool": {"raw": <value>}}` for an unknown tool, where
`<value>` is the provider arguments when they parsed and otherwise the raw
provider text as a JSON string. `$crabber_prepare_error` (a string message) is
written for preparation failures only. `execute_tool` derives class and input
from the record alone:

| `record.arguments` | Tool in plan | Class | Input |
| --- | --- | --- | --- |
| object whose only key is `$crabber_unknown_tool`, holding an object with `raw` | either | `UnknownTool` | `Raw(raw)` |
| object whose only key is `$crabber_unknown_tool`, any other shape | either | `UnknownTool` | `Unavailable { Unresolved }` |
| object whose only key is `$crabber_prepare_error` | either | `PrepareFailed` | `Unavailable { PrepareFailed }` |
| object whose only key is any other key starting with `$crabber_` | either | `PrepareFailed` | `Unavailable { PrepareFailed }` |
| anything else | yes | from execution | `Normalized(arguments)` |
| anything else | no (the defensive `(None, Ok(_))` arm) | `UnknownTool`, `resolved` false, seed `"unknown tool: <name>"` | `Unavailable { Unresolved }` |

The chain's seed text for the two reserved-key rows (a sole other
`$crabber_*` key, or a non-string `$crabber_prepare_error`) is the fixed string
`reserved argument key`, the same text `stage_tool` records.

- *Schema.* This is a record-content change with **no schema change and no
  migration**: `ToolCallRecord.arguments` is an opaque JSON value and the
  schema version stays 5.
- *Reason.* The class must come from a key check, not from matching the
  `"unknown tool: "` text, which a preparation error can also produce; and a
  re-executed unknown call on a paused run has no other source for `Raw`
  (`PendingCall` is rebuilt from the record).
- *Reserved keys.* The model controls a resolvable tool's arguments, so
  prepared arguments could themselves be `{"$crabber_unknown_tool": …}` and be
  read back as an unknown tool. `stage_tool` closes this: when the prepared
  arguments of a resolved tool are an object whose only key starts with
  `$crabber_`, it records a preparation failure
  (`$crabber_prepare_error` with the fixed text `reserved argument key`) and
  the tool is not run. A record staged under this contract therefore carries a
  sole `$crabber_*` key only when the runtime wrote it. The same hole exists
  today for `$crabber_prepare_error` (`orchestrator.rs:3123-3128`) and closes
  with it.
- *Nesting depth.* Stores decode records with `serde_json::from_str`, which
  stops at 128 levels, and the sentinel adds levels. When the provider
  arguments cannot be stored inside the sentinel in decodable form, `raw` holds
  their JSON text as a string, the same representation as unparseable provider
  text. Consumers must treat `Raw` as unvalidated.
- *Persistence exposure.* Raw provider arguments for unknown tools become
  durable in the tool-call record. They are unvalidated and unnormalized. The
  assistant message already persists exactly the same value in its `ToolCall`
  block, parsed arguments as JSON and unparseable ones as a JSON string of the
  raw text (`orchestrator.rs:3017-3023`). The record therefore adds a second
  copy in both cases, not a new class of data. Hosts that treat tool-call
  records differently from messages must account for it, and bounded-snapshot
  sizes for such calls grow by the argument size.
- *Defensive arm.* The stored arguments there are normalized for a tool that no
  longer resolves. They are exposed neither as `Raw` (they are not provider
  arguments) nor as `Normalized` (a binding must never match an unresolved
  tool).
- *Old records.* There is no shim. A record staged before this change with
  `$crabber_prepare_error: "unknown tool: …"` classifies as `PrepareFailed`;
  the fingerprint version already stops such runs from resuming.
- *Consequences.* `crabber-zv2d` changes `stage_tool` and the record reader
  together and replaces the `Result<Value, String>` outcome at
  `orchestrator.rs:3171` with a private typed outcome carrying the class.
  Consumers must not bind on `Raw` or `Unavailable` input.

**2. WASM.** No WIT change. `after-tool-call` keeps its signature and the
package stays `crabber:extensions@0.1.0`; none of the five WIT copies changes
(root `wit/`; the SDK copies `crates/crabber-guest/wit/` and
`crates/crabber-guest-macros/wit/` that `check_wit` compares; and the vendored
`fixtures/wasm/src/tool-and-sink/wit/` and
`fixtures/wasm-negative/src/missing-manifest/wit/`, which `check_wit` does not
cover and which would have to be edited by hand if the WIT ever changed). The
adapter registers through `on_result_transform` and fills the existing
arguments:

| WIT argument | Value |
| --- | --- |
| `tool-name` | `context.tool_name` |
| `tool-call-id` | `context.call_id` |
| `executed-input-json` | JSON text of the tagged `context.input` object |
| `output-json` | JSON text of the whole envelope (`context`, `result`, `mark_error`) |
| `is-error` | `context.is_error` |
| `turn` | unchanged (empty `turn-metadata`) |

Reply: `unchanged` keeps `result` with `mark_error` false; `json(string)` must
parse to a full envelope and passes `parse_result_envelope`; `error(_)`, an
unparseable string, any other variant, a trap and `WasmError::Size` are D2
failures, and guest-authored error text is never persisted.

- *Reason.* D9 limits the WASM change to the adapter and guest fixture
  sources; the envelope already carries class, input tag and `mark_error`.
- *Interruption.* Token-driven epoch interruption, no opt-out. A new method
  carries the token; `LoadedModule::call` keeps its signature and behaviour
  and is the same call with a token that is never cancelled:

  ```rust
  impl LoadedModule {
      pub async fn call_cancellable(
          &self,
          interface: &str,
          function: &str,
          args: &[Val],
          cancellation: &CancellationToken,
      ) -> Result<Val, WasmError>;
  }
  ```

  The adapter calls it with `context.cancellation()`. The token is captured
  by the per-store `epoch_deadline_callback`
  (`crates/crabber-wasm/src/lib.rs:700`), which also traps when the token is
  cancelled, within one 10ms epoch tick plus the time to reach a safepoint.
  The driver additionally drops the call future, which also releases a caller
  waiting on the module's `serial` mutex and a guest blocked inside a host
  import.
- *Exclusion.* WASM handlers are always phase `Ordinary`; a WASM guest cannot
  be a final redactor in this change. Like every ordinary handler it is
  skipped once cancellation is observed, so **a WASM redactor cannot provide
  the protection guarantee**: with a native or JSON final redactor mounted,
  the accepted value is persisted after cancellation without the WASM guest
  having run on it, and with none mounted nothing tool-authored is persisted.
  A host that needs redaction to hold under cancellation mounts a native or
  JSON final redactor.
- *Consequences.* This is an ABI break without a WIT diff: a guest built for
  the old contract returns a bare result and every call it handles settles
  `Failed`. `all-in-one` and `redact-middleware` are rewritten to parse and
  return the envelope. The input appears in two arguments, so large inputs
  reach `max_input_bytes` sooner. The test at
  `crates/crabber-wasm/src/lib.rs:1334` moves to the driver. `crabber-rmjd`
  moves the adapter to `on_result_transform` using `call`; `crabber-nzy3` adds
  `call_cancellable` and switches the adapter to it.

**3. D7 ownership and reachability.**
- *Observation.* `Registry` takes an optional `MountCloseObserver`
  (`with_close_observer`), called once per timed-out `close` with a
  `MountCloseTimeout`. `crabber-runtime` adds the defaulted
  `Observer::mount_close_timed_out`. The facade installs a registry observer
  that forwards to every observer registered with `AgentBuilder::observer`. It
  is a local callback, not a durable event; `EventKind::ExtensionNotice` and
  `OperationalObservation` are not used because a registry-level close has no
  session or run.
- *Reaper.* `crabber-extension` owns it: the mount's detached close task. It
  only awaits plan leases and `TaskTracker::wait`, so the crate needs neither
  `crabber-runtime` nor tokio's `process` feature; children belong to the
  extension's own cleanup tasks. It lives until those tasks finish or the Tokio
  runtime shuts down, where `kill_on_drop` is the backstop.
- *Reaching close.* `Agent::close_extensions` calls `Registry::close_all` and
  returns `Ok(())` when the agent has no registry. It is terminal from the
  moment it starts, not from when it returns: `close_all` closes the registry
  before deactivating any mount (Mount close, above), so a `prompt`, `resume`
  or `recover` that races with it fails with `RuntimeError::Extension` instead
  of getting a plan without the redactor, and it stays terminal when the call
  returns `MountCloseTimeout`. The name avoids the existing `datadog`-only
  `Agent::shutdown`. It does not interrupt runs; the host interrupts them
  first, and a run still holding a plan lease counts against the bound.
- *Consequences.* The registry half is `crabber-b4zy`; the `Observer` method
  and the facade are `crabber-eig3`, which is ordered after it and owns
  `orchestrator.rs` at that point. Required tests: in `crabber-b4zy`, with two
  mounts and a lease held on the first, a `close_all` in progress makes a
  concurrent `acquire_plan` return `Err(RegistryClosed)`, and it still does
  after `close_all` returned `MountCloseTimeout`; in `crabber-eig3`, through
  the facade, `prompt` during an in-progress `close_extensions` returns
  `RuntimeError::Extension`. The D8 test calls `close_extensions` after the
  interrupt, and a second test forces the timeout and asserts the error, the
  observer call, the killed child, and the permit released only after reaping.

**4. D6 persistence of an accepted, redacted value.**
- *Path.* `execute_tool` settles it through its ordinary settlement code with
  status `ToolResultStatus::Interrupted`, `is_error` true and the redacted
  value as content. `settle_interrupted_call` is not changed and keeps writing
  only the fixed text for live interruption, resume and recovery.
- *Double settlement.* The store rejects a second settle with
  `StoreError::Conflict`, so each call is settled exactly once.
  `Interrupted { redacted: Some(_) }`: `execute_tool` checks the lease, settles,
  then returns `Err(RuntimeError::Interrupted)`. `Interrupted { redacted: None }`:
  it returns `Err(RuntimeError::Interrupted)` without settling. The run's error
  arm then calls `settle_unfinished_calls`, which lists only unfinished calls,
  skips the one already settled and writes the fixed text for the rest.
- *Parallel ordered settlement.* An `Interrupted` outcome never waits on the
  ordered-settlement watch and never advances it; a payload-carrying one
  settles immediately, out of index order. The returned error fails
  `try_collect`, which drops the sibling futures, and `settle_unfinished_calls`
  settles whatever is still unfinished. No path waits on an index that will
  not be sent, so siblings cannot deadlock. Consequence: in `Parallel` mode a
  sibling's accepted value can be dropped mid-redaction and replaced by the
  fixed text. That is the permitted direction, and "the redacted value is
  persisted" is deterministic only in `Sequential` mode or for a single call.
  Tests for that case use those; the `Parallel` test asserts no deadlock, no
  `Conflict`, every call terminal within the bound and nothing unredacted.
- *`ToolSettled`.* Notified for the payload-carrying settlement, as for every
  settlement `execute_tool` performs. Not notified for the fixed settlement,
  as today; that exclusion is documented, not changed.
- *`is_error`.* Always true for status `Interrupted`, with or without payload,
  whatever the class and `mark_error`.
- *Lease loss.* On a lost lease nothing is settled by this process; D7
  tracking is the only protection for children there.

**5. `unresolved_work_stays_registered_until_noncooperative_transform_finishes`
(`examples/agui-sse/tests/http.rs:490`).** Its premise is void: under D5 the
interrupt drops the non-cooperative transform, so the worker resolves. It is
replaced by `noncooperative_transform_is_dropped_on_interrupt`, with the same
`HeldResult` handler moved to `on_result_transform` and never released. It
asserts that after the response is dropped the worker resolves within
`INTERRUPT_SETTLEMENT_BOUND`, `unresolved` stays 0, `host.shutdown()` returns
`Ok`, and the call is durably `Interrupted` with the text `interrupted`. The
"unresolved work stays registered" property keeps its coverage in
`held_admission_times_out_or_shuts_down_without_losing_late_handle`
(`http.rs:682`), which does not use a result transform.

**6. External probe (D11).** A new standalone crate at
`testdata/result-transform-probe/`, with its own `[workspace]` table and
lockfile. It is separate from `testdata/external-consumer/`, which uses path
dependencies and is run by `cargo xtask check`; the new probe is **not** run by
`cargo xtask check`, because it needs the network and a published revision. It
depends on `crabber` by git URL and a full 40-hex `rev`, uses no WASM, and
passes with `cargo test` and no environment variables. The `rev` is the last
implementation commit of the feature branch (the commit before the one that
sets the pin). Any later commit, including a review fix, makes the pin stale,
so the pin is re-set as the final commit of the branch immediately before
merge, and that commit changes nothing but the `rev` and the probe's lockfile.
The pull request must be merged with a merge commit, not squashed or rebased,
so that revision stays reachable from `main`. If a squash merge is ever
needed, a tag on the pinned revision keeps it fetchable and is pushed first
(a human gate). The reply
on the request reports both the `main` merge commit and the probe's `rev`.
`crabber-8sxd` runs the probe from a clean clone after publication.

**Resume and recover cancellation.** Nothing changes. `resume` and `recover`
have no caller-visible cancellation source; their token is cancelled only on
lease loss, and that path drops the work without settling. Re-executed calls
still get a child of that token and the full chain. The D5 bound is asserted
through live runs only; resume and recovery are a documented exclusion.

**Measurement reason.** Every cancellation path of a tool call reports
`TerminalReason::Cancelled`, set explicitly before the measurement is dropped
(`Measurement::drop` already forces it when the token is cancelled). A lost
lease reports `LeaseLost`, which takes precedence. A D2 failure reports
`ToolError`.

### Implementation order and file ownership

No slice is added. Work that no slice named is assigned below and marked
*(assigned here)*. The table follows the `blocked_by` chain: each slice
compiles and keeps the workspace tests passing using only items owned by
itself or by a slice it is ordered after, and no file is touched by two
slices that are not ordered by a dependency. Files a slice touches beyond its
own `Reserves` line are marked *(extra file)*.

Order: `cv10` → `gl4i` → `f7iv` → {`slgt` → `ctj9`, `2xrl`}; `b4zy` after
`2xrl` and `ctj9`; `zv2d` after `cv10` and `5f1n`; `klyf` after `zv2d`; `orgu`
after `klyf`, `ctj9`, `2xrl` and `0bks`; `v1we` after `orgu`; `eig3` after
`v1we` and `b4zy`; `rmjd` after `orgu`; `nzy3` after `rmjd` and `v1we`;
`ufiw` and `o8ai` after `v1we`.

| Slice | Files | Owns |
| --- | --- | --- |
| `crabber-cv10` | `crabber-extension/src/result_transform.rs`, `lib.rs` | `ToolInput`, `InputUnavailable`, `ToolOutcomeClass`, `TransformPhase`, `TransformOutput`, `ToolResultOutcome`, `ResultTransformCallback`, `ToolResultContext` with its crate-private phase, `is_error` and tracker setters; `CleanupTracker` including `detached()` and its crate-private constructor; the envelope functions, `EnvelopeError`, `json_result_transform`; `result_transform_failed_message`; only the `crabber-extension` constants: `RESULT_TRANSFORM_CONTRACT_VERSION`, `FINAL_REDACTION_DEADLINE`, `DEFAULT_MOUNT_CLOSE_TIMEOUT` |
| `crabber-gl4i` | `crabber-extension/src/plan.rs`; one test in `crabber-runtime/src/tests.rs` | Contract component in `compute_fingerprint`; re-pinning the two hashes |
| `crabber-f7iv` | `crabber-extension/src/registry.rs`, `dispatch.rs` | `Handler.phase` and `Handler.cleanup` (default detached), `HandlerFn::ResultTransform`, the phase-first sort, `on_result_transform`, `on_final_redaction`, `on_final_redaction_json`; *(assigned here)* the phase in the handler component version. `on_transform` is not changed |
| `crabber-slgt` | `crabber-extension/src/dispatch.rs`, `result_transform.rs` | `Dispatcher::transform_tool_result` without cancellation: order, D2 containment, D4 rules, wrapping `on_transform` callbacks with `json_result_transform`, and passing `handler.cleanup`, the phase and the effective `is_error` into each handler's context |
| `crabber-ctj9` | same two files | Entry check, cancellation, acceptance, the final-redaction phase and deadline in the driver |
| `crabber-2xrl` | `crabber-extension/src/registry.rs`; `lib.rs` *(extra file, one re-export line)* | `CleanupOwner` and `CleanupJoinTimeout` (in `registry.rs`), the mount's owner, stamping `Handler.cleanup` in `Registry::mount`. It does not touch `dispatch.rs` or `result_transform.rs`, which `slgt` and `ctj9` hold concurrently |
| `crabber-b4zy` | `crabber-extension/src/registry.rs`, `plan.rs`; `dispatch.rs` and `lib.rs` *(extra files)* | Bounded close under one bound, `MountCloseTimeout`, `MountCloseObserver`, both new `ExtensionError` variants, the mount-closing checks in the driver (steps 1, 2 and 4); *(assigned here)* `Registry::with_close_timeout`, `with_close_observer`, `try_acquire`, the terminal `closed` flag and the three-step `close_all`, with the registry test of answer 3. Nothing in `crabber-runtime` or `crabber` |
| `crabber-zv2d` | `crabber-runtime/src/orchestrator.rs` | Context construction on all five paths, the private typed outcome; *(assigned here)* the `$crabber_unknown_tool` record content, the reserved-key rule in `stage_tool` and the record-reading table. Needs only `cv10` items |
| `crabber-klyf` | `crabber-runtime/src/policy.rs`, `orchestrator.rs`, `tests.rs`, `lib.rs` | New `ToolPipeline::transform_result`, pre-stage D2, `TOOL_PIPELINE_HANDLER_ID`; *(assigned here)* `OrchestratorBuilder::pipeline_cleanup` |
| `crabber-orgu` | `crabber-runtime/src/orchestrator.rs`, `extension_tests.rs`, `result_transform_tests/mod.rs` | `execute_tool` on the driver, persisted forms for `Completed` and `Failed`. In-tree `on_transform` consumers keep working unchanged through the driver's wrapping, except the `extension_tests.rs` handler that writes `is_error` |
| `crabber-v1we` | `crabber-runtime/src/orchestrator.rs`; `lib.rs` *(extra file, re-exports)* | Per-call child token, `biased` select, both `Interrupted` settlements, the Parallel rule, measurement reason, `INTERRUPT_SETTLEMENT_BOUND`, `INTERRUPTED_RESULT_TEXT` |
| `crabber-eig3` | `crabber-runtime/src/orchestrator.rs`; `crates/crabber/src/agent.rs` *(extra file)* | The D10 split as implemented and documented in code; *(assigned here)* `Observer::mount_close_timed_out` (the trait is in `orchestrator.rs`), `AgentBuilder::extension_close_timeout`, `Agent::close_extensions`, the facade's registry observer, and the facade test of answer 3 |
| `crabber-rmjd` | `crabber-wasm/src/adapters.rs`, `lib.rs` tests, guest fixture sources | Adapter on `on_result_transform` with the argument table of answer 2, envelope reply handling, guest fixtures; *(assigned here)* the `wasm-after-tool:<module name>` handler id. No WIT file changes |
| `crabber-nzy3` | `crabber-wasm/src/lib.rs`, `adapters.rs` | `LoadedModule::call_cancellable`, the token in the epoch callback, the ordinary-only exclusion |
| `crabber-ufiw` | `examples/native-extension/**` | The example redactor on `on_final_redaction`; README |
| `crabber-o8ai` | `examples/agui-sse/**` | The `check.rs` redactor on `on_final_redaction`; the replacement test of answer 5 |
| `crabber-uvzv`, `crabber-wzlh`, `crabber-mfil` | as reserved | D8 child and reducer, the real interruption and close tests, the PostgreSQL durable check |
| `crabber-mlgw`, `crabber-8sxd` | `testdata/result-transform-probe/` | The probe of answer 6 and its verification; the final pin commit |
| `crabber-exbv` | `docs/embedding.md` | Filling the `Covered by` column |
| `crabber-mx0r` | `docs/embedding.md`, `README.md`, READMEs | The user-facing guide below |
| `crabber-8q7h` | files of earlier slices | Removing the old contract; *(assigned here)* making generic `transform` and `transform_pinned` reject this point, last, once no in-tree caller is left |

#### Slice text overridden by this section

- `crabber-cv10`: its text puts the interrupt bound in `crabber-extension`.
  `INTERRUPT_SETTLEMENT_BOUND` is a `crabber-runtime` constant owned by
  `crabber-v1we`; `cv10` defines only the three `crabber-extension` constants.
- `crabber-f7iv`: its text allows changing JSON registration for this point.
  `on_transform` is left alone; the driver wraps its callbacks.
- `crabber-2xrl`: its text has the driver resolve a `mount_id` to a tracker
  and keeps an in-flight count and token on the mount. The tracker is stamped
  on the handler instead, and the close signal on that tracker is what the
  driver observes; no separate count or token is needed.
- `crabber-b4zy`: its text joins the tracker under the bound and then waits
  for leases without one. Leases and the tracker share **one** bound, so
  `close` can now return `MountCloseTimeout` after 5s while a long run still
  holds a plan lease; the reaper finishes the close when the run ends. Its
  text also does not mention the terminal registry state, which it owns.
- `crabber-eig3`: its text reserves `observation.rs` for the D7 observation
  and expects a runtime-owned reaper. The `Observer` trait is in
  `orchestrator.rs`, `observation.rs` is not touched, there is no runtime
  reaper, and the slice additionally owns the facade methods in
  `crates/crabber/src/agent.rs`.
- `crabber-o8ai`: its text wants the replacement test to still prove that
  unresolved work stays registered. Answer 5 drops that requirement for this
  test, because the premise no longer exists; the property stays covered by
  the held-admission test.
- `crabber-ufiw`, `crabber-o8ai`: the example redactors move to
  `on_final_redaction`, not merely to the typed ordinary registration.
- `crabber-mlgw`: the pin it writes is provisional; it is re-set as the final
  commit before merge (answer 6).

### User-facing guide

Reserved for `crabber-mx0r`: usage examples, the outcome-path matrix written
for extension authors, and the adoption and rollback procedure. Until then the
contract above is the reference. In short: adopting means rewriting every
`ToolResultTransform` handler and every `ToolPipeline` implementation against
the API above and rebuilding WASM guests; runs frozen before the upgrade do not
resume (`RuntimeError::PlanChanged`); rolling back means reverting the
revision, with no data migration in either direction. Unfinished runs must be
settled before either move, or abandoned afterwards through
[fenced abandonment](fenced-abandon.md).
