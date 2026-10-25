# Configured server-host contracts

The durable-host API is under implementation and is unstable until the public
consumer and dependency tuple are qualified. The currently implemented context
entry points preserve one configured Agent's registrations, store, policy and
prompt settings while selecting an admitted workspace/session and model for each
request. The server owns root authorization and allowed model selection.

`SessionContext` contains immutable session, workspace and directory identity.
`ExecutionContext` combines it with an explicit `Selection`. Identity comparisons
use exact strings: no path normalization, filesystem lookup or process `chdir`.
Empty persisted workspace fields remain empty and are exposed as unavailable by
native tool workspace context; agent defaults never replace them.

Call `Agent::ready` to initialize shared registrations once. A failed partial
mount rolls back the mounts already installed in that initialization attempt.
`prompt_keyed_in` can atomically create a new host-selected session ID with its
first admission. `prompt_in` is the unkeyed entry for an existing session. Both
accept an optional host trace context separately from execution routing.

Retain the original execution context, input and `AdmissionOptions` after an
unknown response. `recover_admission_in` verifies those original semantics before
claiming a retained unstarted admission. An identical terminal retry replays a
receipt and grants no new executor. A changed root fails before extension setup;
the store repeats its identity check inside admission to protect concurrent
creation. A changed selected model under the same key conflicts even when a
caller reuses a stale claimed fingerprint. See [admission receipts](admission-receipts.md)
for canonical hashing, unknown-outcome reconciliation and behavior versioning.

`Scope::Workspace` and `Scope::SessionIn` select shared registrations using the
context supplied to plan acquisition. Scope filtering does not create independent
mutable state. Plan fingerprints use only the ordering of applicable mounts, so
an unrelated session's mount cannot break recovery. Handler identity includes its
owning extension, so changing the order of relevant handlers still changes the
fingerprint. This changes fingerprints for plans containing handlers or legacy
prompt contributors. Finish or settle old paused work with its matching old
binary before upgrading; it must not execute under an incompatible plan.

`Agent::close(bound)` terminally closes fresh local intake, requests cancellation,
and observes the retained execution, preparation, resource, detached invocation,
service and contextual cleanup owners under one caller deadline. Keep the Agent
available after a deadline or dropped waiter: another close observes the same
original owners. Concurrent callers share an in-progress observation. A later
caller can extend observation after an earlier bounded pass has actually joined;
close never restarts business work or recreates a native completion operation.

Inspect `HostCloseReport::local_drained()` and the report's runtime and extension
fields. `CloseObservation::Complete` requires every covered local owner to have
joined successfully. `DeadlineExpired`, `Pending`, `SelfClose` and
`UnrepresentableDeadline` preserve ownership and do not establish clean shutdown.
Zero duration still closes intake and signals work. An unrepresentable duration
also closes intake and returns a finite observation. Closing from the actual
owned execution or extension callback returns `SelfClose` without awaiting its
own join; an external caller can observe it after the callback returns. A naked
unregistered task that waits on close can still create an unobservable cycle and
will remain pending under the caller deadline.

Fresh readiness and execution requests return `RuntimeError::HostClosed` once
closing starts. An exact committed keyed retry can still return its original
metadata receipt. This exception grants no new factory, binding or executor.
Pre-effect reads and contextual initialization on an admitted execution stay in
the retained root even when its original caller disconnects.

`close_extensions` stops fresh intake after its contextual self-close preflight
succeeds, including for an Agent with no extensions, and closes extension
resources and leases. A rejected `SelfClose` preserves intake. It preserves its existing
non-cancelling behavior for active executions. Use `close` to signal and observe
all the covered local Runtime registries. Neither operation certifies a durable
terminal outcome, ownership on another controller, native policy retirement,
Session deletion readiness, or shutdown of unregistered external resources.
Exporter `shutdown` remains specific to the configured telemetry exporter.

Runtime initial preparation/admission, retained-Unstarted recovery preparation/
claim, execution workers and polled `resume` calls retain their actual task join
handles. Once a runtime entry is polled, dropping its waiter detaches the result
subscription and leaves its work owned, including a late committed admission. `RunHandle::done`
awaits the worker's join as well as its result. `drain_execution_tasks(bound)` on
Agent or Orchestrator joins a snapshot of retained workers and reports remaining
owners after the deadline; timed-out joins remain available to a later drain.
Call drain periodically if dropping handles, and stop host ingress before a final
drain. Inspect both `pending` (admitted run IDs) and `pending_preparations`
(local `PreparationTaskId`s). A preparation can create a worker after the drain's
initial snapshot; drain again while either list has entries. The pending owner
snapshot observes both categories under one lock. `joined_preparations` reports
actual preparation task joins separately from execution joins. Session-scoped retained preparations, workers and tool resources have exact
session identity, including ordinary admission before a run exists. Readiness
and pre-routing roots retain an unscoped local task ID until routing is known;
`joined_unscoped_preparations` reports their actual joins without inventing a
session identity. `drain_session_execution_tasks(session_id,
bound)` joins only that session's local owners and reports its remaining work;
it does not join or cancel another session. Stop the selected session's ingress
first and repeat the drain for preparation-to-worker handoffs. This does not
certify ownership on another controller, release session services, or establish
durable deletion readiness. The initial read that routes a resumed run has no
side effects; its subsequent validation, plan preparation and claim stay owned.
These local IDs
are neither run identities nor durable authority. Multiple pending entries for one run represent
distinct retained local tasks. Dropping the configured host does not request
cancellation; retain it and drain to establish joins. Runtime shutdown may still
stop tasks.

Initial admission and `resume_execution` return the same `RunHandle` interface.
Each worker gets its own bounded, sole event receiver, including separate workers
with the same run ID. Dropping observation does not cancel work. Lag ends that
observation path; reconcile durable state instead of treating a later partial
stream as complete. `control()` clones local interrupt/completion access without
exposing a fence or transferring receiver ownership. An interrupt is a local
signal, not a durable cancellation acknowledgement. `resume()` remains a convenience
that awaits the resumed handle. Eligibility and binding errors can arrive through
its result after preparation returns a handle.

Fresh ordinary and keyed workers whose plans retain an owned preparation lease also register
an independent native `crabber.runtime.outer-worker.v1` owner. The Store must
support execution drivers. Direct ordinary and keyed admission check that capability before
preparation factory acquisition. Registration and activation replay return metadata only. Only a fresh
activation opens the exact registered worker gate. The retained supervisor observes
the actual outer worker join, including final measurements, observers, completion
publication and drops, then separately joins registered cleanup before retiring the
native driver. Dropping a handle preserves the original pending native call.
Cleanup signals every captured scope before awaiting any one join. A worker panic
remains visible even if its supervisor returns normally. Pending control I/O can
delay retirement; bounded drains continue to report that supervisor as pending.

The Runtime bridge covers initial ordinary and keyed admission. Native adapters support
an initial unkeyed native driver from the complete frozen binding anchored in the
original preparation Join. Fresh unkeyed registration requires the original admission
revision; activation requires the exact original driver Begin revision. Any intervening
publication conservatively refuses fresh dispatch. Original metadata retries require
no current binding, keyed receipt, or fabricated journal. Recovered workers still require
their own authenticated driver paths. The driver does not
certify provider, shared-service, callback or whole-run resource coverage. A failed
cleanup leaves native retirement unresolved, and retained resource failures remain
visible through drains. Durable deletion readiness remains conservative.

These joins do not prove durable terminal outcome, callback/child-process cleanup
or notification-task completion. Terminal-write errors, including lost acknowledgements
after commit, return `RuntimeError::AttemptBoundaryUnknown` without a second terminal
write. Reconcile the durable run and events; live terminal events require an
acknowledged commit. Value validation and exact post-close metadata replay
remain available without admitting execution work. Initial workers, expired Unstarted recovery and
eligible suspended continuation now reserve/reclaim and one-shot Begin a native
attempt before business work. Native tool calls claim and begin their own phases
before dispatch and atomically settle protected output after their resources join.
Durable run cancellation records acceptance separately from worker observation
and actual resource joins. Orderly close reports the covered local joins;
positive native coverage for every resource class remains unfinished.
Result/completion signals and event EOF never replace these checks.

The live `ApprovalFacade::request` and `UserPrompter::ask` APIs have been removed.
Ordinary `ToolContext::request_interaction` returns the finite
`DurableInteractionUnsupported` error synchronously. It invokes no host callback
and does not create an awaiting question. An explicit `ResumableTool` can instead
return a bounded typed checkpoint and declared questions. Native suspension commits
the frozen batch and pause together; host answers and a fresh continuation claim
resume that exact workflow without capturing a live callback. The runtime's
durable pre-effect approval gate persists the complete prepared batch before
dispatch; host answers and continuation claims use separate atomic controls.
`terminate_approval` can cancel or expire a fully joined waiting batch without
initializing execution resources. Frozen
plans include interaction contract version 2 so older hook-bearing plans cannot
silently resume with these capabilities removed.

Native tools can register task-owned resources with `ToolContext::resources()`.
A call accepts at most 64 tasks. Registration is sealed when its tool-call future
returns, is cancelled or panics; retained context clones then receive a typed
registration error. The cooperative closing future does not abort work. Include
all resource cleanup in the registered future and return `ResourceFailure` if
cleanup cannot be established. A standalone context without an owner rejects
registration instead of detaching work.

The runtime retains the actual task joins independently of the worker result.
`drain_execution_tasks` also reports `resources`, keyed by run and call: inspect
`sealed`, `pending` and `failures`. An active scope stays open during a snapshot
drain; a sealed scope resumes its retained joins under the remaining deadline.
A timeout preserves ownership, and failures remain visible across repeated drains.
Before committing a tool result or run boundary, the worker closes and joins its
registered resources under a one-second bound. Pending joins or failures stop
continuation with `AttemptCleanupIncomplete`, retain ownership and leave the native
attempt uncompleted. A later drain does not automatically resume execution or
manufacture a successful result. These are local task observations; durable phase quiescence,
external-effect reconciliation and explicit session-owned job transfer must be
authenticated separately. They must not be inferred from this report. Execution
fingerprints include resource contract version 3 and its
capacity. Native attempt driver version 1 and its join bound are also fingerprinted;
old plans fail closed and require their matching binary to finish. Custom stores
must explicitly declare `supports_owned_attempts()` and implement its full contract;
ordinary admission rejects unsupported adapters before accepting work.

Register shareable state explicitly with `shared_tool`, `shared_extension`,
`shared_wasm_extension`, or `shared_host_services`. These replace the old builder
names without compatibility aliases. A scope filters visibility of the same
instance. Its state must tolerate concurrent use across admitted contexts.

`RunPlanProvider::registration_fingerprint` inspects frozen declarations without
creating contextual owners, initializing components or acquiring a plan lease.
Static, live Registry and contextual providers implement it; custom providers explicitly implement
this capability. Contextual providers capture shared metadata and factory declarations
at configuration time, including failures needed for truthful readiness rollback.
Extension `config_hash` and factory configuration/version must identify all
registration-affecting inputs, including schemas and prompt data. Metadata replay
cannot discover changes hidden behind falsely unchanged declarations without
initializing native code. This declaration fingerprint does not replace checking
actual restored instances before execution. The built-in WASM wrapper binds the
mandatory module digest, exact nonsecret guest configuration, instance mode and
runtime limits without loading the module. Artifact relocation with the same
verified bytes does not change its declaration; preparation still validates paths.

`extension_factory` creates a fresh extension, tool and service graph per
`FactoryScope::Workspace` or `FactoryScope::Session` owner. The factory receives
immutable workspace identity and an owned cleanup tracker; only session factories
receive a session ID. Factories must create independent mutable state. Native code
that returns shared state violates that contract; the library cannot inspect Arc
aliasing. Creation/install configure resources and must not execute tool or
provider business effects. Identity/configuration metadata must describe behavior
without including credentials. Instance metadata is frozen before publication.

Initialization is coalesced per owner and retained when a waiter drops or reaches
`initialization_timeout`. No incomplete waiter receives a plan. Failed partial
setup rolls back registrations and shuts down installed instances before retry.
Registrar host services override supplied capabilities; absent capabilities inherit
the shared services. Service behavior belongs in component configuration identity.

`close_session_resources` closes only that session's factory resources;
`close_workspace_resources` also closes its session resources. They never delete
files or cancel active executions. Closure rejects new plans for that owner.
Cleanup that exceeds its bound returns a typed incomplete/timeout error and stays
owned; a later close can establish completion. Other owners remain available.

The common owned initial/resumed execution lifecycle, typed admission,
authoritative controls and session management are implemented parts of this
contract. Public-consumer and dependency-tuple qualification remains separate
from these local implementation facts.

Experimental `Store::admit_bound_run` and `admit_bound_keyed_run` atomically retain
an `ExecutionBindingCapsule` with admission in all bundled stores. They require
provider-binding and host-behavior components in the independently hashed runtime
configuration. Keyed admission additionally verifies its original execution
capsule; exact replay checks the retained binding before granting any new work.
A legacy receipt with missing binding evidence is never patched during replay.

`execution_binding(session, run)` reads scoped private evidence, including the
original input. Its Debug output is redacted; it is not a public snapshot or an
execution capability. New runtime admissions retain binding capsule version 3,
keyed execution capsule version 2 and canonical input digest version 2. The
binding codec is bounded to 16 MiB encoded JSON and 64 levels of configuration
nesting. Historically, PostgreSQL schema 7 and fresh SQLite schema 3 expanded
the private codec carriers. Current storage requires PostgreSQL 25 / SQLite 20;
retained older payloads are not rewritten or made executable. Custom
adapters default to `HostUnsupported` and must implement the atomic capability.
The runtime uses these atomic APIs for both ordinary and keyed admission. It
restores the retained backend/configuration before recovery claims, holds the
same implementation across turns and compaction, and uses captured limits rather
than querying a live model catalog. Missing legacy binding evidence fails closed.
The ordinary execution driver uses the retained binding. Complete resource
coverage and all provider/preparation paths remain under qualification.

Every Agent/Orchestrator builder requires an explicit `behavior_fingerprint` or
`behavior_version` declaration for opaque native policy, tool and callback behavior.
Bump it when that behavior changes; keyed options must carry the configured
fingerprint (available through `agent.behavior_fingerprint()`). Credentials and
transport correlation are excluded. Provider implementations must separately
declare their immutable route/backend/configuration through `binding_descriptor`.
Built-in HTTP adapters compute pure declarations from their endpoint, protocol,
implementation version and user agent, with unknown model limits left unknown.
Custom resolvers and opaque custom HTTP callbacks require an explicit
`VersionedResolver`/`VersionedAdapter` allowlist; no declaration means unavailable.
Known keyed replay checks retained metadata before factory initialization and
never reacquires a provider or grants execution authority.

The frozen plan also records a private fixed inventory of configured wall and
monotonic clocks, observers, model streams, permission policy, tool pipelines,
host services and workspace readers. Changing a slot between its runtime default
and an explicit host callback rejects executable recovery before a provider
factory runs. The host behavior fingerprint still versions the callback's
semantics. This inventory grants no resource cleanup or whole-run ownership
coverage. Older plans without the inventory cannot pass executable recovery.
An Agent with no configured observers uses the runtime's default observer;
configured observers, including Datadog, remain explicit host callbacks.

The experimental core `InputEnvelope` validates bounded ordered input and defines
the versioned canonical hash for typed admission. It preserves supplied message,
parent and part IDs, explicit media representations, frontend input/output schema
separation, context, null-versus-absent state, captured revision and continuation
generation. Construction does not grant role, result or filesystem authority.
`Agent::prompt_input_in`, `prompt_input_keyed_in` and
`recover_input_admission_in` accept that original envelope in an explicit
`ExecutionContext`. The host authorizes roles/history/artifacts and retains the
coherent snapshot revision it validated, including when a wire client omitted
If-Match. All bundled stores check that revision inside the admission mutation
after identity and receipt reconciliation. A stale new admission changes no
input, run or revision; an exact prior receipt still replays after revision
advances. The text convenience APIs adapt to an envelope; explicit typed retries
must retain their original envelope rather than refreshing its revision.

Supplied message and part IDs are unique across the entire shared store, not
only within a session or tenant. Hosts must authorize incoming history against
host-owned canonical history and admit only its new suffix with globally unique
storage IDs. Wire IDs are correlation data, not authority. A collision fails
closed as `StoreError::Conflict`, which also represents archived-session and
stale-owner conditions; do not infer rejection or automatically rewrite IDs from
that variant. Persist the exact accepted envelope/key for retries. Never strip or
regenerate IDs on an uncertain retry, or refresh its captured revision.

Admission preserves every message/part and supplied identity in order, with
new internal IDs excluded from the digest. Recovery validates the original input
prefix and uses the original envelope. Version-2 pause checkpoints reference its
canonical digest instead of duplicating private context/state into a run record.
Context contributes to every turn and compaction request separately from
activity. HTTP transports encode context as canonical user-role JSON text and
report supported roles, media sources, MIME types and placements through
`input_support()`. Responses encodes image data/URLs/opaque files in user input
and function output; Chat Completions encodes data/URL images in user input.
Other representations fail before credentials, headers or HTTP. Typed media
remains ordered and Crabber never fetches source URLs. Frontend descriptors and
continuation envelopes use the explicit durable driver described in [frontend tools](frontend-tool-continuations.md).

`Store::session_revision` reads the current opaque public revision. Public history,
call, epoch, event and accepted same-run input mutations advance it atomically;
lease refresh, private extension state, failed writes and receipt replay do not.
Values can have gaps and are separate from event cursors and history-snapshot
invalidation counters. Custom stores can run `revision_contract::run_contract`.
This scalar alone does not validate a separately captured history snapshot;
use the coherent public snapshot contracts described below. Conditional admission
is available through the captured typed envelope and `AdmitRequest.expected_revision`.

The experimental `Store::public_state` reads state, display activities and their
revision from one store snapshot. `ExecutionStore::mutate_public_state` replaces
the complete state/activity set at an expected revision, atomically recording a
`PublicStateSnapshot` event and operation receipt. Null, scalar and array state,
embedded NUL and finite JSON numbers retain their meaning in all bundled stores.
State and activities remain separate from model conversation input.

A native tool receives its bound display writer through `ToolContext::public_source()`.
Call `snapshot()` for the current public view, then retain the original operation ID,
expected revision and `PublicSourceChange` before `publish()`. The writer supports
state snapshots, RFC6902 state deltas, activity snapshots and activity deltas. A failed
patch changes no state, event, receipt or revision. Attribution comes from the actual
native attempt/tool phase or detached invocation; a payload cannot choose another
session, run or call. The committed source records feed the protocol projector.
Ordinary legacy state writes do not manufacture that source association.

Retained exact retries reconcile their original receipt even after revision or
ownership advances. They grant no fresh writer: a new write after its source closes
refuses stale ownership. Retain unknown operations; absence from a lookup does not
prove an in-flight write rolled back. Custom adapters must forward or implement the
atomic source methods; their defaults remain `Unsupported`.

The current bounds are 256 KiB for state, 64 KiB per activity, 128 activities,
1 MiB for the combined view and 256 operations per patch. Bound source mutations
and canonical source-publication reads use bounded proof accounting. Ordinary
`public_state()` snapshots retain their separate codec limits. These are ceilings,
not a promise that every combination fits. State/activity updates do not append
model messages or tool results; a tool returns its separate typed result through
the normal execution boundary.

Retain the original `PublicStateMutation` and its operation ID before writing.
After `UnknownStoreOutcome`, use `Store::lookup_public_state_mutation` to reconcile
the original ID. Compare its digest with `mutation.fingerprint(session, run)`;
exact retry also returns the original receipt after ownership or revision changes.
A missing lookup does not prove an in-flight write cannot later commit.
A changed request under that ID conflicts. Receipt reconciliation does not grant
permission to execute other operations.

For `ToolContext::public_source()`, `PublicSourceError::UnknownOutcome` can also
mean the native write committed but live delivery could not reconcile every
outstanding original operation. Retain each operation ID, expected revision and
original change privately. Reconcile its canonical receipt or retry the identical
publish before attempting fresh changes; a new ID does not repair uncertainty.
A missing receipt is not rollback evidence. The shared worker delivery gate emits
none of the pending group until every original has a canonical cursor, then emits
in cursor order without duplicate delivery. Additional native writes can commit
while delivery remains unresolved, so their errors must also be reconciled.
Termination or suspension seals that delivery scope: later exact retries can
reconcile metadata but never reopen its live observer or grant a fresh write. Custom stores default to `Unsupported`
and can qualify against `public_state_contract::run_contract` and the injected
rollback/unknown-response contract. This state snapshot does not include history
and therefore cannot validate a separately captured history snapshot.

Experimental attempt primitives are implemented by Memory, SQLite and PostgreSQL.
The runtime's first reservation uses `ReserveBound`: within the same fenced
transaction, validate its retained typed binding and exact original input prefix,
reject a checkpoint/nonzero usage or execution events/calls/phases/attempts, and
allow only admission's original context epoch. The check and fresh reservation
commit together. This recovers an admitted run whose process died before its first
reservation, including a lost keyed admission-Begin acknowledgment, without taking
absence of an attempt read as dispatch authority. Exact retries return metadata.
A Started native attempt, including a lost native Begin acknowledgment, never
re-enters this path. Recovery reports ineligible native attempts through
`RecoverySkipReason::Attempt` and continues with other eligible runs.

`Store::execution_attempt` returns coherent metadata and the current public revision.
`latest_execution_attempt(run)` discovers the highest generation without requiring
its ID or returning authority. An existing run with no reservation returns `None`;
absence does not prove an in-flight operation cannot commit. A missing run returns
`AttemptError::NotFound`. Custom stores default to `Unsupported`.
`ExecutionStore::mutate_attempt` reserves, begins, finishes, records cleanup or
reclaims an expired unstarted attempt. Only a fresh applied reserve/reclaim returns
ephemeral worker authority. Beginning is one-shot; receipt replay grants no new
permission to execute. Individual tool phases require separate authority.

Retain the operation key and original `AttemptMutation`. Reconcile an unknown
response through `Store::lookup_attempt_mutation` and compare its fingerprint.
Metadata lookup works without restoring a provider, plan or worker handle; a new
execution handle still requires current ownership. Started or unknown attempts
are never automatically reclaimed. Only a suspended attempt with completed
cleanup can advance to another generation. After claiming the paused run's fresh
fence, a new reservation atomically changes it to Running and creates an Unstarted
attempt. The reservation rechecks current run-wide phase quiescence and running
calls, even when the preceding attempt already recorded complete cleanup. Only
its fresh authority and one-shot Begin permit execution. A replay
returns metadata, and an old generation's authority cannot begin the new attempt.
A legacy pause without native suspended-attempt evidence cannot activate this path.

Record complete cleanup only after the owner has proved its resources stopped.
The stores additionally block it for running calls or unquiesced phase records.
These primitives record that proof's result; metadata, a lease or a returned task
alone does not establish quiescence. Runtime retains actual task joins and commits
native continuation barriers; these do not certify generic or custom resources
without authenticated coverage. Custom stores default to `Unsupported` and can run
`attempt_contract::run_contract` and its commit-fault conformance suite.

`ExecutionStore::conclude_attempt` commits a known terminal or suspended logical
run together with its owned attempt's outcome, complete-cleanup assertion, source
event, audit event, revision and receipt. The owner must first join all resources;
the transaction additionally rejects active/unquiesced phases and running calls.
Terminal outcomes reject pending tool calls too. A completed run with pending
inbox input returns `AttemptError::PendingInput` without ending its attempt or
writing a receipt, so the worker can incorporate that input and continue.
A suspension can retain pending calls and a bounded private checkpoint, with
usage recorded on the run. The runtime uses this operation for terminal and pause
boundaries. Incomplete/uncertain cleanup blocks continuation; native phase recovery
and the full public lifecycle/control interface remain unfinished.

Retain the original `AttemptConclusion` finish key/revision/target and boundary
after an unknown acknowledgement. Compare `AttemptConclusion::fingerprint` with
`lookup_attempt_mutation`; a metadata-only `mutate_attempt(Finish)` has a distinct
digest and cannot replay as a conclusion. Status, error, usage and checkpoint are
semantic; generated presentation timing/turn identity is excluded. Replays are
metadata only, including after a later continuation advances the run.
`RunBoundary` and `AttemptConclusion` redact Debug output. Public attempt audit
version 2 carries only the finite outcome, digest and attempt metadata; private
checkpoint/error bodies remain off the source stream. Existing metadata audit
version 1 is still read without rewriting it. `MAX_RUN_BOUNDARY_BYTES` bounds the
encoded boundary to 4 MiB, and checkpoint JSON is depth bounded before encoding.
Custom adapters default to `Unsupported` and can reuse the canonical source/run
helpers while implementing their own atomic ownership/receipt transaction.
The shared attempt contract and fault suite cover these boundaries; SQLite and
PostgreSQL also kill writers before commit and reconcile lost acknowledgements
from fresh processes without recovering worker authority.

Experimental native phase transitions work in Memory, SQLite and PostgreSQL.
Custom adapters default to `Unsupported`. `mutate_phase` commits a
phase record, source event, revision and receipt together. A fresh claim returns
opaque authority; only a fresh applied begin permits dispatch. The authority is
never serialized into a receipt or returned on replay. The runtime uses phases
around backend execution and frontend result processing. Host reconciliation remains explicit.

`ExecutionStore::settle_tool_phase` atomically completes a started phase and its
logical tool call, records the worker's assertion of complete quiescence, appends
protected typed result history and source events, and commits the final revision
and receipt. Invoke it only after joining every phase-owned resource. The Store
cannot inspect native tasks or prove external cleanup itself. The operation uses
`PhaseSettlement` with an exact `PhaseCommand::Complete`, generated presentation
IDs/timing and final protected output. It cannot turn an uncertain phase into a
completion, and the legacy logical-settlement API still rejects phase-owned calls.

Retain the original completion key/revision/target/output after an unknown
acknowledgement. Compare `PhaseSettlement::fingerprint` with
`lookup_phase_mutation`, since this compound operation has a separate digest
domain from metadata-only `mutate_phase(Complete)`. Those operations cannot replay
as one another. Generated presentation IDs/timing are excluded from semantics;
replay appends no message/event and grants no worker authority. Custom adapters
can use `PhaseSettlement::materialize` for canonical protected result records and
`verify_replay` for semantic/result checks; native fence and persisted receipt/event
verification remain the adapter's responsibility. The shared phase conformance
suite includes mixed media, changed choices, duplicate-message rollback and lost
commit acknowledgements. SQLite/PostgreSQL also kill writers after all compound
writes but before commit and reconcile committed receipts in fresh processes.

Interrupting an executing native tool can return
`RuntimeError::PhaseOutcomeUnknown { run_id, phase, operation_id, .. }` from
`RunHandle::done()` instead of settling the run as `Interrupted`. Retain the exact
phase target, inspect `list_tool_phases` / `Store::tool_phase`, and reconcile any
saved operation ID with `lookup_phase_mutation`. Do not blindly resume that tool
or infer cleanup from the interrupt. The following resolution contract applies
to an already uncertain target and requires the host's verified outcome.

A lost claimed or started phase becomes uncertain, including a retry-safe call.
The host can resolve its exact target as success with supplied typed output,
failure, or refusal. The receipt and event retain the resolver identity and
host-asserted authority claim. The library cannot verify a human's authority.
Identical retries return the original receipt; changed choices, output or claims
conflict. Refusal leaves uncertainty intact. Final output remains separate from
metadata and reconciliation never acknowledges worker cleanup. Only record
quiescence after actual task joins or explicit ownership transfer. An uncertain
phase blocks attempt cleanup even when its worker is quiescent. Compound
suspension/checkpoint/interaction/barrier persistence is implemented for explicit
native and frontend continuations; an ordinary phase claim cannot
resume a suspended phase.

Retain the original `PhaseMutation` and reconcile unknown acknowledgements with
`lookup_phase_mutation`. Metadata reconciliation does not restore factories or
grant dispatch authority. `phase_contract::run_contract` and its commit-fault
suite qualify these native transitions. SQLite/PostgreSQL process tests kill
writers before commit and reconcile lost acknowledgements in fresh processes.
Worker capabilities stay inside the writer; phase validation checks its parent
without returning the broader attempt capability. An owner can finish phase
quiescence after reporting incomplete attempt cleanup. The runtime connects
actual owned task joins to durable phase/attempt boundaries;
a public quiescence claim still requires the embedding to account for its own
external resources.

If execution cannot acknowledge its native attempt conclusion, it returns
`RuntimeError::AttemptBoundaryUnknown` with the runtime run ID and attempt
storage cause. The compatibility variant `RuntimeError::TerminalOutcomeUnknown`
is individually unstable and is not emitted by the current native driver.
The task may have stopped while the retained run is still
running, or the terminal write may already have committed. Reconcile the run and
durable events before deciding what to do next. Runtime terminal notifications
follow acknowledged persistence. Owned ordinary/resumed paths retain joins. Complete resource coverage across
all preparation providers, external services and historical runs is not inferred.

Current regression coverage is in `crates/crabber/tests/server_host.rs` and
`crates/crabber-extension/tests/context_plans.rs`: concurrent explicit workspaces,
preserved custom registrations and defaults, exact retry and root drift,
unavailable workspace values, terminal ingress closure, and fingerprint ordering.

### Typed tool completions

`ToolExecutor::execute` and `execute_with_context` return validated
`crabber::core::ToolOutput`: ordered text and explicit media sources, plus a
self-declared error bit. `JsonToolExecutor` is the deliberate JSON-only adapter;
its JSON becomes literal text and is never inspected for media or authority.
Use `ToolOutput::from_json` for a JSON result, or `ToolOutput::new` for content.
`json_value` explicitly reads a single JSON text block and rejects mixed content.

Execution around-middleware receives and returns a version-1 completion envelope
through `tool_output_envelope` / `parse_tool_output_envelope`. Result transforms
receive `ToolOutput` directly; JSON result-transform envelopes carry it in
`result`. The transform contract fingerprint is version 3. Content passes through
ordinary and final redactors into durable tool settlement and source events.
Tool, middleware and transform error escalation cannot be cleared by a later
stage. Call/session/workspace identity remains runtime-owned.

WASM tools use `crabber:extensions@0.2.0` and return the same versioned completion
envelope. `crabber_guest::json_tool_output` provides explicit JSON-to-text output;
`ToolOutput::into_envelope` supports mixed content. Unsupported versions and
malformed content fail closed. Rebuild guests after this contract change.


## Bounded public management and adoption

The [offline server-host probe](../examples/server-host-probe/README.md) and
[locked path consumer](../testdata/server-host-consumer/README.md) execute public
custom Store, custom tool, scoped factory, two-context, explicit interaction and
post-close control flows. They use a scripted provider and require no credentials.
They qualify local embedding only; the external Git-only consumer, final SDK
projection and standard-tool adoption gates remain separate. The original
minimal-embed glue remains subject to its existing 60-line limit.

Use `Agent::session_summary` and `list_sessions` for bounded display metadata.
`manage_session` supports stable-ID empty creation, rename, selected defaults and
reversible archive under revision CAS. Archive blocks fresh ordinary admission;
it does not cancel existing work or prevent original receipt replay. Explicit
execution contexts keep their immutable workspace/directory and selection.

`public_session_snapshot_v2` combines history and safe queue, continuation,
frontend and ownership summaries. It shares one proof allowance across the
actual bounded readers; it returns explicit Limited/unsupported outcomes instead
of silently omitting evidence. V1 remains available with its original shape.
Follow the returned inner history continuation with the same V2 request profile;
it binds the captured revision and imported-history cutoffs. A revision change
invalidates the capture. These DTOs expose no worker capability, private checkpoint,
raw external answer or supplied frontend output. Empty local task lists are not
durable quiescence evidence.

Tools acquired from `Registry::mount_owned` use concrete mount invocation
facades, including explicit resumable start and resume calls. An extracted
executor refuses new invocation after that mount seals. The mount retains the
original invocation join across caller cancellation; Runtime phase resources
also retain that same join through their callback owner. Legacy mounts and
standalone plans keep their original behavior. A held executor `Arc` can still
keep the original object and opaque resources alive after close: invocation
joins do not prove reference disposal, physical generation shutdown or a whole
Run's resource policy. Guards, provider streams and raw host
services require their own lifecycle bridges.

Prompt contributors and typed system-prompt middleware from owned mounts also
retain their original invocation joins. A collector deadline or cancellation can
return before callback disposal finishes; mount close and Runtime callback drains
still wait for that original task. Disposal failure remains a cleanup failure.
Reader capabilities or other opaque objects retained by callbacks require their
own lifecycle ownership.

`ForkSession` copies a verified closed history prefix into a new session with
nonexecuting `ImportedToolCall` and message provenance. It does not clone run
capabilities, pending questions, native effects or execution authority. The
original operation key and source/target identity must be retained for exact
replay. An exact replay remains metadata after later source changes.

### Fixed Memory scripted sessions

`crabber::session::session_owner::owned_scope::FixedMemoryInitialHost` runs a
finite scripted response using its own MemoryStore, SystemClock and cleanup
supervision. Keep one host for multiple sessions; cloned hosts share that same
store and supervisor. `host.store()` exposes the real store for session management
and reads. This path takes a pristine `LibraryScriptedSource` built from owned
route/response values. It accepts no replacement Store, clock, factory or
already-prepared execution.

Create a managed session first, then supply its exact workspace, directory and
current revision. Registration returns a parked task: call `start()` explicitly
and await `join()` to observe execution and cleanup. The optional admission key
identifies the original input; it does not turn a retry into another execution.

Keep the host in application-owned state. Create the managed session separately
with a stable caller-owned `ManageSession` request; retain that original request
across a creation error or canceled await. Use its acknowledged current revision
and exact session context below. The management workflow above covers creation.
Call this synchronous helper inside an existing Tokio runtime. It borrows the
host and returns the original parked task without starting or replacing an
execution:

```rust,no_run
use crabber::{
    AdmissionKey, Selection, SessionContext,
    core::{InputEnvelope, SessionRevision},
    extension::LibraryScriptedSource,
    session::session_owner::owned_scope::{
        FixedMemoryInitialHost, FixedMemoryInitialInput, FixedMemoryInitialTask,
    },
};

fn register_fixed(
    host: &FixedMemoryInitialHost,
    context: SessionContext,
    revision: SessionRevision,
) -> Result<FixedMemoryInitialTask, Box<dyn std::error::Error + Send + Sync>> {
    let source = LibraryScriptedSource::new(
        Selection { provider_id: "library".into(), model_id: "text".into() },
        vec!["A finite library-owned response.".into()],
    )?;
    Ok(host.register_scripted(FixedMemoryInitialInput {
        context,
        title: "scripted session".into(),
        input: InputEnvelope::text("hello", revision)?,
        key: Some(AdmissionKey::new("first-input")?), // None selects unkeyed admission.
    }, source)?)
}
```

Retain that host and task in the caller, together with an initially empty
`Option<LocallyJoinedInitialSeal>`. Call `task.start()` once. The observation helper
borrows the task and retry slot, so an error or canceled await leaves both with
the caller. It saves any original cleanup retry before reporting an unresolved
outcome:

```rust,no_run
use crabber::session::session_owner::owned_scope::{
    FixedMemoryInitialTask, LocallyJoinedInitialSeal, PendingInitialProducerStatus,
};

async fn observe_fixed(
    task: &mut FixedMemoryInitialTask,
    seal: &mut Option<LocallyJoinedInitialSeal>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let observed = task.join().await?;
    if seal.is_none() {
        *seal = observed.take_retry();
    }
    if observed.error().is_some()
        || observed.observation_failed()
        || observed.status() != Some(&PendingInitialProducerStatus::ClosedAssignment)
    {
        return Err("execution or cleanup needs reconciliation; retain the original state".into());
    }
    Ok(())
}

async fn retry_fixed_cleanup(
    seal: &mut Option<LocallyJoinedInitialSeal>,
) -> Result<PendingInitialProducerStatus, Box<dyn std::error::Error + Send + Sync>> {
    let original = seal.as_mut().ok_or("no original cleanup retry is available")?;
    Ok(original.retry().await?)
}
```

Retry cleanup explicitly on that same retained handle. A retry error or canceled
await leaves the handle in the slot. Its returned status reconciles cleanup only;
it does not acknowledge a previously unknown business call or resume execution.
Do not rerun registration or `start()` to resolve an unknown outcome. A missing
retry handle does not prove cleanup succeeded. These local handles cannot be
recovered from stored IDs after process restart.

Use `task.control().request_stop()` for an explicit stop and still observe the
original task's join. Dropping the task also signals stop; the host retains its
original cleanup work. A caller timeout or an empty event/task list does not prove
that cleanup finished. Keep the same host alive and await its retained
`wait_for_supervision()` after resolving the original observation. Caller-owned
retry state remains separate from that wait.

After successful completion and cleanup, create a `DeleteSession` with a new
operation ID and the current `host.store().session_revision(&session)`. Retain that
exact request in caller-owned state before sending it. This helper borrows it:

```rust,no_run
use crabber::session::{
    DeleteSession, DeletionError, DeletionOutcome, Store,
    session_owner::owned_scope::FixedMemoryInitialHost,
};

async fn delete_fixed(
    host: &FixedMemoryInitialHost,
    original: &DeleteSession,
) -> Result<DeletionOutcome, DeletionError> {
    host.store().delete_session(original.clone()).await
}
```

On a transport error or canceled await, retain and retry the same request before
choosing another ID or revision. `Applied(receipt)` and exact `Replayed(receipt)`
identify the same deletion. A blocked result leaves the session intact; inspect
the blocker rather than assuming the task's completion guarantees deletion.

This is a one-turn text-only path. It accepts no caller-supplied tools, prompts,
extension factories or host callbacks, and runs without a heartbeat. Generic
`Agent`/injected Store paths and SQL backends do not acquire this fixed resource
policy. Successful completed fixed sessions
may contain several completed fixed tasks and can become eligible for guarded
deletion once all their retained execution and owner history is covered. The full
readiness-and-purge operation shares one 32 MiB proof allowance: even completely
covered history can return `ProofLimit` when the whole operation does not fit.
That refusal preserves the session and records no deletion receipt. Extra
Raw/generic runs, owners or incomplete sources keep deletion blocked. Stopping
before Open, closing without a published Run, or joining a partial preparation
does not make that history eligible for deletion.

`crabber::session::DeleteSession` requires an exact revision (including revision
zero), stable operation ID and positively verified creation coverage. Quiet
managed or fork-created sessions still require full native readiness. Memory
also accepts managed sessions whose entire retained history consists of completed
fixed Memory scripted tasks and their joined owners. Extra generic/Raw runs,
incomplete sources, legacy history and uncovered owners remain blocked with
`OwnershipUnknown`; active work retains its specific blocker. Generic Store and
SQL execution do not gain this additional positive path.

Deletion commits a minimal permanent tombstone and receipt with physical purge
atomically under the session gate. After response loss, retry the original
request before choosing another key. Changed semantics conflict; exact replay
precedes current CAS. This is logical erasure, not cryptographic erasure of backups
or proof that arbitrary native resource types have joined.

Current bundled storage versions are PostgreSQL 25 and SQLite 20. PostgreSQL uses
its versioned migration roster; SQLite accepts fresh current files and rejects
older schemas rather than silently upgrading. Always run explicit migration
before read-only `connect`; see [SQLite storage](sqlite-store.md).

## Workspace-scoped roles and row-level security

PostgreSQL schema 25 enforces workspace isolation in the database. Run
`PostgresStore::migrate` as the schema owner. It creates the NOLOGIN groups
`crabber_worker` and `crabber_host`, tenant policies, and
`crabber_workspace_grants`. Use one LOGIN role per worker. Policies consult
`current_user`, so grant rows name the LOGIN role, rather than its group.
Hosts see every workspace; workers see only explicitly granted workspaces.
Memory and SQLite provide workspace-filtered discovery without database role
isolation. A host must still admit filesystem roots and execution contexts.

Provision roles as the owner, replacing these placeholders through your secret
manager. Set `CRABBER_POSTGRES_URL` to a connection URL naming the worker LOGIN
role; keep credential values outside documentation and source control.

```sql
CREATE ROLE <worker_role> LOGIN PASSWORD '<password-from-secret-manager>' IN ROLE crabber_worker;
INSERT INTO crabber_workspace_grants(role_name, workspace_id)
VALUES ('<worker_role>', '<workspace_id>');
CREATE ROLE <host_role> LOGIN PASSWORD '<password-from-secret-manager>' IN ROLE crabber_host;
-- Retire a worker after stopping its processes:
DELETE FROM crabber_workspace_grants WHERE role_name = '<worker_role>';
DROP OWNED BY <worker_role>;
DROP ROLE <worker_role>;
```

| Operation | Agent/Store entry | Worker role needs | Host role needs |
| --- | --- | --- | --- |
| Connect and verify | `PostgresStore::connect` | SELECT `schema_version` | Same |
| Claim, prepare, admit, dispatch future items | `mutate_queue`, `prepare_queue_in`, `admit_queue_in`, `dispatch_queue_in`, `recover_queue_in` | DML on `sessions`, `runs`, `future_queue*`, `queue_publication_*`, `operation_receipts`, `events`, `messages`, `parts`, `admission_*`, `execution_attempts`, `run_execution_bindings` within granted workspaces | Same, all workspaces |
| Ordinary/keyed admission and execution | `prompt*`, `recover_admission*` | Tenant DML within granted workspaces; USAGE on identity sequences | Same, all workspaces |
| Recovery discovery and sweep | `recover_workspace*`, `list_workspace_unfinished_runs` | SELECT `runs`, `sessions`, plus execution DML | Same, all workspaces |
| Native enrollment and reconciliation | `reserve_session_owner`, `register_execution_driver`, `pending_initial_native` | DML on `session_owner_*`, `pending_initial_*`, `initial_consumer_*`, `native_*` | Same, all workspaces |
| Controls observation | `run_controls`, `observe_run_controls` | SELECT/INSERT `run_controls`, `run_control_progress` | Same, all workspaces |
| Public snapshots | `public_session_snapshot*` | Tenant SELECT; SELECT `snapshot_auth` | Same, all workspaces |
| Session management, fork, delete | `manage_session`, `fork_session`, `delete_session` | Tenant DML, including INSERT `session_tombstones` | Same, all workspaces |
| Grant administration | SQL | None | INSERT/DELETE `crabber_workspace_grants` |
| Schema migration | `PostgresStore::migrate` | Denied for schema changes | Denied for schema changes; owner only |

Calling `migrate` against an already current schema is read-only and can succeed
as a runtime role. Upgrading remains an owner operation. Group roles are
cluster-global: the owner needs `CREATEROLE`, or a DBA must create both groups
before migration. Runtime roles cannot create objects in the Crabber schema.
The RLS coverage test requires an explicit policy classification when a future
migration adds a table. Existing trigger functions retain invoker privileges;
`crabber_session_born(text)` is the sole SECURITY DEFINER helper, with a pinned
search path and an existence-only result for pre-birth scoping.

`Agent::recover_workspace` and `Store::list_workspace_unfinished_runs` scope
recovery discovery. `Agent::list_queue_heads` returns the first Queued or Claimed
item per session, ordered by session ID with limits 1–256 and a session-ID
cursor. A Claimed head is reclaimable only through the existing expiry-checked
claim operation. These filters aid discovery; PostgreSQL roles enforce access.
Keep the PostgreSQL CI suites on both architectures and inspect
`EXPLAIN (ANALYZE)` under a worker role for events, messages, tool calls, parts,
and queue heads after changing policies. Compare identical worker/admin
journeys on the same machine; a worker slowdown above 1.5× requires an owner
stop/go decision.

The isolation contract has these accepted residual risks:

| Risk | Mitigation or acceptance |
| --- | --- |
| `session_tombstones` metadata is readable by every worker | Documented read-only exception; inserts remain scoped and mutation is denied |
| `snapshot_auth` is one shared database secret | Accepted; per-worker keys are deferred |
| Another session's advisory lock can be held to stall it | Accepted availability risk; roles isolate rows, not advisory locks |
| Session IDs are global and pre-birth IDs can be squatted | Accepted: a worker learning a future ID may observe enrollment in its own reserved workspace until birth and make the ID unusable by the intended owner. Reservation and birth fail closed on identity drift; allocate unpredictable IDs |
| Owner-installed functions are callable unless their grants restrict them | Keep the schema owner trusted and prohibit worker DDL; only the existence helper has definer privileges |


## Proof and recovery discovery limits

Bundled credential readers authenticate the actual selected Run and its original
enrollment evidence; current-issuer checks authenticate the selected Run's native
source/issuer prefix. They do not revalidate every message or all sessions' history
merely because a session is old. Actual selected evidence must fit the operation's
finite allowance (commonly 32 MiB); exceeding it refuses authentication, never
silently drops evidence or substitutes trust in a cached head. Missing-middle
originals and inconsistent reverse membership still refuse. There is no automatic
compaction, source rollover or guaranteed constant per-write cost.

`Store::list_unfinished_runs` deliberately authenticates the complete captured
unfinished set under one snapshot and one 32 MiB materialization/proof allowance.
A Pending origin that cannot be returned as a Raw credential, corrupt evidence or
exhausted allowance refuses the entire list. `recover` / `recover_with_context`
first await this discovery; its error returns no partial `RecoverReport`.
`SkippedRun` and sweep continuation apply only to the supported per-run refusals
encountered after successful discovery. Keep original run IDs and requests for
individual inspection/reconciliation when discovery refuses; do not interpret an
error as an empty list, reset evidence, invent new IDs or rerun uncertain work.

These allowances bound selected materialization and verification, not database
scan cost, stored history or transaction runtime. Memory mutation still clones
the whole State and some reads scan global collections; unrelated retained data
can affect cost. SQLite serializes writes across its whole file. SQL/client page
limits likewise do not promise bounded server-side scans. Plan capacity using
actual workload measurements; the finite Memory transaction-cost fixture is a
regression check, not a production memory cap or an O(1) performance guarantee.

For process telemetry, install [`Telemetry::layer()`](observability.md) once, then open a host request span per HTTP request and call `Telemetry::current_trace_context()` inside it. Hosts that propagate incoming W3C context set the request span parent with `OpenTelemetrySpanExt::set_parent` before entering it.


## Required PostgreSQL verification

Install the [pinned test tools](test-performance.md), then run the actual host
probe and public facade contracts through managed, per-history databases:

```sh
cargo run -p xtask --features postgres-check -- check-postgres --package server-host-probe
cargo run -p xtask --features postgres-check -- check-postgres --package crabber --test server_host
```

Docker supplies the PostgreSQL 14 server by default. An existing dedicated
administrative test server can be selected with `CRABBER_TEST_POSTGRES_ADMIN_URL`.
Raw URL-only required test configuration is unsupported; fixture URLs and opaque
identities are passed explicitly to fresh workers by their parent.
`ExecutionContext::with_model_controls` selects reasoning effort beside each
provider/model selection. Stored session defaults (`SelectionFallback`) carry
provider/model IDs only and never supply effort; admissions using defaults keep
the configured host controls.

`ExecutionContext::with_session_defaults()` resolves stored provider/model IDs
atomically at admission, using `selection()` as fallback. The context's own
`ModelControls` apply to the resolved model, allowing one Agent to select effort
per admission even when the model comes from stored defaults.
