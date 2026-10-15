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

`close_extensions` now stops ingress even for an agent with no extensions.
Readiness and new prompts return `RuntimeError::HostClosed` once closing starts.
Matching committed keyed requests can still return their original metadata receipt.
It closes extension resources and leases; it does not join active executions.

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
actual preparation task joins separately from execution joins. These local IDs
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

These joins do not prove durable terminal outcome, callback/child-process cleanup
or notification-task completion. Terminal-write errors, including lost acknowledgements
after commit, return `RuntimeError::TerminalOutcomeUnknown` without a second terminal
write. Reconcile the durable run and events; live terminal events require an
acknowledged commit. Facade readiness/context checks before entering the runtime
are outside this ownership scope. Native attempt/phase integration, durable
cancellation and full orderly close remain unfinished. Result/completion signals
and event EOF never replace these checks.

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
Worker completion and a committed logical outcome do not imply those resources
were cleaned up. These are local task observations; durable phase quiescence,
external-effect reconciliation and explicit session-owned job transfer are still
part of the pending native phase integration. They must not be inferred from
this report. Execution fingerprints include resource contract version 1 and its
capacity; old plans fail closed and require their matching binary to finish.

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

The common owned initial/resumed execution lifecycle, typed admission, authoritative controls
and session management remain part of this implementation. Their completion is
required before claiming the durable server-host contract delivered.

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
nesting. PostgreSQL schema 7 and fresh SQLite schema 3 expand the private codec
carriers; retained older payloads are not rewritten or made executable. Custom
adapters default to `HostUnsupported` and must implement the atomic capability.
The runtime uses these atomic APIs for both ordinary and keyed admission. It
restores the retained backend/configuration before recovery claims, holds the
same implementation across turns and compaction, and uses captured limits rather
than querying a live model catalog. Missing legacy binding evidence fails closed.
The complete common execution driver remains unfinished.

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
remains ordered and Crabber never fetches source URLs. Frontend descriptors and continuation envelopes
still require the pending durable interaction driver and fail closed today.

`Store::session_revision` reads the current opaque public revision. Public history,
call, epoch, event and accepted same-run input mutations advance it atomically;
lease refresh, private extension state, failed writes and receipt replay do not.
Values can have gaps and are separate from event cursors and history-snapshot
invalidation counters. Custom stores can run `revision_contract::run_contract`.
This scalar alone does not validate a separately captured history snapshot;
coherent public snapshots are still being implemented. Conditional admission
is available through the captured typed envelope and `AdmitRequest.expected_revision`.

The experimental `Store::public_state` reads state, display activities and their
revision from one store snapshot. `ExecutionStore::mutate_public_state` replaces
the complete state/activity set at an expected revision, atomically recording a
`PublicStateSnapshot` event and operation receipt. Null, scalar and array state,
embedded NUL and finite JSON numbers retain their meaning in all bundled stores.
Activities never become model conversation input. These operations are not yet
wired into runtime tools or the protocol projector.

Retain the original `PublicStateMutation` and its operation ID before writing.
After `UnknownStoreOutcome`, use `Store::lookup_public_state_mutation` to reconcile
the original ID. Compare its digest with `mutation.fingerprint(session, run)`;
exact retry also returns the original receipt after ownership or revision changes.
A missing lookup does not prove an in-flight write cannot later commit.
A changed request under that ID conflicts. Receipt reconciliation does not grant
permission to execute other operations. Custom stores default to `Unsupported`
and can qualify against `public_state_contract::run_contract` and the injected
rollback/unknown-response contract. This state snapshot does not include history
and therefore cannot validate a separately captured history snapshot.

Experimental attempt primitives are implemented by Memory, SQLite and PostgreSQL.
`Store::execution_attempt` returns coherent metadata and the current public revision.
`ExecutionStore::mutate_attempt` reserves, begins, finishes, records cleanup or
reclaims an expired unstarted attempt. Only a fresh applied reserve/reclaim returns
ephemeral worker authority. Beginning is one-shot; receipt replay grants no new
permission to execute. Individual tool phases require separate authority.

Retain the operation key and original `AttemptMutation`. Reconcile an unknown
response through `Store::lookup_attempt_mutation` and compare its fingerprint.
Metadata lookup works without restoring a provider, plan or worker handle; a new
execution handle still requires current ownership. Started or unknown attempts
are never automatically reclaimed. Only a suspended attempt with completed
cleanup can advance to another generation, and paused runs cannot begin work.

Record complete cleanup only after the owner has proved its resources stopped.
The stores additionally block it for running calls or unquiesced phase records.
These primitives record that proof's result; metadata, a lease or a returned task
alone does not establish quiescence. Runtime task ownership and continuation
barriers remain under implementation. Custom stores default to
`Unsupported` and can run `attempt_contract::run_contract` and its commit-fault
conformance suite.

`ExecutionStore::conclude_attempt` commits a known terminal or suspended logical
run together with its owned attempt's outcome, complete-cleanup assertion, source
event, audit event, revision and receipt. The owner must first join all resources;
the transaction additionally rejects active/unquiesced phases and running calls.
Terminal outcomes reject pending tool calls too. A completed run with pending
inbox input returns `AttemptError::PendingInput` without ending its attempt or
writing a receipt, so the worker can incorporate that input and continue.
A suspension can retain pending calls and a bounded private checkpoint, with
usage recorded on the run. Incomplete/uncertain cleanup and the runtime's adoption
of this operation remain part of the pending attempt driver.

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
never serialized into a receipt or returned on replay. These APIs are not yet
wired to the runtime's tool pipeline.

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

A lost claimed or started phase becomes uncertain, including a retry-safe call.
The host can resolve its exact target as success with supplied typed output,
failure, or refusal. The receipt and event retain the resolver identity and
host-asserted authority claim. The library cannot verify a human's authority.
Identical retries return the original receipt; changed choices, output or claims
conflict. Refusal leaves uncertainty intact. Final output remains separate from
metadata and reconciliation never acknowledges worker cleanup. Only record
quiescence after actual task joins or explicit ownership transfer. An uncertain
phase blocks attempt cleanup even when its worker is quiescent. Compound
suspension/checkpoint/interaction/barrier persistence remains under implementation;
an ordinary phase claim cannot resume a suspended phase.

Retain the original `PhaseMutation` and reconcile unknown acknowledgements with
`lookup_phase_mutation`. Metadata reconciliation does not restore factories or
grant dispatch authority. `phase_contract::run_contract` and its commit-fault
suite qualify these native transitions. SQLite/PostgreSQL process tests kill
writers before commit and reconcile lost acknowledgements in fresh processes.
Worker capabilities stay inside the writer; phase validation checks its parent
without returning the broader attempt capability. An owner can finish phase
quiescence after reporting incomplete attempt cleanup. Connecting runtime task
joins to these durable phase/attempt transitions remains under implementation.

If the initial execution cannot acknowledge its interruption/failure terminal
write, it returns `RuntimeError::TerminalOutcomeUnknown` with the runtime run ID
and storage cause. The task may have stopped while the retained run is still
running, or the terminal write may already have committed. Reconcile the run and
durable events before deciding what to do next. Runtime terminal notifications
follow acknowledged persistence. The common owned/resumed execution lifecycle
and cleanup/join guarantees remain under implementation.

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
