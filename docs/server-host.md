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

Register shareable state explicitly with `shared_tool`, `shared_extension`,
`shared_wasm_extension`, or `shared_host_services`. These replace the old builder
names without compatibility aliases. A scope filters visibility of the same
instance. Its state must tolerate concurrent use across admitted contexts.

`RunPlanProvider::registration_fingerprint` inspects frozen declarations without
creating contextual owners, initializing components or acquiring a plan lease.
Static, live Registry and contextual providers implement it; custom providers explicitly implement
this capability. Contextual providers capture shared metadata and factory declarations
at configuration time, including failures needed for truthful readiness rollback.
This declaration fingerprint does not replace checking actual restored instances.

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

Immutable provider-binding restoration, the common
owned initial/resumed execution lifecycle, typed admission, authoritative controls
and session management remain part of this implementation. Their completion is
required before claiming the durable server-host contract delivered.

The experimental core `InputEnvelope` validates bounded ordered input and defines
the versioned canonical hash for typed admission. It preserves supplied message,
parent and part IDs, explicit media representations, frontend input/output schema
separation, context, null-versus-absent state, captured revision and continuation
generation. Construction does not grant role, result or filesystem authority.
The envelope is not yet wired into Agent admission or durable execution capsules.

`Store::session_revision` reads the current opaque public revision. Public history,
call, epoch, event and accepted same-run input mutations advance it atomically;
lease refresh, private extension state, failed writes and receipt replay do not.
Values can have gaps and are separate from event cursors and history-snapshot
invalidation counters. Custom stores can run `revision_contract::run_contract`.
This scalar alone does not validate a separately captured history snapshot;
coherent public snapshots and conditional admission are still being implemented.

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

Experimental native phase transitions work in Memory, SQLite and PostgreSQL.
Custom adapters default to `Unsupported`. `mutate_phase` commits a
phase record, source event, revision and receipt together. A fresh claim returns
opaque authority; only a fresh applied begin permits dispatch. The authority is
never serialized into a receipt or returned on replay. These APIs are not yet
wired to the runtime's tool pipeline or logical result/history settlement.

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
quiescence after reporting incomplete attempt cleanup. Runtime task joining
remains under implementation.

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
