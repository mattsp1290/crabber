# Crabber session stores

The `postgres` feature uses `sqlx` with Tokio, rustls, and a PostgreSQL pool. It targets PostgreSQL 14 or newer and requires a dedicated database. Migrations create the Crabber tables in the default schema, so do not run them in a shared application schema.

The host calls `PostgresStore::migrate(url)` before `PostgresStore::connect(url)`. Connect verifies the schema version without changing it. The default build does not include the PostgreSQL driver.

The `sqlite` feature exposes `SqliteStore`. Call `SqliteStore::migrate(path)` then
`SqliteStore::connect(path)`; connect never creates a file or changes schema
(`connect_never_creates_a_file`, `connect_is_read_only`). See the
[SQLite operations guide](../../docs/sqlite-store.md) for lifecycle, file protection,
backup and restore.

Set `CRABBER_TEST_POSTGRES_URL` to run the live contract tests. Set `CRABBER_REQUIRE_POSTGRES=1` to fail if that URL is missing.

## Keyed admission (MemoryStore and PostgreSQL)

`SqliteStore` provides the same keyed contract (`sqlite_admission_contract`).

`Store::admit_keyed_run(KeyedAdmitRequest)` atomically creates a session if needed,
its user message, run/epoch, and immutable `AdmissionReceipt`. The request must
supply a stable session ID, `AdmissionKey`, claimed `InputFingerprint`, and host
`behavior_fingerprint`. A key is unique within its session for the full retained
session lifetime, including expired, reclaimed and terminal runs. There is no TTL,
eviction or key reuse. MemoryStore retains receipts only for the lifetime of its
shared in-memory state; restarting the process loses that state.

Only `KeyedAdmitOutcome::Started` contains `AdmitOutcome` and fencing authority.
`Replayed` and `lookup_admission` contain just the original receipt. They never
renew/claim a lease, modify a run, refresh receipt fields, or recreate an executor.
The receipt's run ID is also its admission identity. It includes the stable session,
run and user-message IDs plus bounded fingerprint metadata; no prompt, history,
checkpoint, paths, owner, claim token, status or execution fence. Keys are omitted
from receipts and Debug output. Use opaque correlation IDs, never secrets.

Stores must validate message/session/part identity and immutable session workspace
and directory **before replay**, then check the retained `(session_id, key)`:
identical fingerprint and semantic digest return `Replayed`; either mismatch returns
`AdmissionConflict`. Only an absent key reaches active-run `Busy` and fresh admission.
Another session may use the same key. Receipt/run/message creation is one transaction;
any failure before commit must roll back all three. A failed commit acknowledgement
may still mean that all records committed; do not report definitive rejection. Existing unkeyed admission remains available.

Custom Store implementors must override both new methods transactionally and run
`admission_contract::run_contract`, alongside `storetest::run_contract`. The default
implementations return `AdmissionUnsupported`, so adapters cannot silently degrade
keyed requests to unkeyed starts. Forward both methods in wrappers.

PostgreSQL schema version 2 adds retained receipts and their semantic digests in a
forward, idempotent migration that preserves existing sessions, runs and messages.
Per-session transaction locks serialize all mutations, including keyed/unkeyed
admission, lease renewal, recovery, inbox and abandonment. PostgreSQL locks the
session advisory gate before run rows, dependent rows and event/revision rows;
unrelated sessions remain independent. Receipt,
run, epoch and user message commit together. Lookup is read-only; replay never
changes lease ownership. PostgreSQL receipts survive pool and process restarts.

### Version 1 fingerprint contract

The host supplies a lowercase 64-character SHA-256 claimed fingerprint of its
canonical semantic input envelope. Keys accept 1–128 ASCII letters, digits, dash,
underscore and dot. Both constructors and deserialization validate these bounds,
with redacted errors and Debug. Validation does not imply the host-provided digest
is truthful: the library independently computes a separate semantic digest.

The runtime hashes structured JSON with domain `crabber.runtime.admission.v1`:
provider and model selection, system prompt, output token cap, execution mode/concurrency,
compaction ratio/tail count, turn limit, acquired tool schemas/metadata, ordered
prompt sections, restrictions, component identities, guard IDs and provider IDs/names.
The runtime admission semantics array has 15 ordered elements. Local capsules
and pause checkpoints created before the output-cap field was introduced are
invalid. Reset or re-migrate local PostgreSQL data when applying or reverting
this change; the admission domain and capsule version remain version 1.

The runtime includes a `contract:crabber/registration-profile.v1` component that
binds frozen declaration metadata and the plan provider's original fingerprint.
This additional component preserves the existing capsule and admission digest
formats. It enables committed request verification before contextual setup;
actual acquired instance schemas are still checked for Unstarted restoration.
Runtime replay requires this evidence. Bare receipt lookup remains available for
older capsules without the component.

The acquired plan fingerprint is also retained. The store's
`KeyedAdmitRequest::semantic_digest` hashes domain `crabber.admission.v1`, immutable
workspace/directory, title, user role/parent, ordered part ordinal/kind/content,
runtime config hash, plan fingerprint and host behavior fingerprint. JSON object
keys are explicitly sorted recursively before hashing; array order is
significant. Generated message/part/run IDs, timestamps, owner and lease are excluded.
History is durable session state, not a newly supplied retry input, and is excluded
so retries after execution still reconcile the original admission.

**Opaque behavior requires host versioning.** `behavior_fingerprint` is mandatory:
version native callback bodies, policies, approvers, provider/resolver routing and
configuration, model/tool middleware, host services, and any other semantic
configuration that cannot be reflected as data. Extension implementations must also
maintain their version/config identity. A code or opaque configuration change must
change that behavior digest. The library cannot detect a host falsely claiming
unchanged native code; it does detect changed inspectable payload/configuration even
when the caller reuses both old claims. Direct Store callers have the same obligation
to independently compute `config_hash` from their full execution configuration:
never populate it from the claimed input fingerprint alone.

### Host reconciliation

Persist the stable session ID, key, payload/config/version and fingerprint before
first admission. On `Started`, consume that single handle. On `Replayed`, correlate
by receipt and observe/recover through existing APIs; there is no second handle.
On `AdmissionConflict` or `SessionIdentityMismatch`, correct the caller's request;
do not reinterpret it as accepted. `Busy` for an absent/different key means another
run occupies the session. FIFO/outbox/dispatch policy belongs to the host.

On timeout or transport failure the result is unknown. Look up the original key,
or retry with the exact original inputs. A missing lookup while admission may be
in flight is **not proof of rejection**: the original transaction may still commit.
Keep using the same key; never invent a replacement key to resolve uncertainty.
Receipts establish admission, not completion or exactly-once external tool effects.
Existing fenced resume/recover rules remain responsible for execution recovery.


A PostgreSQL transport/commit failure is currently sanitized as
`StoreError::Validation("PostgreSQL operation failed")`. The `Validation` variant
alone therefore does **not** establish rejection. SQLite database failures use
`StoreError::Validation("SQLite operation failed")` and require the same unknown-outcome
handling (`busy_timeout_surfaces_as_sanitized_unknown_outcome`,
`sqlite_public_facade_fault_restart_journey`). Do not classify rejection by
matching human-readable error strings. Unless an error has a documented definitive
semantic outcome, reconcile it as unknown using the original key and payload. See
[the complete host decision table](../../docs/admission-receipts.md).

## Fenced abandonment (MemoryStore)

`SqliteStore` provides the same atomic settlement and exact-replay contract
(`shared_abandonment_contract`).

`Agent::abandon(AbandonRequest { expected, expected_owner, authority })` calls the
store directly, without resolving providers, mounting extension plans or invoking
tools or lifecycle hooks. It handles eligible legacy Pending, Running and Paused
runs even when their executable configuration is unavailable. Any native attempt
or phase evidence, or an active session approval barrier, causes
`AbandonError::NativeReconciliationRequired` without mutation. Existing live
`RunHandle::interrupt` and resume/recover behavior remain available.

Use `AbandonAuthority::ExpiredLease` for an observed expired owner. The store
checks expiry against its own clock under the transaction lock; a live lease,
including a paused run's live lease, returns `AbandonError::LiveLease`. The exact
run ID, claim token and owner must match. A replacement owner or stale token
returns `StaleOwner` without changes. Expiry is inclusive at the lease boundary.

`AbandonAuthority::HostStoppedOwner` is a separate administrative assertion.
Before using it, the host must verify that the actual worker has stopped using
authoritative process exit/wait or coordinator evidence, stop its renewal loop,
and prevent a replacement worker from using that ownership. An expected fence,
a timeout, a paused checkpoint or an owner string does not establish process
death. Crabber checks the exact expected owner and token atomically; it cannot
verify external process death. This authority may revoke an unexpired lease for
eligible legacy work. Neither process death nor lease expiry establishes an
uncertain external result or proves native resource cleanup. Native work requires
explicit reconciliation; legacy abandonment cannot replace that outcome or
release unresolved cleanup.

MemoryStore rotates the claim token before settlement inside a rollback-safe
transaction. PostgreSQL provides the same contract in one transaction, taking
the session gate then the run row before reading its clock or checking ownership;
renewal and recovery claims use that same order. Every Pending or Running tool call becomes Interrupted with one
matching result message and ToolCallSettled event. The run becomes Interrupted
with one durable RunSettled event. All old ExecutionStore writes are fenced out.
Completed tool records/results, history, usage, checkpoint, admission receipts
and unrelated steer/follow-up inbox rows are retained. The session can admit
subsequent work, which may claim the retained inbox rows.

Retain and retry the identical request after an unknown response. Success returns
the same saved run snapshot, terminal event and interrupted tool IDs on replay;
no messages/events are duplicated. Do not resume execution to retry abandonment.
An error is not proof of successful settlement. `NotFound` identifies a missing
run, and `AlreadyTerminal` identifies a run settled by another operation. A
different request against a previously abandoned run returns `StaleOwner`.
Memory state is process-local, so its replay evidence survives only as long as
the shared store state. PostgreSQL replay survives fresh connections and host
processes through a private abandonment commit and its terminal event. Schema
version 4 adds the commit table in an idempotent forward migration preserving
sessions, receipts and arbitrary event history. Event payloads alone never
authorize replay; the migration does not backfill caller-authored markers. Custom
stores return `Unsupported` until they implement this complete atomic contract.

Memory verification is in facade `fenced_abandon` and session
`memory::abandon_tests`: Running/Paused zero-effect journeys, live/stale denial,
all old-owner writes rejected, privately seeded Pending settlement, rollback
after tool writes, preserved usage/history/completed tools/inbox and exact replay.


Shared session `abandonment_contract` checks Pending/Running/Paused with both
expiry and administrative authority, the exact expiry boundary, all stale writes,
completed/failed tool history, usage, checkpoint, admission receipts, inbox rows,
next admission and exact replay. PostgreSQL `abandon_tests` adds independent-pool
renewal/recovery/abandonment races, transaction rollback before commit, unknown
committed-response reconciliation, fresh-process readback/retry and a subprocess
assertion that required-service tests reject a missing database URL.


## Durable unstarted admission execution

`SqliteStore` provides the same retained evidence and fenced-begin contract
(`sqlite_admission_execution_contract`).

Keyed runtime admission atomically retains a version-1, redacted-Debug execution
capsule with the receipt, run, original user message and epoch. It includes original
request/configuration/plan and opaque behavior identity, never credentials or
transport context. Receipt JSON/Debug and public lookup remain prompt-free.

`Agent::recover_admission` restores an expired, provably Unstarted admission using
exact original semantics. `load_admission_execution` is a sensitive adapter read;
`claim_unstarted_admission` validates observed owner/fence and immutable evidence
under the run lock, rotates the token and leaves Unstarted intact. One-shot
`begin_admission_execution` checks the live fence and commits Started before
execution effects. Unknown claim/begin acknowledgements never authorize work.
All effect-bearing ExecutionStore writes deny Unstarted, while lease maintenance
remains allowed. Generic `claim_expired_run` returns `AdmissionRecoveryRequired`;
explicit abandonment can terminally settle it only when there is no native
attempt/phase evidence or active session approval barrier. Started and missing
evidence retain conservative recovery semantics. A replay never creates or upgrades evidence.

Custom adapters default to typed Unsupported until they implement the complete
load/claim/begin contract. `KeyedAdmitRequest::execution = None` supports legacy
safety-only direct admissions; Some is validated before any write or replay. Store
trait source changes require updating struct literals and forwarding wrappers.
Memory state survives host recreation only while the shared store exists.

Schema 5 adds retained private execution records without backfilling legacy runs.
Stop writers, back up, migrate explicitly and deploy the matching binary. Connect
is read-only and rejects other schemas; mixed v4/v5 writers are unsupported.
Rollback requires backup restore or forward repair. No receipt/capsule pruning,
TTL or retained-key reuse is introduced. See the [full host contract](../../docs/admission-receipts.md)
for eligibility classifications, original behavior restoration and process proofs.


### Experimental atomic binding admission

`admit_bound_run` and `admit_bound_keyed_run` commit immutable provider-binding
records with the original message/run and, when keyed, receipt/Unstarted capsule.
Never implement this by calling legacy admission and then writing the binding.
The bundled stores reuse their transaction-only admission helpers. PostgreSQL
uses the session gate first; SQLite explicitly completes its write transaction;
Memory publishes its cloned state only after all writes succeed.

`ExecutionBindingCapsule` version 3 retains the original validated `InputEnvelope`
with binding evidence; legacy version 2 remains metadata-only for the runtime.
New keyed execution capsules and canonical semantic digests use version 2. Their
configuration must contain exactly one `PROVIDER_BINDING_COMPONENT` whose version
encodes the complete descriptor and one `HOST_BEHAVIOR_COMPONENT` whose version is
the declared opaque behavior fingerprint. The store hashes the complete runtime
semantics and checks original input, selected route, limits, configuration and
behavior rather than trusting the caller's claimed fingerprint. Credentials and
transport correlation do not belong in these declarations. The runtime uses
this capability for typed and text-adapted ordinary/keyed admission and recovery.
`AdmitRequest.expected_revision` is checked under the same session lock, after
identity and exact receipt lookup; stale new admissions change no input or owner.
The typed envelope includes that revision in its canonical semantics. The legacy
raw text guard remains a precondition separately from its v1 digest.

All ordered message records commit with admission. Supplied IDs are preserved,
generated IDs never change the typed digest, and every original record is checked
on scoped private reads. Model-input media has an explicit part kind, separate
from custom display activity; it is never removed by the conversation projector.
`typed_admission_contract::run_contract` qualifies this capability, including
context/state JSON that requires lossless TEXT rather than PostgreSQL JSONB.
PostgreSQL schema 7 adds that private text carrier; SQLite schema 3 is initialized
only on fresh databases, and existing SQLite versions fail closed as before.

Custom adapters can use `KeyedAdmitRequest::validated_digest` before taking locks,
`ExecutionBindingCapsule::validate`/`validate_keyed`, and the bounded
`RetainedExecutionBinding` codec and row validator. Recheck persisted identity and
receipt under the session lock. Replay compares the original binding, even after
Started, terminal settlement or lease changes, and never inserts missing evidence
or grants another fence. Missing/inconsistent binding evidence returns a conflict;
legacy receipt lookup still returns metadata. Default methods return
`HostUnsupported` rather than silently dropping the binding.

`execution_binding(session, run)` is a scoped private read, not a public snapshot.
It checks immutable run fields and the original committed input in a coherent
read transaction. The SQL projection omits private worker tokens and bounds its
payload before decoding. Both writers and readers cap private encoded records at
16 MiB; configuration nesting is capped at 64. Debug output is redacted. The
public `execution_binding_contract::run_contract` qualifies adapters; bundled
fault/process tests cover rollback, lost acknowledgement and killed writers for
both keyed and unkeyed admission, without dispatching a provider or tool.

### Experimental uncertain phase resolution

`Store::resolve_uncertain_phase(PhaseResolution)` targets an exact session, run,
call, phase and generation at the captured session revision. The request embeds
`PhaseCommand::ResolveUncertain`, including the host's asserted resolver identity
and authority claim. The library records that assertion; the host verifies it.
Success requires a supplied non-error `ToolOutput`. Failure records the supplied
output as an error, or an empty error when omitted. Refusal leaves the phase and
logical call uncertain. Supplied output must already satisfy host protection
policy: resolution never executes tools or reruns result middleware.

Memory, PostgreSQL and SQLite commit the native outcome, canonical logical tool
result/message/source event when applicable, audit, revision and receipt in one
transaction. They reconcile the original operation key before fresh eligibility
checks. Identical retries return metadata, including after lease expiry or later
cleanup; changed scope, choice, output or authority claim conflicts. Generated
message/part/turn IDs and timing do not change retry semantics. Retain the entire
original request when an acknowledgement is lost. The shared operation-key
namespace distinguishes this compound control from historical metadata writes;
fresh metadata-only `ResolveUncertain` requests are rejected.

This host control requires no worker fence and returns no execution authority.
It accepts only an already uncertain exact phase and preserves its current
quiescence, parent attempt, run ownership and approval barrier. A known outcome
with incomplete cleanup remains incomplete; resolution neither releases retained
resources nor resumes or retires the run. `PhaseResolution::fingerprint`,
`materialize` and `verify_replay` support custom transactional adapters, whose
default Store method remains explicitly Unsupported. Receipt verification also
requires its persisted audit event. No schema migration is introduced.
Session management is a pure Store/Agent control path: `manage_session` creates an
empty session under a host-generated stable SessionId and ControlId, or renames,
changes selected defaults, or reversibly archives one using SessionRevision CAS.
Create binds workspace/directory permanently. Exact retries replay the original
summary before current revision/visibility checks; changed semantics conflict.
Reconcile unknown outcomes with the original operation key and complete request.
An absent lookup while a request may be in flight is not proof of rejection.

`session_summary` and workspace-filtered `list_sessions` expose bounded display
metadata and revisions, without runtime fences, leases, checkpoints or outputs.
Lists use session ID keysets, with limits 1–100 and cursors bound to workspace
and archive visibility. Pages reflect current committed rows, rather than a
frozen multi-page snapshot; use the separate snapshot contract for history.
Sessions created by ordinary admission lazily read as unarchived with empty
selected defaults, without migration rewrites.

Archive changes visibility and rejects fresh ordinary admissions under the same
session transaction. It permits exact admission retries and existing approval,
continuation and reconciliation controls; it does not cancel work. Selected
provider/model identifiers are persisted host preferences. The current runtime
still uses the explicit immutable admission selection; hosts must resolve these
preferences into that selection themselves. Automatic selection of stored
preferences, deletion/tombstones, safe forks and an aggregate coherent public
management snapshot remain outstanding parts of WP5.

PostgreSQL schema 9 adds session metadata and session-only audit records, sharing
ControlId uniqueness with all other controls. Management audit never fabricates
a RunId. PostgreSQL migration is additive/idempotent and connect remains read
only. SQLite schema 5 creates this representation only on fresh files and refuses
older files; its schema assembly replaces the still-empty legacy receipt table
so management receipts may reference their separate session audit. Existing run
control receipts retain their mandatory run-event relationship.


### Waiting approval cancellation and expiry

`Store::terminate_approval` and its Runtime/Agent delegates accept an exact
session/run/continuation generation, revision, operation key and host resolver
claim. The host authenticates administrative authority separately from permission
to execute. These pure controls remain available after extension shutdown.
Cancellation selects `Interrupted`; expiry selects `Failed` and requires a named,
unanswered interaction whose Store-clock deadline has passed. Accepted answers
remain recorded and unconsumed. Other unanswered questions become expired only
when their own deadlines passed; otherwise they become cancelled.

Fresh approval reads and controls authenticate continuation creation time against
the original suspension audit and source turn/time against the committed native
pause. Coherent private changes to creation time and question deadlines cannot
extend or shorten the decision window. Generated presentation fields may differ
on an original-key retry; its committed receipt still replays as metadata.

Only a waiting batch whose latest suspended attempt has fully joined is eligible.
Adapters verify the frozen suspension and answer receipts, pending call bindings,
absence of native phases for prepared calls, and every historical phase's
terminal output and complete cleanup. The transaction writes a fixed empty error
result for each prepared call in original order, terminal source events, run
outcome and revoked fence, barrier release, audit and receipt. It invokes no tool,
provider or middleware and does not invent a native execution outcome. Claimed,
started and uncertain work require their separate ownership controls.

Retain the entire original request when acknowledgement is lost. Exact retries
replay metadata before current eligibility checks, including after another run
is admitted. Changed scope, choice or authority conflicts. Memory, PostgreSQL
and SQLite support the same atomic contract; custom Store defaults return
`Unsupported`. No schema migration is added by this control.

Parent reconciliation uses two keyed host controls. `mark_attempt_uncertain`
revokes the exact latest Started attempt and run authority, preserving every
child outcome and cleanup state. Expiry/revocation permits detecting uncertainty;
it never proves that an external resource stopped. A host-stopped assertion is
an explicit authenticated-host obligation, bound to the exact attempt target.

After independently reconciling all effects and cleanup, `retire_attempt` selects
Failed or Interrupted for the latest OutcomeUnknown attempt and logical run.
The host must assert that the whole matching worker and attempt-owned resources
actually joined. Every run phase must have a known terminal outcome and Complete
quiescence, with matching logical results and immutable call bindings. Pending
calls block retirement unless `CancelNeverStarted` explicitly selects fixed
Interrupted results for bounded calls that never owned any phase. Running,
Claimed, Started, Suspended and Uncertain effects are never bulk-interrupted.

Retirement preserves usage and binding, closes any durably identified open turn
once, and commits result/source records, terminal run, attempt cleanup, eligible
claimed-barrier release and audit/receipt atomically. Waiting approvals require
their dedicated termination control. Both operations return metadata only and
remain available after extension close, without factories, providers or tools.
Retain the original key and request after an unknown acknowledgement; exact
retries and `lookup_attempt_mutation` reconcile without restoring authority.
These controls do not implement durable cancel requests or orderly host close.
