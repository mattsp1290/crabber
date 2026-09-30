# Crabber session stores

The `postgres` feature uses `sqlx` with Tokio, rustls, and a PostgreSQL pool. It targets PostgreSQL 14 or newer and requires a dedicated database. Migrations create the Crabber tables in the default schema, so do not run them in a shared application schema.

The host calls `PostgresStore::migrate(url)` before `PostgresStore::connect(url)`. Connect verifies the schema version without changing it. The default build does not include the PostgreSQL driver.

Set `CRABBER_TEST_POSTGRES_URL` to run the live contract tests. Set `CRABBER_REQUIRE_POSTGRES=1` to fail if that URL is missing.

## Keyed admission (MemoryStore and PostgreSQL)

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
Per-session transaction locks serialize keyed and unkeyed admission. Receipt,
run, epoch and user message commit together. Lookup is read-only; replay never
changes lease ownership. PostgreSQL receipts survive pool and process restarts.

### Version 1 fingerprint contract

The host supplies a lowercase 64-character SHA-256 claimed fingerprint of its
canonical semantic input envelope. Keys accept 1–128 ASCII letters, digits, dash,
underscore and dot. Both constructors and deserialization validate these bounds,
with redacted errors and Debug. Validation does not imply the host-provided digest
is truthful: the library independently computes a separate semantic digest.

The runtime hashes structured JSON with domain `crabber.runtime.admission.v1`:
provider and model selection, system prompt, execution mode/concurrency,
compaction ratio/tail count, turn limit, acquired tool schemas/metadata, ordered
prompt sections, restrictions, component identities, guard IDs and provider IDs/names.
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
alone therefore does **not** establish rejection. Do not classify rejection by
matching human-readable error strings. Unless an error has a documented definitive
semantic outcome, reconcile it as unknown using the original key and payload. See
[the complete host decision table](../../docs/admission-receipts.md).

## Fenced abandonment (MemoryStore)

`Agent::abandon(AbandonRequest { expected, expected_owner, authority })` calls the
store directly, without resolving providers, mounting extension plans or invoking
tools or lifecycle hooks. It handles persisted Pending, Running and Paused runs
even when their executable configuration is unavailable. Existing live
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
verify external process death. This authority may revoke an unexpired lease.

MemoryStore rotates the claim token before settlement inside a rollback-safe
transaction. PostgreSQL provides the same contract in one transaction, locking
the run row before reading its clock or checking ownership; renewal and recovery
claims serialize on that lock. Every Pending or Running tool call becomes Interrupted with one
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
version 3 adds the commit table in an idempotent forward migration preserving
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
