# Crabber session stores

The `postgres` feature uses `sqlx` with Tokio, rustls, and a PostgreSQL pool. It targets PostgreSQL 14 or newer and requires a dedicated database. Migrations create the Crabber tables in the default schema, so do not run them in a shared application schema.

The host calls `PostgresStore::migrate(url)` before `PostgresStore::connect(url)`. Connect verifies the schema version without changing it. The default build does not include the PostgreSQL driver.

Set `CRABBER_TEST_POSTGRES_URL` to run the live contract tests. Set `CRABBER_REQUIRE_POSTGRES=1` to fail if that URL is missing.

## Keyed admission (MemoryStore)

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
any failure must roll back all three. Existing unkeyed admission remains available.

Custom Store implementors must override both new methods transactionally and run
`admission_contract::run_contract`, alongside `storetest::run_contract`. The default
implementations return `AdmissionUnsupported`, so adapters cannot silently degrade
keyed requests to unkeyed starts. Forward both methods in wrappers. PostgreSQL keyed
admission currently returns this explicit unsupported error; durable receipt schema
and implementation are the next milestone slice. PostgreSQL's existing unkeyed path
and schema are unchanged in this slice.

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
