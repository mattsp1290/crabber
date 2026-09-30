# Retained admission receipts

A receipt identifies one admitted turn. It has no execution authority and makes no
claim that the turn completed. PostgreSQL atomically commits the receipt, run,
user message and epoch. Only a fresh `Admission::Started` returns a handle;
`Admission::Replayed` and `lookup_admission` never return one or renew a lease.
The receipt's run ID is its admission identity, retained unchanged after completion
and lease changes. Existing unkeyed `prompt` calls retain their previous behavior.

## Host algorithm

Before the first call, persist a stable `SessionId`, session-scoped `AdmissionKey`,
the original semantic input/configuration, and both fingerprints in the host's
outbox. Reuse them verbatim after uncertainty or restart. Persist the session ID
before first-session admission too; generating a fresh session would defeat the
key's uniqueness boundary. Serialize application dispatch as needed: FIFO and the
application database's outbox/dispatch transaction belong to the host. Crabber
does not provide a transaction spanning that database and its own store.

| Result | Host action |
| --- | --- |
| `Started { receipt, handle }` | Correlate the accepted outbox item by receipt. Consume this sole execution handle and observe its result. |
| `Replayed(receipt)` | Correlate the same accepted item; read durable events/run state. There is no new executor. |
| Lookup `Some(receipt)` | Acceptance is durable; exact retry can verify matching input. Lookup by itself does not validate a new payload. |
| `AdmissionConflict` | Same retained key has different input/behavior; surface the conflict. Never overwrite the original acceptance. |
| `SessionIdentityMismatch` | Correct the session/workspace/directory mismatch; a retry cannot change identity. |
| `Busy` | An absent/different key reached a currently active session. Retain the queued item's key/input and apply host dispatch policy. An identical retained key replays before Busy. |
| Lookup `None` | No committed receipt was visible at that instant. An original admission may still commit; retry the exact original request or continue reconciliation. |
| Timeout, cancellation, transport error, or other outcome without a documented rejection guarantee | Outcome is unknown. Lookup and/or exact retry, with backoff as appropriate. Never invent a replacement key to resolve uncertainty. |
| `AdmissionUnsupported` | The Store has not implemented the required transaction. Fix the adapter; never fall back to an unkeyed prompt. |

Do not classify rejection from error text. In particular PostgreSQL connection and
commit errors are sanitized as `StoreError::Validation("PostgreSQL operation failed")`.
Even this `Validation` variant can represent a committed admission whose reply was
lost. Only typed, documented semantic outcomes justify the corresponding table row;
a broad `Validation` match is not evidence that nothing committed.

After a committed admission loses its response **before execution is spawned**, an
exact retry returns only a receipt. Wait for lease expiry and use existing fenced
`Agent::resume` / recovery rules. The current runtime interrupts an expired,
non-paused run without rerunning its provider; it does not transparently finish the
original prompt. After response loss **while execution is live**, the original
worker may complete once. Observe it; receipt replay cannot start another worker.
Recovery of persisted tool calls follows existing retry-safety and fencing rules.
This is not an exactly-once guarantee for external tool effects.

## Fingerprints, retention and Store adapters

Construct the claimed `InputFingerprint` as SHA-256 of a canonical, versioned
semantic envelope: prompt, immutable identity, provider/model selection,
configuration and plan-affecting inputs. Exclude generated IDs, timestamps, lease
owner and request-attempt metadata. Use a mandatory `behavior_fingerprint` to
version opaque native callbacks, policy/approver bodies, resolver/provider routing,
middleware, services and other behavior the runtime cannot inspect. Changing that
behavior must change this fingerprint. The host must retain the original version
for exact retries; silently applying a new deployment's behavior is not equivalent.

The library independently hashes inspectable semantics and rejects changed payloads
even when the host reuses stale claimed hashes. It cannot detect a host lying about
opaque code. JSON objects are sorted recursively; array order remains meaningful.
See the [versioned fingerprint contract](../crates/crabber-session/README.md#version-1-fingerprint-contract)
for the precise fields and domain separators.

Keys are unique within a session for the **entire retained session lifetime**,
including terminal, expired and recovered runs. No TTL, automatic pruning or key
reuse exists. MemoryStore durability ends with its shared store lifetime;
PostgreSQL survives process restarts. Use opaque correlation IDs, not secrets:
receipt metadata contains IDs and fingerprints, never prompt/history, private
paths, claim tokens, checkpoints or execution fences.

Custom Store adapters must override both `admit_keyed_run` and `lookup_admission`;
defaults fail closed. Validate message/part/session and immutable identity before
replay; independently compute `KeyedAdmitRequest::semantic_digest`; compare both
claimed fingerprint and semantic digest; resolve replay/conflict before Busy.
Serialize admission for each session and commit receipt/run/message/epoch as one
transaction. Roll back failures before commit; preserve uncertainty when the commit
acknowledgement is lost. Keep replay/lookup read-only and never manufacture execution
authority. Forward both methods in wrappers. Run the shared
`admission_contract::run_contract` and `storetest::run_contract` for implementations.

## Request crabber-r-wgh9: assertion map

The original request from consumer `agentcraft-dhtf` asks for the following outcomes.
These are repository assertions and documented boundaries, not an Agentcraft migration.

| Request requirement | Evidence |
| --- | --- |
| Public host key + input fingerprint, atomic admit-or-return and lookup | Public facade `concurrent_and_terminal_retries_execute_once`; external-consumer fixture compiles/calls the receipt API with `serde_json/preserve_order`; shared store admission contract. |
| Concurrent identical starts: one run/user message and same receipt | Memory's 24 concurrent facade tasks; PostgreSQL `independent_pools_and_saturated_existing_session`; facade `postgres_public_facade_fault_restart_journey` starts two gated independent OS processes and compares their receipt JSON, SQL run/receipt counts and user-message count. |
| Different payload under same key explicitly conflicts | `stale_claim_cannot_hide_changed_inspectable_semantics` changes 12 payload/config/plan inputs; shared Memory/Postgres contract covers honest and stale fingerprints, session-local uniqueness and Busy ordering. |
| Response loss after commit reconciles by receipt after restart | Facade journey's `commit-loss` adapter calls the real PostgreSQL commit then discards the outcome before runtime sees it. The host process exits; new `reconcile`/`recover` processes perform public lookup/exact retry. Original receipt IDs agree, SQL counts remain one, provider/tool ledger counts remain zero. |
| Preserve fencing and immutable identity | Facade `identity_and_unkeyed_compatibility`; shared store contracts; expired-owner facade recovery asserts old-fence rejection and immutable receipt. Replay compares durable run state before/after, including ownership/lease. |
| Memory/Postgres parity and restart tests | Shared `admission_contract::run_contract`; store `independent_processes_and_fresh_process_lifecycle`; facade process journey and both noninteractive demo modes. |
| Safe API excludes raw prompt data | Facade `metadata_validation_and_redaction`; shared contract receipt JSON/Debug redaction assertions and safe receipt DTO. Demo output contains only correlation metadata/counts. |
| Retention, uniqueness, ambiguous timeout semantics | Host table and retention rules above; delayed original call has an absent lookup in a fresh process and later commits exactly once; terminal retry replays unchanged. |
| FIFO stays app-owned; no post-hoc mapping substitute | Host algorithm above; Store transaction includes receipt itself, never a separate post-hoc mapping. No cross-database transaction or external-effect exactly-once claim. |

The same facade journey also injects a pre-admission failure (fresh process finds
no session/receipt), and withholds a public host reply after provider execution
begins. A new host reconciles while the original worker is gated, then after it
finishes; durable ledger records **two** provider requests (tool request plus final
answer) and **one** controlled tool effect, with no additions from replay. The
text-only demos use one provider request and no tools. Counts are measured, not
inferred from successful receipt lookup. The store's
`process_crash_before_commit_rolls_back_every_record` separately kills a transaction
before commit and asserts zero partially admitted records.

## Reproduce

Set `CRABBER_TEST_POSTGRES_URL` to a disposable dedicated PostgreSQL 14+ database;
no real provider credentials are used. Required mode fails if the URL is absent.

```sh
cargo xtask check
CRABBER_REQUIRE_POSTGRES=1 cargo test -p crabber-session --features postgres
CRABBER_REQUIRE_POSTGRES=1 cargo test -p crabber --features postgres --test admission_receipts -- --nocapture
cargo run -p admission-receipt -- --memory
cargo run -p admission-receipt --features postgres -- --postgres
```

`xtask check` runs the memory demo and workspace tests (including stdout tests that
execute the existing unkeyed examples), builds workspace targets, runs the
external-consumer check and enforces the minimal-embed glue limit. CI's PostgreSQL job requires the store suite,
facade fault/restart journey and PostgreSQL demo. The migration test
`forward_migration_preserves_v1_and_connect_is_read_only` verifies retained baseline
data and idempotent forward migration. Skipped live tests do not establish acceptance.
Run from a clean committed checkout to bind printed full source SHA to the tested
source; save the command logs with that revision. Normal/default library builds do
not include a PostgreSQL driver; the facade tests use SQLx as a dev dependency to
measure durable run/receipt counts independently of receipt lookup.
