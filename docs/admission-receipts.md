# Retained admission receipts

A receipt identifies one admitted turn. It has no execution authority and makes no
claim that the turn completed. PostgreSQL atomically commits the receipt, run,
user message, epoch and a private versioned execution capsule. Only a fresh `Admission::Started` returns a handle;
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

After response loss **before spawn**, exact retry and lookup return the original
receipt. Restore the original configuration and opaque behavior version, then call
`Agent::recover_admission(session, original_text, original_options)`. This explicit
operation verifies all original semantics before claiming an expired owner. A
successful claim returns `Started` for the **original run**, with its original
user message and receipt; consume the handle to establish actual completion.
`recover_admission_with_context` accepts fresh transport metadata.

| Recovery result | Host action |
| --- | --- |
| `Started { receipt, handle }` | Consume the handle and observe completed output. Ownership transfer alone does not establish completion. |
| `Replayed(receipt)` | Terminal or Started evidence grants no new execution authority. Observe durable state; ordinary resume/recover may conservatively settle expired Started work. |
| `AdmissionExecutionError::LiveLease` | Wait or observe the current owner. No ownership change occurred. |
| `StaleOwner`, `AlreadyStarted`, `AlreadyTerminal` | A race advanced ownership/state. Reconcile the original key; never replace the request. |
| `AdmissionConflict` / `SemanticConflict` | Restore the exact original input, reflected plan/configuration and opaque behavior version, or surface conflict. Verification also precedes terminal replay. |
| `MissingEvidence` / `Unsupported` | Legacy or adapter evidence cannot authorize fresh initial execution. Running status and checkpoint/event absence are insufficient. |
| `UnknownStoreFailure`, timeout, cancellation or transport loss | Spawn nothing. Reconcile the original key. A committed claim remains Unstarted until begin, and can be replaced only after its lease expires. |

Before the first execution hook, lifecycle notification, state sink callback,
provider resolution or provider/tool call, the executor commits a one-shot fenced
`Unstarted -> Started` marker. Repeating begin never grants a second permission.
A lost begin acknowledgement permits **zero execution** by that caller, even if
it committed. Started identifies uncertainty, not proof that an effect happened;
a crash there remains conservative and ordinary recovery may interrupt it.
There is no blind initial provider retry once Started. Paused tool continuation
retains existing retry-safety and fencing rules. External tool effects are not
exactly-once.

Plan acquisition and global extension initialization are pre-admission setup,
including on replay. They must not perform provider/tool business effects; the
execution boundary does not promise exactly-once arbitrary plugin initialization.
Generic `resume` returns `AdmissionRecoveryRequired` for Unstarted, and bulk
`recover` skips such records so their completion route remains available. Explicit
abandonment may settle them terminally when the host chooses not to execute.

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

Custom Store adapters must override `admit_keyed_run` and `lookup_admission`;
defaults fail closed. Validate message/part/session and immutable identity before
replay; independently compute `KeyedAdmitRequest::semantic_digest`; compare both
claimed fingerprint and semantic digest; resolve replay/conflict before Busy.
Serialize admission for each session and commit receipt/run/message/epoch as one
transaction. Roll back failures before commit; preserve uncertainty when the commit
acknowledgement is lost. Keep replay/lookup read-only and never manufacture execution
authority. Recoverable adapters must also forward `load_admission_execution`,
`claim_unstarted_admission` and `ExecutionStore::begin_admission_execution`.
Defaults fail closed with typed Unsupported. `KeyedAdmitRequest::execution = Some`
is validated before replay/fresh insertion and committed atomically; `None`
retains safety-only direct admission and cannot be upgraded by replay.
Effect-bearing execution writes deny Unstarted; only begin and lease maintenance
may mutate its live execution ownership. PostgreSQL claim/begin/renew/writes and
abandonment serialize on the run row, without session-first lock inversion.
Retain private capsules and markers for the same session lifetime as receipts.
Forward the entire contract in wrappers. Run the shared
`admission_contract::run_contract` and `storetest::run_contract` for implementations.

## Request crabber-r-wgh9: assertion map

The original request from consumer `agentcraft-dhtf` asks for the following outcomes.
These are repository assertions and documented boundaries, not an Agentcraft migration.

| Request requirement | Evidence |
| --- | --- |
| Public host key + input fingerprint, atomic admit-or-return and lookup | Public facade `concurrent_and_terminal_retries_execute_once`; external-consumer fixture compiles/calls the receipt API with `serde_json/preserve_order`; shared store admission contract. |
| Concurrent identical starts: one run/user message and same receipt | Memory's 24 concurrent facade tasks; PostgreSQL `independent_pools_and_saturated_existing_session`; facade `postgres_public_facade_fault_restart_journey` starts two gated independent OS processes and compares their receipt JSON, SQL run/receipt counts and user-message count. |
| Different payload under same key explicitly conflicts | `stale_claim_cannot_hide_changed_inspectable_semantics` changes 12 payload/config/plan inputs; shared Memory/Postgres contract covers honest and stale fingerprints, session-local uniqueness and Busy ordering. |
| Response loss after commit reconciles by receipt after restart | Facade journey's `commit-loss` adapter calls the real PostgreSQL commit then discards the outcome before runtime sees it. The host process exits; new lookup/retry/recovery processes perform public lookup/exact retry. `postgres_unstarted_completion_and_uncertainty_journey` races fresh expired-owner
claimants with the winner gated after begin, completes the original run, and
asserts one receipt/run/user message/provider request, zero tools and actual
assistant output. Fresh-process AG-UI replay reconstructs live-only presentation
boundaries and text from the actual history row at each persisted MessageCommitted
event, then projects real settlement: original text, original thread/run
correlation and one completed RUN_FINISHED. |
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

## Schema 5 rollout and acceptance

Stop writers, back up the database, explicitly migrate, then deploy the matching
schema-5 binary. Connect verifies schema 5 read-only. Do not mix v4/v5 writers.
Migration is transactional and idempotent, retaining v1-v4 sessions, receipts,
history, usage, checkpoints, snapshots, inbox and abandonment evidence. It never
backfills Unstarted from legacy records. Rollback requires restoring the backup
or forward repair; an old binary cannot connect to v5.

Library proof includes shared Memory/Postgres execution contracts, live process
claim/start uncertainty and provider/tool death, delayed old-owner spawn fencing,
semantic drift and immutable receipt retention. Agentcraft consumer acceptance
is separately owned: pin the immutable implementing Crabber revision and run the
owner's supported native `backend:feasibility --json` proof against PostgreSQL.
Until that gate confirms original dispatch Completed, library completion alone
does not satisfy the full amended request.

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
