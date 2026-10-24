# SQLite session store

## When to use it

Use SQLite for a single-operator host that needs durable sessions without a
service. `sqlite_interrupt_then_fresh_process_continues_session` proves a session
continues after the host exits. Use `MemoryStore` for volatile tests and PostgreSQL
for deployments spanning machines. Multiple processes on one machine can share
SQLite safely (`multi_process_writers_serialize_and_keep_fences`); this is still
one local database with one writer at a time.

## Enable

Enable Cargo feature `sqlite` on `crabber`, `crabber-session`, or
`crabber-runtime`. The public store is `crabber_session::SqliteStore`, also
available as `crabber::session::SqliteStore`. It exposes `migrate`, `connect`,
`with_clock` and `with_limits`; `crabber::AgentBuilder::sqlite(path)` connects
without migrating (`sqlite_builder_requires_migration_and_persists_a_completed_prompt`).

The SQLx dependency builds bundled SQLite and needs a C compiler. Cargo allows
one dependency linking `sqlite3`; hosts using `rusqlite` must choose a version
using `libsqlite3-sys 0.30`, or use SQLx for their own tables in a **separate**
database. The pinned build is recorded in [Cargo.lock](../Cargo.lock) and feature
wiring in the [session manifest](../crates/crabber-session/Cargo.toml).

## Lifecycle

The host creates the parent directory, then calls `SqliteStore::migrate(path)`
before `SqliteStore::connect(path)`. Current storage versions are PostgreSQL 24
and SQLite 20; the authoritative
constants are `POSTGRES_SCHEMA_VERSION` and `SQLITE_SCHEMA_VERSION` in
[crabber-session](../crates/crabber-session/src/lib.rs).
SQLite initialization is fresh-only: archive or export needed history with the
matching old binary, then create a new current database. There is no in-place
upgrade from earlier SQLite schemas. Keep an untouched backup and its matching
binary when rolling back; opening a current file with an older binary is not an
upgrade or rollback mechanism. Migration owns file creation, schema changes
and enabling WAL. Connect verifies the application identity, schema version 20
with its complete numbered history and WAL mode; it never creates a file or changes schema or journal mode.

| Input | `migrate(path)` | `connect(path)` | Executable proof in `sqlite::lifecycle_tests` |
| --- | --- | --- | --- |
| Missing file, existing parent | Creates version-20 WAL database | Refuses; creates nothing | `migrate_creates_current_version_wal_database`, `connect_never_creates_a_file` |
| Fresh empty SQLite file | Initializes version 20, WAL and signing secret | Refuses until migrated | `interrupted_migration_leaves_a_fresh_database` |
| Crabber version 20, WAL | Idempotent; preserves data and secret | Opens without schema writes | `migrate_is_idempotent_and_keeps_the_snapshot_secret`, `connect_is_read_only` |
| Prior version 1–19 | Refuses without upgrading or changing journal mode | Refuses | `version_one_files_are_refused_without_migration_or_journal_changes`, `version_two_files_are_refused_without_migration_or_journal_changes` |
| Unknown version or malformed version metadata | Refuses without changing logical content or journal mode | Refuses without changing logical content | `unknown_schema_version_is_refused_without_changes`, `malformed_version_metadata_is_refused_without_changes`, `unknown_schema_in_a_backup_is_refused_before_enabling_wal` |
| Foreign tables/application ID, or non-SQLite input | Refuses without rewriting content | Refuses without rewriting content | `foreign_database_is_refused_without_changes`, `foreign_application_id_is_refused_without_changes`, `non_sqlite_inputs_are_refused_without_changes` |
| Version-20 restore in non-WAL mode | Re-enables WAL without regenerating the secret | Refuses; asks host to migrate | `restored_backup_requires_migrate_then_connects` |

A WAL checkpoint can move committed frames into the main file without changing
logical records. Refusal tests compare logical database content rather than
assuming byte-identical WAL layouts. `concurrent_migrate_initializes_once` and
`classification_snapshot_survives_concurrent_initialization` cover concurrent
startup; `migrate_waits_for_a_journal_mode_lock` covers WAL-mode lock retry.

## Files and permissions

Protect the database directory for the service user: for example Unix mode
`0700`, with umask `0077` or a pre-created empty database file at mode `0600`.
The host owns this policy. The store does not chmod files; creation follows the
process/SQLite filesystem policy. Protect the main `<path>` file and its
`<path>-wal` and `<path>-shm` side files, plus every backup. On Unix SQLite attempts to give
side files the main file's permissions; verify actual modes in the host environment.
This is upstream SQLite behavior, not a repository permission test. See the
[SQLite Unix VFS](https://github.com/sqlite/sqlite/blob/6e35c19f170958b1e846e01c55318988f2d192e0/src/os_unix.c#L6428).

Session prompts, tool results and the snapshot signing secret are unencrypted.
Backups retain the secret: protect them as database contents. The
[schema](../crates/crabber-session/migrations/sqlite/0001_initial.sql) defines the
stored records; `migrate_is_idempotent_and_keeps_the_snapshot_secret` verifies
secret retention. File access also grants access to authentication material for
snapshot continuations; do not distribute copies to untrusted readers.

Use local filesystems only; network filesystems are unsupported. SQLite WAL
requires processes to share the same host (see [SQLite WAL](https://www.sqlite.org/wal.html)).
Use one dedicated database per host application; do not create host tables in it.
`foreign_database_is_refused_without_changes` proves migration rejects a foreign
file. After migration, the file is host-protected storage, not an authorization
boundary against a process that can modify it directly.

## Concurrency and unknown outcomes

Each store clone shares one writer connection and four query-only readers.
Every write transaction takes `BEGIN IMMEDIATE`, serializing writers for the
whole file across independent stores and processes. Readers can progress while a
writer holds the lock. Proofs: `reader_pool_rejects_writes`,
`write_transactions_take_the_lock_at_begin`, `migrate_reopen_and_serialized_writers`
and `multi_process_writers_serialize_and_keep_fences`.

SQLite's busy timeout is five seconds. This bounds waiting on a database lock,
not the total operation: waiting for the single writer pool connection has a
separate default SQLx acquisition timeout (30 seconds). Constants/options are in
[sqlite.rs](../crates/crabber-session/src/sqlite.rs). The shortened-timeout test
`busy_timeout_surfaces_as_sanitized_unknown_outcome` proves lock exhaustion maps
to `StoreError::Validation("SQLite operation failed")`; the production five-second
value is fixed, with no tuning API.

Treat database/transport/commit failures as unknown outcomes, not definitive
rejections. Reconcile keyed admission with the original session/key/input; retry
abandonment with the exact saved owner, fence and authority. Never invent a new
key or rotate the saved request to resolve uncertainty. Typed semantic denials
retain their meaning. `sqlite_public_facade_fault_restart_journey` and
`rollback_unknown_response_and_fresh_process_replay` cover response loss and
fresh-process reconciliation. See [admission receipts](admission-receipts.md) and
[fenced abandonment](fenced-abandon.md).

## Durability

Connections use WAL, foreign keys on and `synchronous=FULL`
(`migrate_creates_current_version_wal_database`). Committed history survives process
kill; writes killed before commit leave no partial records
(`independent_processes_and_fresh_process_lifecycle`,
`process_crash_before_commit_rolls_back_every_record`,
`collation_crash_barrier_rolls_back`, `public_cross_join_crash_barrier_rolls_back`).
These are process-crash proofs, not physical power-loss tests. Storage hardware
must honor synchronization for power-loss durability; see
[SQLite synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous).

Authenticated snapshot tokens survive a fresh process connected to the same
file (`continuation_survives_fresh_process`). Copying/restoring that database
preserves its secret; mutations can still invalidate an existing boundary
(`public_message_claim_and_settlement_mutations_invalidate_authenticated_tokens`).
Snapshot caps limit returned records and client allocations, not total database
work or retained host history. The allocation test
`large_sqlite_history_keeps_client_allocation_bounded` measures the calling thread;
it does not count SQLx SQLite worker-thread buffers.
`snapshot_reads_only_records_inside_the_page` is the damaged-record proof that
out-of-page records are not decoded.

## Backup and restore

**Offline:** stop every host/reader/writer using the database. Copy the main file
and any existing `-wal` and `-shm` side files together while it is quiescent.
Do not copy the main file alone while the host runs: committed data can still be
in the WAL ([SQLite WAL](https://www.sqlite.org/wal.html)). Clean shutdown may
already have checkpointed and removed side files; absent files need no copy.
The offline copy procedure is host guidance, not a separate repository test.

**Online:** from another connection/process, run `VACUUM INTO '<dest>'` to a new
backup file while writers continue. `online_backup_during_writes_is_consistent`
executes this operation against an active writer, checks integrity and exact
history prefix, and exercises restore. Protect and durably retain the completed
backup before relying on it. See [SQLite VACUUM INTO](https://www.sqlite.org/lang_vacuum.html).
The SQLite shell's `.backup` is another upstream option, but the repository does
not exercise it; see [SQLite backup](https://www.sqlite.org/backup.html).

**Restore:** stop every user of the target database, replace the main file with
its consistent backup, and remove stale target `-wal`/`-shm` files before reopening.
For an offline set, retain the backup's matching WAL together with its main file;
never mix those files with a different target's side files. Call `migrate` to
re-enable WAL, then `connect`. A `VACUUM INTO` copy is not in WAL mode:
`restored_backup_requires_migrate_then_connects` proves connect refuses it until
migration; `online_backup_during_writes_is_consistent` restores after deleting
stale target side files and verifies retained history.

## Schema versions and rollback

Historical introduction: SQLite schema 2 and PostgreSQL schema 6 added the
durable-host foundation:
public revisions/state, immutable execution-binding storage, attempt/phase owners,
operation receipts and independently owned session invocations. These tables do
not by themselves deliver the still-experimental host/control APIs.

Current fresh SQLite databases apply the complete migration assembly through
`SQLITE_SCHEMA_VERSION` (20), including those foundation tables. Connect requires
the complete numbered version history. Every older file is refused before schema
or journal changes; there is no in-place upgrade. Finish or
settle paused work with its matching binary, back up/export it, and explicitly
create a fresh dedicated database. No operation automatically deletes data.
Rollback requires the matching binary and database backup; no down migration is
provided.

## Limits

No tuning API for timeout, pool size or synchronous mode; no in-memory SQLite
mode, cross-store migration, host-table hosting or public backup helper.
`MemoryStore` covers volatile use. `with_clock` injects lease time and
`with_limits` sets record validation limits; these are the only extra store
configuration methods ([public implementation](../crates/crabber-session/src/sqlite.rs)).
Standalone pinned probes under `testdata/` have no SQLite variant by explicit
scope decision; the in-workspace probe does.

## Request-to-assertion map: crabber-r-8bdu

| Request or acceptance item | Executable evidence |
| --- | --- |
| Full Store/ExecutionStore parity: records, identity, leases, admission, Unstarted/fenced begin, abandonment, snapshots | Session `sqlite_contract`, `sqlite_workspace_identity_contract`, `sqlite_storable_record_contract`, `sqlite_admission_contract`, `sqlite_admission_execution_contract`, `shared_abandonment_contract`, `untrusted_terminal_markers_are_never_replay_authority`; facade admission, snapshot and abandonment suites below |
| Explicit lifecycle, WAL, busy timeout, foreign keys, single writer | Lifecycle table; `write_transactions_take_the_lock_at_begin`, `migrate_reopen_and_serialized_writers`, `busy_timeout_surfaces_as_sanitized_unknown_outcome` |
| Existing conformance suite on SQLite | The seven contract entrypoints above invoke the six shared contracts and untrusted-terminal supplement; `cargo test -p crabber-session --features sqlite` |
| File protection/backup guidance; unknown schemas never deleted | Host duties and upstream references above; `unknown_schema_version_is_refused_without_changes`, `online_backup_during_writes_is_consistent`, `restored_backup_requires_migrate_then_connects` |
| Conformance and feature-aware quality gate | `cargo xtask check`: explicit SQLite-only lint/test commands, probe/restart tests and four SQLite demos, plus workspace/WASM/external/glue checks |
| Credential-free restart continuity | `sqlite_interrupt_then_fresh_process_continues_session`: Interrupted first process, Completed distinct run in second; probe `fresh_host_resumes_paused_run_with_the_persisted_workspace`; `sqlite_fresh_workers_preserve_receipts_sessions_and_cursors` |
| Completion response names merge SHA, feature and symbols | Tracking issue `crabber-yvd6`; completion response is pending the documentation PR's human merge, then reports its verified merge SHA, `sqlite`, `SqliteStore::{migrate, connect, with_clock, with_limits}` and `AgentBuilder::sqlite` |

The sole example-parity deviation is the excluded standalone pinned probes.
Consumer adoption in `crabber-channels` remains later, non-blocking consumer work.

## Reproduce

Run from the repository root without PostgreSQL variables. These commands use
fake providers, temporary SQLite databases and no credentials. The first command
builds the local WASM fixtures required by the WASM test.

```sh
cargo xtask check
cargo test -p crabber-session --features sqlite
cargo test -p crabber --features sqlite --test admission_receipts --test bounded_snapshots --test fenced_abandon_facade --test result_transform_interrupt --test facade_contracts
cargo test -p crabber --features datadog,sqlite --test trace_context
cargo test -p crabber --features wasm,sqlite --test wasm_roles
cargo test -p crabber-runtime --features sqlite
cargo test -p workspace-context-probe --features sqlite
cargo test -p minimal-embed sqlite_interrupt_then_fresh_process_continues_session
cargo run --quiet -p admission-receipt --features sqlite -- --sqlite
cargo run --quiet -p bounded-snapshot --features sqlite -- --sqlite --check
cargo run --quiet -p fenced-abandon --features sqlite -- --sqlite
cargo run --quiet -p host-trace --features sqlite -- sqlite
```

For the minimal executable's own persistent file, set `CRABBER_SQLITE_PATH` in a
private existing directory, run `cargo run -p minimal-embed -- --store sqlite
--interrupt-after-first-delta`, then invoke a second process with the same path
and `--resume <run-id>` (using the printed original ID). Only this example reads
that host path; other SQLite demos own isolated temporary directories. T6.1 also
checks the missing-variable invocation fails before creating any file. For source
identity evidence, rebuild after committing and run demos from that clean checkout.

Historical introduction: typed admission added private carriers in SQLite schema 3
and PostgreSQL schema 7; current databases require versions 20 and 24 respectively.
That change expanded private binding/keyed capsule carriers without making older
execution payloads runnable.
SQLite initializes only fresh databases; all older files remain unchanged and
must be settled/exported with their matching binary or retained while the host
creates a separate fresh database. Connect never migrates a file. PostgreSQL's
explicit migration preserves legacy payloads and adds a lossless TEXT carrier
for typed original context/state; runtime codec checks still fail closed.
