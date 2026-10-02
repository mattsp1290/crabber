# Bounded session snapshots for embeddings

A host with a large durable session can read `Store::snapshot` through
`crabber::session::Store`. `SnapshotRequest`, `SnapshotLimits`, `SnapshotOutcome`,
`SnapshotContinuation`, `SnapshotPage` and `SnapshotUsage` are also exported by
`crabber`. This seam reads all history independently of runtime prompt context.
Existing `list_messages` and `list_all_messages` remain unbounded and unchanged;
they are not a safe substitute for this bounded embedding read.

## Run the journey

Commit changes, then run from the invocation source checkout:

```sh
cargo run -p bounded-snapshot -- --memory
# CRABBER_TEST_POSTGRES_URL identifies a disposable dedicated PostgreSQL 14+ database.
CRABBER_REQUIRE_POSTGRES=1 cargo run -p bounded-snapshot --features postgres -- --postgres
```

The Memory mode needs no credentials or provider. PostgreSQL uses fake data and
requires only the dedicated database. The example explicitly migrates that database,
opens independent reader/writer pools and starts a new child process that connects
without migrating. Avoid pointing test/demo commands at an application database.
The facade and allocation tests use the same journey source as the executable.

Output records the full invocation checkout Git SHA after checking that its tree is
clean (reported as `source_clean=true`), backend, host-assigned session identity, page number, immutable H, record
counts, exact text/encoded usage and post-H event IDs. It prints no message payloads,
continuations, DSNs, credentials or filesystem paths. Runtime Git resolution avoids
cached binaries embedding another worktree's `CARGO_MANIFEST_DIR`. Invoke through
Cargo in the intended checkout; a clean checkout is required to label evidence.
Ordinary facade tests and the xtask Memory demo use check mode (`--check`), which
permits uncommitted edits and reports `source_clean=false` honestly. Default user
demos still require a clean commit for publication evidence.
Session IDs are assigned once per invocation, with backend and process ID suffixes,
and remain stable across pages, writer pools and the child process.

## Host algorithm and record order

1. Set five per-page caps and request `continuation: None` for the stable session ID.
2. On `Page`, retain the original inclusive `high_water` H and verify it is unchanged
   on every subsequent page. Store or project returned records incrementally; retain
   IDs and links until the snapshot is complete. Pass its opaque continuation with
   the same session ID to read the next page. Limits can change between pages.
3. On `Limited`, the next indivisible record cannot fit an empty page. Retain H and
   retry at the same position using the returned continuation and larger permitted
   caps. No record was skipped. If the host cannot allow that size, stop explicitly.
4. On `Invalidated`, discard **all** collected pages and restart without a token.
   Never combine pages from different boundaries or consume an incomplete snapshot's
   event tail. Repeated mutation may require host retry/backoff policy.
5. Only when a `Page` has `continuation: None` is the snapshot complete. Then read
   `list_events(session, Some(H), batch_limit)`, advancing to the last returned cursor
   each time. It returns events **strictly greater than H**. Empty means no currently
   available tail events, not that the session can never append another event.

Messages are ordered by append order, then tool records by creation order, across
all runs and epochs including hidden context. Creation timestamps are not the sort
key. A call message, result message and tool record may span pages. Preserve message
IDs, `parent_id`, tool `call_id`, status and result content; assemble all pages before
presenting a settled relationship as complete. Pure appends stay outside captured
sequence cutoffs and become part of the event tail when corresponding durable events
are appended. Mutation of existing messages/calls (including `append_part`, call
claim or settlement) conservatively invalidates captured continuations. A bare
message append does not manufacture an event: writers are responsible for their
durable event contract.

## Limits and errors

`messages` and `tool_calls` cap complete returned records. `parts` counts only
`Message.parts`, not nested content blocks or tool-result blocks. `text_bytes` sums
UTF-8 bytes of every Text and Reasoning block, recursively through ToolResult content,
in both message and tool records. Settled results represented in both records count
twice. Other block types and tool arguments do not count toward text, but do count
in encoded bytes. `encoded_bytes` is the sum of compact serde JSON serialization of
each **complete** returned domain record, including IDs, metadata and opaque nested
values. Page envelopes and continuation bytes are excluded. Zero caps are allowed.
Backends preflight before fetching/decoding/cloning a non-fitting record and never
materialize a whole-history vector on this path. Caps bound returned data and client
record allocation, not CPU traversal time, SQL server memory or total host retention.
A host collecting every page in one vector would defeat bounded host memory.

`Limited` is normal flow; it is distinct from `Err(StoreError)`.
`NotFound` means no such session. `Validation` includes malformed/oversized,
wrong-session, unauthenticated or unsupported continuation input. Backend outages
are errors (PostgreSQL sanitizes database failures as `Validation`), never empty
success or `Limited`. `SnapshotUnsupported` means a custom Store has not implemented
bounded reads: fail closed or report unsupported, never fall back to unbounded reads.
Treat opaque tokens as untrusted and do not parse or modify them. Tokens are at most
2048 bytes; cap external request/envelope size separately.

Memory continuations are volatile, bound to that Store instance; clones share its
key and data. A replacement MemoryStore cannot resume them. PostgreSQL continuations
are authenticated with a durable database secret and work across independent pools
and fresh processes connected to the same migrated database. They bind session,
original H, revision, cutoffs and position. Preserve the database and its token secret
when maintaining durability; restoring a different database does not promise token
portability. Record mutations can invalidate a durable token even after a restart.

## Custom Store contract

The trait's default `snapshot` returns `SnapshotUnsupported`, preserving compatibility
for existing implementations. An implementing Store must capture H and record
cutoffs atomically with history, preserve deterministic ordering and record contents,
preflight all caps before materialization, and return exact usage. Continuations must
retain the original boundary and position, validate session binding and untrusted
input before allocation, and either freeze records or explicitly invalidate on their
mutation. Guarantee committed same-session event order: a later visible cursor must
not hide an uncommitted earlier cursor that the host could skip by advancing H.
Keep limit, invalidation and Store-error paths distinct. A delegating adapter must
delegate the entire contract, not synthesize H with a separate event query.

The separately compiled `testdata/external-consumer` host exercises an old custom
Store that inherits the fail-closed default and a custom Store delegating snapshots
to Memory. It runs limits/retry, immutable H, exact compact-byte usage, page completion
and strict event continuation using public exports with consumer feature unification.

## PostgreSQL maintenance

`PostgresStore::connect` checks schema version 4 and performs no migration writes.
Call `PostgresStore::migrate` explicitly during a maintenance window before opening
application traffic. The upgrade takes exclusive table locks and adds canonical
record storage alongside existing JSONB columns, increasing storage usage. It
backfills one record at a time rather than allocating whole history; upgrade time
still scales with existing data. Existing sessions, pending inbox and settled links
are preserved. New calls have persisted creation order. Legacy v1/v2 calls recover
pending-event sequence where available and use deterministic ID fallback when those
schemas never recorded creation order; original missing order cannot be reconstructed.
Retain schema and token-secret backups. Migration memory guarantees are separate
from ordinary bounded snapshot read guarantees.

## Request-to-assertion map: crabber-r-ef0t

| Requested behavior | Executable evidence |
| --- | --- |
| Public bounded query, no whole-history allocation | `bounded_snapshots` facade suite and demo: read-only allocation measurements over 512 KiB / 32 MiB histories; oversized first and next records rejected for text and encoded caps below 128 KiB client peak |
| Deterministic pages and settled relations | Shared `journey`: 1024 constructed messages; expected append IDs from fixture; exact nested UTF-8/compact JSON accounting; settled call/result parent links and status/content equality |
| Immutable H and gap/duplicate-free concurrent tail | Independent writer runs during a page probe; writer captures expected committed event IDs using bounded metadata; all pages retain H and exclude post-boundary messages; final paginated strict-after-H IDs/payload fixture indices agree exactly |
| Explicit limit/retry, invalidation and errors | Zero message cap returns Limited; same-position retry retains first record/H; mutation invalidates then restart; absent session and bad token remain errors; consumer default is SnapshotUnsupported |
| PostgreSQL transactionality/durability | Required facade test and PostgreSQL demo use separate pools; new child connects only, resumes authenticated token to completion and verifies original H/remaining IDs/tail; session backend suite additionally covers delayed commits and migration |
| Custom Store / external host | `external-host::bounded_snapshots::run`, compiled outside the workspace and run by `cargo xtask check`, covers fail-closed and delegated implementations |
| Required verification | `cargo xtask check` retains fmt/Clippy/workspace/WASM/external/glue gates and executes Memory demo; CI PostgreSQL job requires backend suite, public facade journey/fresh child and executable PostgreSQL demo |

Required PostgreSQL gates never silently skip when `CRABBER_REQUIRE_POSTGRES=1`:

```sh
CRABBER_REQUIRE_POSTGRES=1 cargo test -p crabber-session --features postgres
CRABBER_REQUIRE_POSTGRES=1 cargo test -p crabber --features postgres --test bounded_snapshots -- --nocapture
cargo xtask check
```

Set `CRABBER_TEST_POSTGRES_URL` in the environment for those required gates. Tests
run without provider credentials. HTTP/SSE, AG-UI projection, application FIFO/outbox
and consumer worker migration are host concerns outside this storage contract.
