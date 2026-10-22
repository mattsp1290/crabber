# eino-tools parity

Version 0.2.0 established parity with the eleven tools from eino-tools `catalog.Standard` at
`c0975d6764b2536faf639efe516a79de6154a55d`. Parity means tool names, schemas and
retry_safe declarations modulo the exact allowlist, plus the runtime contracts
mapped below. Runtime corpus parity is proved for apply_patch and user_interact;
the other nine tools have contract mappings and Rust tests, not a ported corpus.
The [Rust contract](tool-contract.md) defines bounds and envelope omissions.

## Package map

| eino-tools package | Rust surface |
| --- | --- |
| result | crabber_tools_core::Outcome and envelopes |
| fileops | crabber-tools-fileops |
| glob | crabber-tools-glob |
| search | crabber-tools-search |
| applypatch | crabber-tools-applypatch |
| shell | crabber-tools-shell |
| urlfetch | crabber-tools-urlfetch |
| userinteract | crabber-tools-userinteract; SurfaceMCP → Pending, SurfaceCLI → Durable |
| tracker + tracker/beads | TrackerPolicy over the bn CLI; no writer trait |
| trackerwrite | crabber-tools-trackerwrite |
| catalog | crabber-tools-catalog: StandardTools, metadata(), EINO_PARITY |
| internal/jsoncompat | serde_json; duplicate top-level keys keep the last value |

## Deliberate deviations

The identifiers in this table are referenced by the contract matrix. These are
behavior differences, including metadata allowlist pointers, rather than claims
of exact runtime equivalence.

| Id | Area | Difference |
| --- | --- | --- |
| D1 | Catalog order | Rust retains the original six followed by glob, apply_patch, url_fetch, tracker_write, user_interact; Go has glob before search and tracker last. |
| D2 | Binding / Concurrent | All Rust tools mount beside an admitted root; static tools ignore it where appropriate. No per-tool Concurrent flag. The test asserts `mutating == (!retry_safe && binding == "workspace")`; only four workspace writers serialize. Go documents serialization of filesystem readers too. |
| D3 | Identity / platform | No per-tool ExecutorHash: whole-extension config_hash covers policies and root. Non-Unix compilation fails instead of ErrUnsupportedPlatform. |
| D4 | Policy construction | Go omits tracker without a writer and defaults user surface to MCP. Rust fails construction for enabled URL, tracker or interaction tools missing explicit policy. All supplied policies validate and hash even when disabled. |
| D5 | Capability / filesystem | Rust uses an admitted directory capability, rejects parent traversal and absolute symlink targets even inside root, and rechecks session workspace (`workspace_mismatch`). Hard links remain host concerns; replacing the root requires reopening it. Regular-file/nonblocking opens prevent FIFO hangs. Errors are sanitized. |
| D6 | Atomic writes | Synced temporary siblings preserve mode and conditional ownership; new files use 0644. Chown failure is io; unprivileged replacements retain host ownership when the original owner cannot be copied. A crash may leave a temp sibling. Started blocking operations may finish after cancellation with owned guards. |
| D7 | Cancellation / admission | Crabber interrupts/drops executor futures, returning no model-facing canceled category. Mount lock/capacity deadlines add unavailable; panics become unknown. Reads do not serialize with writers. Host policy rejection is an orchestration failure. |
| D8 | Process policy | Shell binary, startup mode, environment, run_as and output cap are explicit. Search resolves rg once, disables config and clears RIPGREP_CONFIG_PATH. Shell `/description` says host-supplied cap instead of Go's 256 KiB default. Five-second reap/pipe grace bounds waits; detached processes remain host concerns. |
| D9 | Search schema / output | search `/description` adds host policy; `/parameters/properties/limit/minimum` is 0, selecting default 200. Diagnostics are fixed/sanitized instead of retaining stderr. Retained serialized matches are byte-bounded, submatch offsets reference returned UTF-8 bytes, malformed input/overflow is bounded. Path validation followed by rg resolution retains an accepted symlink TOCTOU window. |
| D10 | Glob resource policy | Bounded directory batches and retained matches may rescan siblings. Pattern byte/brace/complexity bounds add validation failures. No ignore-file support (also absent upstream). Lossy display ties use raw bytes and remain separate entries. |
| D11 | Patch resource / commit policy | Additional 16 MiB per-file and 64 MiB aggregate caps yield too_large. Capability alias targets are checked for overlap; add/move installation is no-clobber. Commit failure partial=true is conservative even if no file was changed by the failing operation. diff_truncated is omitted (no generated diff); Rust always emits partial=false where Go may omit it, and marks a failed move as failed rather than partial; not_directory, workspace_mismatch and unavailable are additional categories. |
| D12 | URL policy | File URLs are workspace-confined. HTTPS has explicit host/address policy, pinned DNS, no proxy/retry, manually checked redirects (max five), 30-second total deadline, 8192-byte URLs and 1 MiB UTF-8 bodies. Adds path_escape, not_directory and forwarded is_directory filesystem errors, too_large, binary, timeout, workspace_mismatch, unavailable. Directory resources detected by metadata return io. file://localhost normalizes locally; other authorities and file queries fail validation. |
| D13 | Tracker backend | bn CLI replaces CloseWriter and optional capability traits. Comment/transition are always available under host workflow policy; link_pr remains unsupported_op. IDs, states, reason and input bounds validate before dispatch. Empty reason uses a default. Exit-code mapping differs from typed adapter errors (see matrix); adds io, workspace_mismatch and unavailable. No raw output, paths or credentials in errors. |
| D14 | Interaction surface / metadata | Durable host responses replace stdin/stderr. user_interact `/description` describes host collection; `/parameters/properties/answer/description` reserves relayed answers for Pending and rejects them on Durable. Questions and answers cross the host boundary unredacted; do not collect secrets. |
| D15 | Interaction validation / bounds | Questions reject controls except newline/carriage return/tab. Answers reject NUL. Question caps are 8192 bytes on both surfaces; durable answers are at most 8192 bytes and relay answers at most 1 MiB. Recorded host Deny/Cancel/Expire yield denied/cancelled/timeout. Hosts explicitly expire overdue questions; no prompt-wait semaphore or process I/O remains. |
| D16 | Pending relay | No approval or question/answer correlation: model-supplied answers succeed on Pending. Durable rejects them. Never interpret Pending success as human consent. |
| D17 | JSON / result API | serde_json keeps the last duplicate top-level key rather than Go's rejection. Unknown fields are rejected. No RawJSON carrier or per-result IsRetryable API; metadata retry_safe is compared, while hosts own retry decisions. Optional zero/empty Rust keys may be omitted as documented below. |

`Surface::Pending` is a relay, not a gate. A model can call with `answer` set and
receive `succeeded` without any human involvement. Hosts must not treat this as
human consent; use `Surface::Durable` when a human must answer.

## Per-tool contract matrix

Derived from the eight `docs/inventory/*.md` files at the pinned Go revision,
checked against [tool-contract.md](tool-contract.md). Field names below are the
model-facing snake_case names of inventory Go fields. `RawJSON` is a Go-only
carrier excluded from JSON (D17); Category/Message map to error.category/message.
All subsections list every inventory category. Common added workspace_mismatch,
unavailable and sanitized messages are D5/D7. A mirrored key means matching
meaning, subject to the documented optional-field omissions, not byte equality.

### fileops

| Upstream keys | Rust mapping |
| --- | --- |
| outcome, error.category, error.message | Mirrored; early validation/admission failures may omit path (D5/D7/D17). |
| Read: path, content, content_bytes, truncated, numbered_content, line_start, line_end, total_lines, next_offset, line_truncated, truncation_reason | Mirrored; empty content/content_bytes and zero/false optional fields omitted. Prefix truncation_reason=prefix is also emitted upstream. |
| Write: path, bytes_written, created | Mirrored; bytes_written includes zero. |
| Edit: path, bytes_written, anchor_occurrences | Mirrored; contextual failures include anchor_occurrences=0; context-free execute omits it (D17). |
| List: path, entries[].path, entries[].is_dir, truncated | Mirrored; truncated omitted unless true. |

Mirrored categories: validation, path_escape, not_found, is_directory,
not_directory, anchor_not_found, anchor_ambiguous, too_large, binary, io, unknown.
`canceled` has no Rust envelope (D7). Added workspace_mismatch and unavailable
(D5/D7). Capability, ownership and atomicity differences are D5/D6.

### glob

| Upstream keys | Rust mapping |
| --- | --- |
| outcome, paths[].path, paths[].is_dir, count, truncated, error.category, error.message | Mirrored, including empty paths/zero count/false truncated on failure. RawJSON excluded (D17). |

Mirrored categories: validation, path_escape, not_found, not_directory, io,
unknown. `canceled` has no Rust envelope (D7). Added workspace_mismatch and
unavailable (D5/D7). Resource and ordering differences are D10.

### search

| Upstream keys | Rust mapping |
| --- | --- |
| outcome, matches, match_count, truncated, truncation_reason, duration_ms, timed_out, partial, error.category, error.message | Mirrored; optional false/empty fields omitted; contextual failures retain empty matches, zero match_count and duration_ms; context-free execute returns only outcome/error (D17). Partial success retains an exec_failed error. |
| matches[].path, line_number, line, line_truncated, submatches[].text/start/end, before/after[].line_number/line/line_truncated | Mirrored; submatch ranges outside retained UTF-8 text dropped, literal mode omits submatches (D9). |

Mirrored categories: validation, path_escape, not_found, invalid_pattern,
timeout, exec_failed, unknown. `canceled` has no Rust envelope (D7). Adds
is_directory, not_directory, io, workspace_mismatch, unavailable through shared
workspace/path/admission handling (D5/D7). Schema, byte accounting, sanitization
and accepted path race are D8/D9.

### shell

| Upstream keys | Rust mapping |
| --- | --- |
| outcome, exit_code, stdout, stderr, stdout_truncated, stderr_truncated, duration_ms, timed_out, error.category, error.message | Mirrored; nonzero exit remains succeeded. False optional fields omitted; contextual failures retain exit_code=-1, empty stdout/stderr and duration_ms; context-free execute omits execution keys (D17). |

Mirrored categories: validation, timeout, exec_failed, unknown. `canceled` has
no Rust envelope (D7). Shared path/admission adds path_escape, not_found,
is_directory, not_directory, io, workspace_mismatch, unavailable (D5/D7).
Explicit startup/output/environment policy and process lifecycle are D8.

### applypatch

| Upstream keys | Rust mapping |
| --- | --- |
| outcome, files, partial, error.category, error.message | Mirrored, with conservative commit failure partial=true; Rust always retains partial=false where Go omits it (D11). |
| files[].operation/path/new_path/status/additions/deletions | Mirrored; new_path omitted unless applicable. A move that installs its destination but cannot unlink the source reports status=failed instead of Go status=partial (D11). |
| diff_truncated | Omitted: Rust generates no diff (D11). |
| RawJSON | No Rust carrier (D17). |

Mirrored categories: validation, path_escape, not_found, is_directory, too_large,
unsupported, conflict, binary, io, unknown. `canceled` has no Rust envelope
(D7). Additional not_directory, workspace_mismatch, unavailable (D5/D7/D11).
Resource caps, no-clobber installation and alias preflight are D11; atomic writes
and ownership are D6. Per-file commits are not a multi-file transaction.

### urlfetch

| Upstream keys | Rust mapping |
| --- | --- |
| outcome, content, error.category, error.message | Mirrored; empty successful content is retained. |
| RawJSON | No Rust carrier (D17). |

Mirrored categories: validation, not_found (file missing or HTTP 404), io,
network (transport and other HTTP failure), unknown. `canceled` has no Rust
envelope (D7). Added path_escape, not_directory and forwarded is_directory filesystem
errors, too_large, binary, timeout, workspace_mismatch, unavailable (D12/D5/D7).
A regular-file ancestor returns not_directory; directory resources detected by
metadata return io. File confinement, UTF-8, DNS and redirect behavior are D12.

### trackerwrite

| Upstream keys | Rust mapping |
| --- | --- |
| outcome, op, id, error.category/message/op | Mirrored; invalid oversized op/id echoed as empty strings to bound envelope (D13). |

| Upstream category | Rust mapping |
| --- | --- |
| validation | Mirrored validation checks; bn exit 1 maps here (D13). |
| unsupported_op | Mirrored for link_pr; comment/transition use fixed bn capabilities (D13). |
| not_found | bn exit 2 (D13). |
| api_request | bn exit 3 or spawn error (D13). |
| rate_limited | bn exit 4 (D13). |
| timeout | Fixed command deadline (D13). |
| unknown | Other exit/signal or panic (D13/D7). |
| auth_failed, conflict, api_status, unsupported | No typed CLI mapping; other exits become unknown (D13). |
| canceled | No Rust envelope (D7). |

Added io for capture failure, workspace_mismatch and unavailable (D13/D5/D7).
Typed adapter retry categorization is absent (D17). Explicit project/hub/actor,
replacement environment, input validation and supported ops are D13.

### userinteract

| Upstream keys | Rust mapping |
| --- | --- |
| outcome (succeeded/pending/failed), answer, question, error.category/message | Mirrored under surface mapping; empty successful answer omitted. Pending includes question and never error. |
| RawJSON | No Rust carrier (D17). |

Mirrored categories: validation, io, unknown. Adds too_large, timeout, unavailable
(D15). Durable questions, Durable answer rejection, control checks and unredacted
boundary are D14/D15. Pending is a relay (D16). All outcomes remain non-retry-safe.

## Beyond parity

| Tool | Reason | Upstream |
| --- | --- | --- |
| `multi_edit` | Ordered edits in a single atomic write | eino-tools has no counterpart |
| `file_delete` | Explicit regular-file removal | eino-tools has no counterpart |
| `file_move` | Preserve inode without overwriting destination | eino-tools has no counterpart |

`multi_edit`, `file_delete` and `file_move` are included in release 0.3.0 and have no eino-tools counterpart.

## How parity is verified

`metadata_parity_docs_hashes_and_prelude` compares the independent per-tool
fixtures modulo every used entry in ALLOWED-DIFFS.md, exact contract schemas,
and frozen identity hashes. `catalog_matches_upstream_definitions` compares
EINO_PARITY ids, names, retry_safe and the binding-to-mutating mapping against
the Go-extracted catalog.json; `fixture_names_match_catalog_rows` ties its rows
to the per-tool fixture files. Documentation checks cover allowlist pointers and
inventory section coverage; reviewers check the matrix contents.

The copied Go runtime corpora for apply_patch and user_interact run in Rust tests.
Scripted catalog runs exercise all tools without credentials, network or a real
hub, plus durable continuation and independent workspace capacities.
[PROVENANCE.md](../fixtures/eino-tools/PROVENANCE.md) records extraction and hashes
of catalog.json and the committed inert crabber-fixtures.go.txt extractor.
Fixtures are frozen at `c0975d6`: upstream drift is detected only by re-running
extraction against a newer revision, never by CI.

Version 0.4.0 adoption: durable questions use Crabber's persisted interaction
protocol. Recorded denials and cancellation have `denied` and `cancelled` tool
categories; executor interruption remains a runtime concern. Failure envelopes
now set ToolOutput.is_error. See [lifecycle](standard-tools-lifecycle.md) for actual resource joins
and interrupted native-phase uncertainty.

## Consolidation identity

The containing workspace uses version 0.1.0. Consolidation changes the
configuration identity domain to v2 and hashes `TOOLS_IDENTITY_REVISION` rather
than the crate version. Plans and checkpoints recorded by crabber-tools 0.4.0
are not restorable; the repositories had no active users at consolidation.
Schema identities and the checked-in schema hashes remain unchanged.
