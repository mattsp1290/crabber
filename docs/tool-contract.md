# Tool contract

A success carries `outcome: "succeeded"` and normally omits `error`. Search partial
successes carry `partial: true` and a sanitized `exec_failed` error; retain their
collected matches. A model-facing failure
carries `outcome: "failed"` and `error: {category, message}`. Shell nonzero exits
remain successful calls; read `exit_code`. Tool deadlines produce failed/timeout; tool-specific timeout fields are described
below. The `timed_out` and `rejected` outcomes are reserved for callers.
`user_interact` alone may return `outcome: "pending"` with the echoed `question`;
it is the only tool-local outcome and never carries `error`.
Errors built by this library never contain absolute host paths, command output,
or file contents. Search diagnostics are sanitized to enforce this rule.

The host admits an absolute existing workspace once, as a capability directory.
Every persisted session directory must be an absolute canonical match;
the default AgentConfig directory `.` fails with `workspace_mismatch`. Reopen
WorkspaceRoot after replacing the directory at the same path. Relative in-root
symlinks work; absolute targets (even in-root) and escaping targets fail with
`path_escape`. Parent traversal is rejected syntactically. Existing hard links are a host concern; file_move creates a transient second link
during a move. Filesystem operations run as the host uid and preserve mode on atomic
writes; new files use 0644. Ownership is copied when the host is root or the
desired owner is the host uid; an attempted chown failure returns `io`. Other
unprivileged replacements keep the new file’s host ownership. A crash may leave a `.crabber-tools-tmp-*`
sibling, visible to file_list. Writes sync their temp file before rename.

Cancellation produces no model-facing result. A pre-cancelled token fails before
any effect. Native work registers with `ToolContext::resources()`; blocking
closures retain their guards and actual joins, and process tasks kill and reap
on cancellation, owner close or executor drop. A context without an owner refuses
to start work with `unavailable`. A panicking blocking operation yields
failed/unknown with no cleanup failure. The runtime can report
`PhaseOutcomeUnknown` after interruption even when all resources joined: physical
cleanup and durable outcome reconciliation are distinct. See [lifecycle](standard-tools-lifecycle.md).

Workspace writers (write, edit, multi_edit, file_delete, file_move, shell and apply_patch) share a process-global lock
keyed by canonical root. Read, list, search, glob and URL fetch take capacity only
and are unordered against writes. Tracker mutations use backend hub locking and
take shared capacity without the workspace writer lock. `user_interact` takes
neither the writer lock nor mount capacity.
One deadline bounds the combined lock and capacity wait; expiry is `unavailable`.
Sorted directory listings use an iterative traversal with at most 5000 pending
names across the entire tree and a constant number of open directories; enumeration
must inspect directory names to establish ordering, even after the output cap.

Registering only enabled tools is the per-node allowlist. `restrict_to_enabled`
is run-wide and denies other extensions and host tools; use false for mixed
mounts. Use `shared_extension` for a single admitted root or `StandardToolsFactory`
with host-owned admission for multiple workspaces. Two shared roots still collide
on tool names. Missing session directory returns `workspace_mismatch` for every
workspace tool, including URL fetch and tracker writes. Factory admission also
requires a workspace ID. An owner is cached once per workspace ID: after closing that owner, the same Agent refuses new admissions for that
workspace ID. To remap or replace its directory, close the Agent extensions and
build a new Agent with the updated admission table. Capacity is shared within that workspace instance, so one session may use
another's slots within the configured bound. Writer locks share the canonical root
across all instances. The factory configuration hash covers policies and enabled
schemas; the per-instance hash also includes the root. Root authorization comes
from host admission and executor path checks, rather than plan identity.
`ExactRoots` compares configured directory strings exactly; trailing slashes differ.
`required_permissions` is advisory metadata,
not enforced by StaticPolicy: hosts enforce permission rules by name or supply
their own policy. Hosts also own command_guard analysis and output redaction.

Process policy explicitly chooses Inherit, Replace, or Allowlist and optional
run_as uid/gid. Minimal allowlist retains PATH, HOME, LANG and TERM. Supply PATH
for shell command lookup. Login shells may source host startup files. Shell and
search binaries are absolute and validated once; replacing binaries later is a
host concern. Search always disables ripgrep config and clears RIPGREP_CONFIG_PATH.
Search validates paths through the capability, then rg resolves them again:
symlink replacement between these steps is an accepted TOCTOU window. rg does
not follow symlink directories by default.

Schema identity hashes version, name, description and recursively sorted schema
keys. Extension identity includes version, enabled ids and schema hashes in
canonical order, policies (including environment and uid), bounds, root path and
restriction mode. Runtime resources and hooks are not serializable identities.

Metadata differences are enumerated in
[ALLOWED-DIFFS](../fixtures/eino-tools/ALLOWED-DIFFS.md). Cancellation, capability
symlink rules, sanitized errors, explicit process policies, mutating-only locks,
ownership preservation, fsync, and pipe grace are deliberate behavior differences
from eino-tools. Search `limit: 0` selects 200 and is reflected in its schema.

## file_read

Host path exclusions return `path_denied` with the constant message
`path denied by host policy`; see [Path deny policy](#path-deny-policy).

Id: `standard.file-read`. retry_safe: `true`. Permissions: `workspace.fs.read`.

Read a workspace-relative UTF-8 text file. Plain {path} calls return the leading content prefix capped at 256 KiB. Supplying offset and/or limit returns a line-window with raw and numbered content. Returns structured errors including path_escape, not_found, is_directory, binary, validation, and io.

```json
{
  "additionalProperties": false,
  "properties": {
    "limit": {
      "description": "Optional number of lines for a line-windowed read. Default 2000 when offset or limit is present; cap is 5000.",
      "maximum": 5000,
      "minimum": 1,
      "type": "integer"
    },
    "offset": {
      "description": "Optional 1-based starting line for a line-windowed read. When omitted, legacy prefix reads are preserved unless limit is present.",
      "minimum": 1,
      "type": "integer"
    },
    "path": {
      "description": "Workspace-relative path of the file to read.",
      "minLength": 1,
      "type": "string"
    }
  },
  "required": [
    "path"
  ],
  "type": "object"
}
```

Success keys: outcome, path, content, content_bytes (empty content omitted). Window mode adds numbered_content, line_start, line_end, total_lines, next_offset, line_truncated, truncation_reason and truncated when nonzero/true.

Limits: Prefix 256 KiB; default window 2000, maximum 5000 lines; line cap 16 KiB. NUL/invalid UTF-8 is binary. Offset past EOF is validation.

Failure categories: path_denied, validation, path_escape, not_found, is_directory, not_directory, io, unknown, workspace_mismatch, unavailable, binary, too_large.

## file_write

Host path exclusions return `path_denied` with the constant message
`path denied by host policy`; see [Path deny policy](#path-deny-policy).

Id: `standard.file-write`. retry_safe: `false`. Permissions: `workspace.fs.write`.

Write (create or overwrite) a workspace-relative file. Content is capped at 256 KiB. Optional create_dirs=true mkdir -p's the parent chain. Returns a structured error envelope on path_escape, not_found (missing parent), too_large, or io.

```json
{
  "additionalProperties": false,
  "properties": {
    "content": {
      "description": "File content. May be empty (truncates the target to 0 bytes).",
      "type": "string"
    },
    "create_dirs": {
      "description": "If true, mkdir -p the parent chain. Default false.",
      "type": "boolean"
    },
    "path": {
      "description": "Workspace-relative target file.",
      "minLength": 1,
      "type": "string"
    }
  },
  "required": [
    "path",
    "content"
  ],
  "type": "object"
}
```

Success keys: outcome, path, bytes_written, created.

Limits: Input 256 KiB. Atomic sibling rename, optional parent creation, preserved permissions/ownership.

Failure categories: path_denied, validation, path_escape, not_found, is_directory, not_directory, io, unknown, workspace_mismatch, unavailable, too_large.

## file_edit

Host path exclusions return `path_denied` with the constant message
`path denied by host policy`; see [Path deny policy](#path-deny-policy).

Id: `standard.file-edit`. retry_safe: `false`. Permissions: `workspace.fs.write`.

Edit a workspace-relative file in place by anchored substring replacement. The anchor MUST appear exactly once in the file. Returns structured error envelopes on path_escape, not_found, is_directory, anchor_not_found, anchor_ambiguous, too_large, or io.

```json
{
  "additionalProperties": false,
  "properties": {
    "anchor": {
      "description": "Literal substring to find. Must match exactly once. No regex.",
      "minLength": 1,
      "type": "string"
    },
    "path": {
      "description": "Workspace-relative file to edit. Must exist.",
      "minLength": 1,
      "type": "string"
    },
    "replacement": {
      "description": "Substitute for the anchor. May be empty.",
      "type": "string"
    }
  },
  "required": [
    "path",
    "anchor",
    "replacement"
  ],
  "type": "object"
}
```

Success keys: outcome, path, bytes_written, anchor_occurrences (including zero on failure).

Limits: Input/output 256 KiB; anchor must occur exactly once (non-overlapping). Empty replacement deletes the anchor.

Failure categories: path_denied, validation, path_escape, not_found, is_directory, not_directory, io, unknown, workspace_mismatch, unavailable, binary, too_large, anchor_not_found, anchor_ambiguous.

## file_list

Host path exclusions return `path_denied` with the constant message
`path denied by host policy`; see [Path deny policy](#path-deny-policy).

Id: `standard.file-list`. retry_safe: `true`. Permissions: `workspace.fs.read`.

List directory entries under a workspace-relative path (empty or "." lists the workspace root). Output is sorted and capped at 5000 entries; oversize results set truncated=true.

```json
{
  "additionalProperties": false,
  "properties": {
    "path": {
      "description": "Workspace-relative directory. Use '.' for the workspace root. Omit the field entirely to default to '.'.",
      "minLength": 1,
      "type": "string"
    },
    "recursive": {
      "description": "If true, walk descendants. Default false (immediate children only).",
      "type": "boolean"
    }
  },
  "type": "object"
}
```

Success keys: outcome, path, entries [{path, is_dir}]; truncated only when capped.

Limits: 5000 entries; sorted pre-order traversal, includes VCS and hidden directories, does not descend symlink directories.

Failure categories: path_denied, validation, path_escape, not_found, is_directory, not_directory, io, unknown, workspace_mismatch, unavailable.

## search

Host path exclusions return `path_denied` with the constant message
`path denied by host policy`; see [Path deny policy](#path-deny-policy).

Id: `standard.search`. retry_safe: `true`. Permissions: `workspace.fs.read`, `workspace.process.exec`.

Search workspace files via ripgrep. Defaults to regex mode and preserves existing pattern/path/timeout behavior. Optional glob filters, literal mode, ignore-case mode, context lines, and match limit are supported. Per-call timeout defaults to 60s and is capped at 600s. The host resolves the ripgrep binary once; configuration files are always disabled.

```json
{
  "additionalProperties": false,
  "properties": {
    "context": {
      "description": "Number of before and after context lines to return. Default 0; cap is 20.",
      "maximum": 20,
      "minimum": 0,
      "type": "integer"
    },
    "glob": {
      "description": "Optional ripgrep include glob or globs, forwarded as repeated -g filters.",
      "oneOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "items": {
            "minLength": 1,
            "type": "string"
          },
          "minItems": 1,
          "type": "array"
        }
      ]
    },
    "ignore_case": {
      "description": "If true, search case-insensitively via ripgrep -i. Default false.",
      "type": "boolean"
    },
    "limit": {
      "description": "Maximum matches to return. Default 200; cap is 1000.",
      "maximum": 1000,
      "minimum": 0,
      "type": "integer"
    },
    "literal": {
      "description": "If true, search for pattern as a fixed string via ripgrep -F. Default false.",
      "type": "boolean"
    },
    "path": {
      "description": "Workspace-relative search root. Optional; empty or \".\" searches the entire workspace.",
      "type": "string"
    },
    "pattern": {
      "description": "Regex pattern. Forwarded to ripgrep via -e. Inline flags such as (?i) are supported.",
      "minLength": 1,
      "type": "string"
    },
    "timeout_seconds": {
      "description": "Per-call timeout in seconds. 0 or omitted applies the default (60s). Cap is 600s.",
      "maximum": 600,
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "pattern"
  ],
  "type": "object"
}
```

Success keys: outcome, matches, match_count, duration_ms; optional truncated, truncation_reason, partial, timed_out, error. Matches carry path, line_number, line, submatches [{text,start,end}], optional line_truncated, before and after.

Limits: 60s default/600s maximum; 200 default/1000 maximum matches; context maximum 20; line cap 4 KiB, serialized matches 256 KiB, JSON record 8 MiB, stderr 4 KiB. Non-UTF-8 paths/lines decode lossily. Submatch offsets index UTF-8 bytes in the returned line; ranges beyond retained text are omitted, and literal mode returns no submatches. Limit or byte truncation succeeds; failed rg after matches yields partial success.

Failure categories: path_denied, validation, path_escape, not_found, is_directory, not_directory, io, unknown, workspace_mismatch, unavailable, timeout, exec_failed, invalid_pattern.

## shell

Id: `standard.shell`. retry_safe: `false`. Permissions: `workspace.process.exec`, `workspace.fs.write`.

Run a command with host-configured shell policy in the agent's workspace cwd. Captures stdout, stderr, exit code, and duration. Per-call timeout defaults to 60s and is capped at 600s. Stdout/stderr use host-configured per-stream caps (explicitly supplied by the host); oversize output sets truncated=true.

```json
{
  "additionalProperties": false,
  "properties": {
    "cmd": {
      "description": "Shell command body. Executed with host-configured shell policy in the agent's workspace cwd.",
      "minLength": 1,
      "type": "string"
    },
    "timeout_seconds": {
      "description": "Per-call timeout in seconds. 0 or omitted applies the default (60s). Cap is 600s.",
      "maximum": 600,
      "minimum": 0,
      "type": "integer"
    }
  },
  "required": [
    "cmd"
  ],
  "type": "object"
}
```

Success keys: outcome, exit_code, stdout, stderr, duration_ms; optional stdout_truncated, stderr_truncated, timed_out, error.

Limits: 60s default/600s maximum; per-stream cap supplied by host (1 byte..16 MiB); excess drained, UTF-8 incomplete suffix removed and invalid sequences replaced. Signals map to 128+signal.

Failure categories: validation, path_escape, not_found, is_directory, not_directory, io, unknown, workspace_mismatch, unavailable, timeout, exec_failed.

## glob

Host path exclusions return `path_denied` with the constant message
`path denied by host policy`; see [Path deny policy](#path-deny-policy).

Catalog id: `standard.glob`.
Retry safe: true. Advisory permission: `workspace.fs.read`. Include hidden entries, skip
`.git`, `.hg`, `.svn`, `.jj` directories, and never descend through directory
symlinks encountered during walking. An explicitly supplied search root is
resolved through the workspace capability. Patterns match relative to that
search root; returned paths are relative to the workspace. Glob separators are
literal: `*` and `?` do not cross `/`, while `**` can.

```json
{
  "properties": {
    "pattern": {
      "minLength": 1,
      "description": "Doublestar glob pattern to match against paths under the search root, e.g. \"*.go\" or \"**/*_test.go\".",
      "type": "string"
    },
    "path": {
      "description": "Workspace-relative directory to search. Omit or use \".\" for the workspace root.",
      "type": "string"
    },
    "limit": {
      "maximum": 5000,
      "minimum": 1,
      "description": "Maximum number of paths to return. Default 1000; hard cap 5000.",
      "type": "integer"
    }
  },
  "additionalProperties": false,
  "required": [
    "pattern"
  ],
  "type": "object"
}
```

Success keys: `outcome`, `paths` (`path`, `is_dir`), `count`, `truncated`.
Failures retain empty `paths`, zero `count`, false `truncated`, and `error`.
Categories: `path_denied`, `validation`, `path_escape`, `not_found`, `not_directory`, `io`,
`unknown`, `workspace_mismatch`, `unavailable`. Default limit 1000, maximum
5000; zero is rejected. Results sort by rendered workspace path and
`truncated` means another matching entry was omitted. Retained results and
directory batches are bounded independently of tree width. Traversal keeps no
recursive call stack or open directory stack; exhausted/evicted batches rescan
siblings, trading additional directory reads for bounded resources. Cancellation
is checked during every scan and follows the common runtime interruption rule.

Non-UTF-8 names use lossy display paths; distinct raw names remain separate
results even when their displayed paths coincide. Raw bytes break display-sort
ties, and every matched entry counts toward truncation.

Glob patterns are limited to 4096 bytes and at most 16 nested unescaped brace
groups outside character classes. These bounds precede recursive glob parsing.
Compilation uses a fallible GlobSet builder on the blocking worker after capacity
admission; overly complex patterns return `validation` instead of panicking.

## apply_patch

Host path exclusions return `path_denied` with the constant message
`path denied by host policy`; see [Path deny policy](#path-deny-policy).

Catalog id: `standard.apply-patch`.
Retry safe: false. Advisory permission: `workspace.fs.write`.

```json
{
  "properties": {
    "patch_text": {
      "minLength": 1,
      "description": "Patch text using the *** Begin Patch / *** End Patch grammar. Cap is 1 MiB.",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "required": [
    "patch_text"
  ],
  "type": "object"
}
```

Input is capped at 1 MiB before parsing. The iterative parser accepts the pinned
Go grammar: add, update with line-anchored unique hunks, optional move, and delete.
CRLF/lone CR patch input normalizes to LF. Updates preserve CRLF when present in
the original file; matching context includes terminal newlines as upstream does.
An ambiguous or missing hunk returns `conflict`, including overlapping matches.

Preflight validates every source, destination, duplicate/ancestor target, and
hunk before any write or directory creation. Capability-resolved aliases are
also checked for overlapping targets. Sources must be regular files; final
symlinks are rejected. Binary deletion is supported; updates reject NUL or
invalid UTF-8. Deliberate resource addition: each source/result text file is
capped at 16 MiB and aggregate planned content at 64 MiB (`too_large`).

Preflight and commit share the per-root writer lock and mount capacity. Both
guards remain owned by blocking work if the runtime drops its future. Cancellation
is checked between files, reads and hunks; an atomic write already started may
finish before the worker stops. Writes use synced temporary siblings and atomic
installation; add/move destinations use no-clobber hard-link installation.
Existing source mode and conditional ownership are preserved as for fileops.
New files use mode 0644. Missing destination parents are created during commit.

Success keys: `outcome`, `files`, `partial: false`. Each file reports `operation`,
`path`, optional `new_path`, `status`, `additions`, `deletions`. Preflight failures
have `partial: false` and only `preflighted` summaries. A commit failure has
`partial: true`, with completed operations `applied`, the failing operation
`failed`, and remaining operations `preflighted`. This conservative flag may be
true even when the failing operation installed no content, or installed a move
destination but could not remove its source. Patches are atomic per file, not
transactional across files. The writer lock coordinates cooperating tools;
external filesystem mutations can still invalidate a preflight snapshot.

Categories: `path_denied`, `validation`, `path_escape`, `not_found`, `not_directory`,
`is_directory`, `too_large`, `unsupported`, `conflict`, `binary`, `io`,
`unknown`, `workspace_mismatch`, `unavailable`. Error messages omit host paths
and source contents. Failed results include `files`, `partial`, and `error`.

Patch preflight uses linear, overlapping, line-anchored literal matching with
cancellation checkpoints at most 4096 scanned bytes apart. Target overlap checks
use component-aware ancestor lookup and ordered descendant lookup rather than
scanning all earlier targets for every operation.

## url_fetch

Host path exclusions return `path_denied` with the constant message
`path denied by host policy`; see [Path deny policy](#path-deny-policy).

TLS roots come from the host native certificate store through
`rustls-tls-native-roots`; no certificate pinning is performed.

Tool id `standard.url-fetch`; retry safe; requires `network.http.fetch` and
`workspace.fs.read` because a call can select HTTPS or a file URL.

```json
{
  "properties": {
    "url": {
      "minLength": 1,
      "description": "URL to fetch. Supported schemes: file:// (local filesystem) and https://. Returns the raw text content of the resource.",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "required": [
    "url"
  ],
  "type": "object"
}
```

HTTPS uses reqwest with rustls and one 30-second total deadline covering capacity
admission, DNS, all redirects and body reads. It returns raw UTF-8 text, including
empty content. HTTP directs the caller to HTTPS. URL user information is rejected.
URL input and each redirect URL are capped at 8192 bytes. Bodies are capped at
1 MiB (inclusive); invalid UTF-8 produces `binary`. Response 404 maps to
`not_found`; other unsuccessful status codes and transport errors map to
`network`. Errors contain fixed messages without URLs or transport diagnostics.

The host supplies `UrlFetchPolicy`. Its default denies private and non-public
addresses with an empty host allowlist. `HostPattern::exact` normalizes a DNS name
or IP literal; `HostPattern::subdomains` permits proper subdomains of a normalized
DNS suffix, excluding the suffix itself. Non-normalized direct enum values fail
construction. An allowlist does not override address restrictions; opting out of
private-range denial requires explicit host policy. Private, loopback, unspecified,
link-local, shared, multicast, reserved and documentation IPv4 ranges are denied.
IPv6 is restricted to ordinary global unicast, excluding mapped private IPv4,
special-purpose, documentation and transition prefixes. Addresses are checked as
a complete set, with at most 64 resolved addresses; any denied address fails the
hop. Each connection uses a fresh client pinned to that checked set, preserving
URL host and TLS SNI. Proxy environment variables, automatic redirects and
retries are disabled. Redirects are followed manually (at most five), validating
scheme, host allowlist and fresh resolved addresses at every hop, including a
redirect to the same hostname. Downgrades to HTTP or file URLs are rejected.

File URLs use the admitted workspace directory capability and the same body cap
and UTF-8 requirement. Absolute file paths must begin with the canonical `WorkspaceRoot::path()`; symlink
escapes produce `path_escape`. Localhost is normalized as local by URL parsing;
other authorities and queries are rejected. Reads require regular files and use
nonblocking opens so FIFOs cannot stall admission. Reads run off the async worker
and keep shared capacity until the blocking task exits after cancellation or
timeout. File operations do not take the workspace writer lock. The host owns
admission and redaction; this crate does not parse or sanitize HTML.

Errors include `path_denied`, `validation`, `path_escape`, `not_found`, `not_directory`,
forwarded `is_directory` filesystem errors, `io`, `network`,
`too_large`, `binary`, `timeout`, `workspace_mismatch`, `unavailable` and `unknown`.
A file URL whose ancestor is a regular file returns `not_directory`; a directory
resource detected by metadata returns `io`. Cancellation is an executor error.
Differences from eino-tools are confined file
URLs, bounded work/body/redirects, explicit network policy and UTF-8 validation.
Metadata preserves the independent upstream description and schema; this section
defines the Rust runtime's stricter bounds and redirect policy.

The conservative address exclusions include IPv6 documentation `3fff::/20` and
deprecated IPv4 relay `192.88.99.0/24`, including mapped IPv4 forms. Prefixes were
checked against the [IANA IPv6 special-purpose registry](https://www.iana.org/assignments/iana-ipv6-special-registry/)
and [IANA IPv4 special-purpose registry](https://www.iana.org/assignments/iana-ipv4-special-registry/).
These explicit static exclusions do not guarantee routability in every network.

## tracker_write

Catalog id: `standard.tracker-write`.
Retry safe: false. Advisory permission: `tracker.write`. Mutates the hub rather
than the workspace, so it takes mount capacity without the workspace writer lock.
Owner-approved G3 backend: the `bn` CLI.

```json
{
  "properties": {
    "op": {
      "enum": [
        "comment",
        "transition",
        "close",
        "link_pr"
      ],
      "description": "Discriminator. v1 implements 'close' and optionally 'transition' or 'comment' when the configured writer supports those capabilities; unsupported ops return tool_failed{error.category=unsupported_op}.",
      "type": "string"
    },
    "id": {
      "minLength": 1,
      "description": "Tracker issue identifier.",
      "type": "string"
    },
    "body": {
      "minLength": 1,
      "description": "Comment body. Required for op=comment.",
      "type": "string"
    },
    "toState": {
      "minLength": 1,
      "description": "Target issue state for op=transition. If the configured writer does not support transitions, op=transition returns unsupported_op regardless of this value.",
      "type": "string"
    },
    "reason": {
      "description": "Optional close reason.",
      "type": "string"
    },
    "prURL": {
      "minLength": 1,
      "description": "Pull-request URL. Required for op=link_pr (post-v1).",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "required": [
    "op",
    "id"
  ],
  "type": "object"
}
```

Supported operations: `close`, `transition`, `comment`; `link_pr` returns
`unsupported_op`. IDs must match `[a-z0-9-]+-[a-z0-9]{4}` and fit 256 bytes.
Comments must be nonblank; transitions must name a host-configured workflow
state. Comment/reason input is capped at 64 KiB. NUL and unknown fields are
rejected. No shell parses model values.

Exact argv mapping after host-owned `--project <project>`:

- close: `close -r <reason or "closed by tracker_write"> -- <id>`
- transition: `update --status <toState> -- <id>`
- comment: `note -- <id> <body>`

The local bn v0.3.0 parser was verified to accept each positional separator,
including a leading-dash comment body, against a nonexistent scratch hub.
Positional model values follow `--`. Reason and state are option values, which
must precede the separator in bn's grammar: close reasons beginning with `-`
are rejected, and states come from a validated host allowlist with no leading
`-`. Thus no model value becomes an option. This uses the plan's validation
fallback where an option value cannot be placed after `--`.

Host `TrackerPolicy` supplies an absolute executable, explicit project, absolute
hub directory, audit actor, configured statuses, and additional replacement
environment entries. The tool adds `BEANS_HUB` and `BN_ACTOR`; those keys and
duplicate environment names are rejected in additional entries. Nothing is
implicitly inherited. Supply PATH/HOME/git configuration explicitly if the
host wants bn to synchronize with a remote. Child cwd is the admitted workspace.

The command deadline is 60s with the shared bounded kill/reap grace. Both streams
are drained concurrently, retaining at most 4 KiB each. Capped stderr selects a
fixed sanitized diagnostic; raw output, host paths and credentials are never
returned. Cancellation drops the child process-group guard and signals its group.

Exit mapping: 0 succeeded; 1 validation; 2 not_found; 3 api_request; 4
rate_limited; other/signal unknown. Spawn errors are api_request; capture errors
io; command deadlines timeout. Workspace mismatch and unavailable capacity use
the common categories. Result keys: `outcome`, `op`, `id`, and on failure
`error {category,message,op}`. Invalid oversized op/id fields are omitted as
empty strings to keep the error envelope bounded.

## user_interact

Id: `standard.user-interact`. retry_safe: `false`. Advisory permission: `interaction.ask`.

```json
{
  "properties": {
    "question": {
      "minLength": 1,
      "description": "The question to present to the user.",
      "type": "string"
    },
    "answer": {
      "description": "Reserved for relaying an answer the host already collected for a pending question; when non-empty, the tool returns it immediately. Rejected on the durable surface. Leave empty when asking.",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "required": [
    "question"
  ],
  "type": "object"
}
```

Unknown fields are rejected; `answer` defaults to empty. Questions must be nonblank,
contain no controls except newline, carriage return and tab, and fit the configured
byte bound (at most 8192 bytes on either surface). Answers reject NUL.

| Surface | Answer | Behavior |
| --- | --- | --- |
| `Pending` | Empty | `outcome: pending`, echoed question |
| `Pending` | Nonempty | `outcome: succeeded`, echoed answer |
| `Durable` | Nonempty | `failed/validation`, answer reserved for pending surface |
| `Durable` | Empty | Persist one question and checkpoint; call Waiting, run Paused |

`Surface::Pending` is a relay, not a gate. A model can supply an answer without
human involvement; there is no question/answer correlation or consent enforcement.
A model-supplied answer, relay echo, timeout or reconnect is never a human decision.

`UserInteractPolicy::durable(authority)` uses a `ResumableTool`. The host reads
`tool_continuation_snapshot`, authenticates the resolver, records
`answer_tool_interaction`, then invokes `continue_tool`, repeating while the run is Paused. Parallel siblings
that had not started at suspension may produce a later pause. Answered strings have
trailing newlines trimmed; empty answers omit the answer key. Recorded Deny,
Cancel and Expire become failed results with `denied`, `cancelled` and `timeout`
respectively. These host decisions differ from executor cancellation. The runtime
checks correlation, authority, revisions, duplicate/conflicting controls and the
response schema before delivering accepted responses to the tool.

Durable answers default to and cannot exceed `MAX_DURABLE_ANSWER_BYTES` (8192).
The response schema's maxLength counts code points; resume also checks UTF-8 byte
length. A schema-accepted oversized byte string therefore returns `too_large`.
Hosts must check UTF-8 byte length before recording an answer: an oversized
schema-accepted string produces a terminal too_large result, so that question
cannot be re-answered. The pending relay's maximum answer remains 1 MiB. All bounds participate in
configuration identity; changing authority or bounds refuses old continuations.

`expires_after` is evaluated by the store clock; the run stays Paused until the
host records Expire, Deny, Cancel or an answer. Nothing expires a question
automatically. Durations must represent at least one millisecond and fit u64 milliseconds;
after expiry the store accepts only Expire. A reconstructed host must restore the
same store, tool configuration and declared behavior version. See [lifecycle](standard-tools-lifecycle.md)
for the untested crash-after-Started recovery boundary.

Questions and answers cross the host boundary unredacted; never collect secrets.
Hosts own admission, permissions and tool_result_redactor redaction. Answer data
grants no additional permission. This tool performs no stdin/stderr I/O.

## multi_edit

Host path exclusions return `path_denied` with the constant message
`path denied by host policy`; see [Path deny policy](#path-deny-policy).

Id: `standard.multi-edit`. Retry safe: false. Required permission: `workspace.fs.write`.

Apply several anchored substring replacements to one workspace-relative file in a single atomic write. Edits apply in order against the running result; each anchor must occur exactly once at its turn. If any edit fails, nothing is written and the failing edit index is reported.

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "path",
    "edits"
  ],
  "properties": {
    "path": {
      "type": "string",
      "minLength": 1,
      "description": "Workspace-relative file to edit. Must exist."
    },
    "edits": {
      "type": "array",
      "minItems": 1,
      "maxItems": 64,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": [
          "anchor",
          "replacement"
        ],
        "properties": {
          "anchor": {
            "type": "string",
            "minLength": 1,
            "description": "Literal substring to find. Must match exactly once when this edit runs. No regex."
          },
          "replacement": {
            "type": "string",
            "description": "Substitute for the anchor. May be empty."
          }
        }
      },
      "description": "Ordered edits. Later edits see earlier replacements."
    }
  }
}
```

Edits apply in order to the running result. Each literal anchor must occur exactly once at its turn. The file, each anchor and replacement, and every intermediate result are capped at 256 KiB. There must be 1–64 edits and every anchor must be nonempty. An in-root final symlink is followed; the target is edited atomically and the symlink remains. The host pre_write hook runs immediately before the single atomic write. Any failed edit leaves the file unchanged.

Success keys: `outcome`, `path`, `bytes_written`, `edits_applied`. Every failure has `outcome`, `error`, `edits_applied: 0`. Parse (`validation`, `too_large`, syntactic `path_escape`) and pre-run (`workspace_mismatch`, `unavailable`) failures have no `path`. Run failures (`path_escape`, `not_found`, `is_directory`, `binary`, `too_large`, `io`, and filesystem `not_directory`) include `path`. Anchor failures add `edit_index` (zero-based) and `anchor_occurrences`; category is `anchor_not_found` or `anchor_ambiguous`.

## file_delete

Host path exclusions return `path_denied` with the constant message
`path denied by host policy`; see [Path deny policy](#path-deny-policy).

Id: `standard.file-delete`. Retry safe: false. Required permission: `workspace.fs.write`.

Delete one workspace-relative regular file. Directories, symlinks and missing files are rejected with structured errors (is_directory, validation, not_found). Not idempotent: deleting a missing file fails.

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "path"
  ],
  "properties": {
    "path": {
      "type": "string",
      "minLength": 1,
      "description": "Workspace-relative file to delete."
    }
  }
}
```

Deletes one regular file; never follows a final-component symlink. Root or a directory fails `is_directory`; a symlink or non-regular file fails `validation`; a missing file fails `not_found`. Escaping symlink parents fail `path_escape`. Other filesystem errors map to `io` or `not_directory`.

Success keys: `outcome`, `path`. Failure keys: `outcome`, `error`; run-stage failures also include `path`. Parse/pre-run failures have no `path` (`validation`, `workspace_mismatch`, `unavailable`). Cancellation returns an executor error.

## file_move

Host path exclusions return `path_denied` with the constant message
`path denied by host policy`; see [Path deny policy](#path-deny-policy).

Id: `standard.file-move`. Retry safe: false. Required permission: `workspace.fs.write`.

Move or rename one workspace-relative regular file to a new workspace-relative path that does not exist yet. Optional create_dirs=true creates the destination parent chain. The file keeps its mode and ownership. Fails with conflict if the destination exists.

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "path",
    "new_path"
  ],
  "properties": {
    "path": {
      "type": "string",
      "minLength": 1,
      "description": "Workspace-relative source file. Must be a regular file."
    },
    "new_path": {
      "type": "string",
      "minLength": 1,
      "description": "Workspace-relative destination. Must not exist."
    },
    "create_dirs": {
      "type": "boolean",
      "description": "If true, mkdir -p the destination parent chain. Default false."
    }
  }
}
```

Uses a capability-directory hard link followed by source removal; inode, mode and ownership survive, including read-only sources and sources with existing hard links. The source must be a regular file: a final symlink is `validation`, directory/root is `is_directory`, missing is `not_found`. Both paths must be relative, different after normalization, and destination must not be root. Any existing destination, including a symlink or directory, is `conflict`. Case-only renames on case-insensitive filesystems also report `conflict`. Escaping parents fail `path_escape`; other filesystem errors include `not_directory` and `io`.

`create_dirs: true` creates destination parents; these directories remain after later failure. Without it, missing parents fail `not_found`. A destination appearing during the move fails `conflict` without being overwritten. The linked name is checked to reject a source raced into a symlink. The host pre_write hook fires between link and unlink; an already running blocking move finishes with its guards held after Agent interruption.

If the host pre_write hook panics, the destination is rolled back and the result is `failed/unknown` with `path` and `new_path`. If that cleanup also fails, `partial: true` reports that both names may remain. Panic payloads are never returned.

If source removal fails, the destination is removed to roll back: `io` with message "move could not remove source", no `partial`. If rollback also fails: `io` with message "move installed destination but could not remove source" and `partial: true`; both names may remain. Retrying then reports `conflict`; inspect with `file_list`. A host crash in the two-name window can also leave both names, without a returned `partial` result.

No cross-filesystem copy fallback. `EXDEV` maps to `io` with "destination is on a different filesystem". Filesystems without hard links, protected_hardlinks restrictions for a source not owned by the host uid, and link-count limits map to `io` with "filesystem refused the link". For either message use `apply_patch` move (content rewrite), or `file_read` plus `file_write` plus `file_delete`.

Success keys: `outcome`, `path`, `new_path`. Failure keys: `outcome`, `error`; all run-stage failures also include `path`, `new_path`, with `partial: true` only for rollback failure after source removal failure or host-hook panic. Parse (`validation`, `path_escape`) and pre-run (`workspace_mismatch`, `unavailable`) failures have no path keys. Cancellation returns an executor error.

## Catalog policies

`ToolId::ALL` and `EnabledSet::all()` include fourteen tools: the original six
(file_read, file_write, file_edit, file_list, search, shell), then glob,
apply_patch, url_fetch, tracker_write, user_interact, multi_edit, file_delete and file_move. `ToolId::EINO_PARITY`
is the explicit eleven-tool upstream set; `EnabledSet::eino_parity()` selects it.
`Options::url_fetch`, `Options::tracker` and `Options::user_interact` are optional
only for disabled tools; an enabled tool without its policy is a Plan construction
error. The parity set requires all three to be `Some`. Supplied policies are
validated even when disabled, and all three values participate in configuration
identity, including routing, workflow, environment, prompt surface and bounds.
Metadata remains root-free and deterministic. The prelude re-exports
`UrlFetchPolicy`, `HostPattern`, `TrackerPolicy`, `UserInteractPolicy`, `Surface`.

Writer serialization applies to file_write, file_edit, multi_edit, file_delete, file_move, shell and apply_patch.
Glob and URL fetch are readers. Tracker writes use backend hub synchronization.
Every registered tool uses the same mount Capacity, including tracker and URL
operations, except `user_interact`, which has none. Hosts can retain a six-tool
subset with all three optional policies `None`.

`EnabledSet::all()` includes destructive `file_delete` and `file_move`; hosts can subtract `ToolId::BEYOND_PARITY` or select `EnabledSet::eino_parity()`.

## Path deny policy

`Policies::deny` is a host-supplied `PathDenyPolicy`. Use
`PathDenyPolicy::new(["*.pem", ".env", ".env.*", ".netrc", "id_*", "**/.git/config"].map(String::from))?`
for a starting set, then adapt it to the workspace. `PathDenyPolicy::none()`
performs no filesystem checks and preserves ordinary tool behavior.

Patterns are workspace-relative, with literal directory separators. A pattern
without `/` matches a basename at any depth; a pattern containing `/` is rooted
at the workspace. `**` crosses directories. A denied ancestor hides its entire
subtree. `.git/config` covers the root repository only; `**/.git/config` also
covers nested repositories. Root itself never matches. Patterns cannot be empty,
absolute, start with `./`, contain NUL, `..` or empty components, end with `/`,
or contain `[` or `!`. There are at most 256 input patterns, each at most 4096
bytes and 16 nested brace levels. Sorted, deduplicated patterns serialize into
the configuration identity, independent of input order.

| Tools | Enforcement |
| --- | --- |
| `file_read`, `file_list`, `glob`, `search`, file-URL `url_fetch` | Check the requested path and resolved symlink prefixes before access. Discovery omits denied entries, subtrees and denied-resolving aliases. Search filters raw path bytes before output limits and context retention. |
| `file_write`, `file_edit`, `multi_edit`, `file_delete` | The same checks run before reading anchors or applying effects. Atomic writers use the checked resolved target. |
| `file_move` | Check both source and destination before installing a link or removing the source. |
| `apply_patch` | Check every source and move destination before planning reads; one denied path rejects the whole patch with `partial: false`. |

Errors use category `path_denied` and the constant message
`path denied by host policy`. They never include patterns, resolved targets or
content. Existing caller-supplied path fields may still be echoed. Resolution
errors fail closed, except capability escapes retain `path_escape` and
non-directory ancestors retain `not_directory`. Missing tails are checked under
the resolved existing prefix, allowing permitted creates and moves.

On Linux, content-returning file opens recheck `/proc/self/fd` before reading;
unavailable descriptor links, deleted targets, outside-root targets and denied
targets fail closed. Other Unix targets rely on pre-open capability resolution.
Hosts must account for the remaining race between a check and a writer or
ripgrep reopening a path. Writers retain existing atomic-rename semantics;
concurrent symlink swaps are not a filesystem sandbox. Hard links under allowed
names are not detected. Shell commands are controlled by the host's
`command_guard`; `shell`, `tracker_write` and `user_interact` do not consume path
globs. Mounting a shell permits content access and symlink changes outside these
checks. Content redaction remains the host's `tool_result_redactor` policy.

## Typed output and call status

Executors return Crabber `ToolOutput` with the JSON envelope intact. Every
`outcome: failed` sets `is_error`, so the persisted call is Failed. Success and
pending envelopes are Completed; shell nonzero exits remain succeeded and callers
read exit_code. Search partial success also remains Completed.

Crabber caps serialized output at 4 MiB. After per-stream caps, the shared envelope
adapter may further truncate shell stdout/stderr or URL content on UTF-8 boundaries,
setting the corresponding `*_truncated` marker while retaining outcome, exit code
and other fields. Host-selected shell caps up to 16 MiB remain valid. Envelopes that
still cannot fit return a bounded `too_large` failure.
