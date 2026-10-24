# Crabber Extensions

Trusted native extensions for [Crabber](https://github.com/mattsp1290/crabber).
All nine reference features are implemented: workspace instructions, ask-user
interaction, delegated tasks, web search, final JSON result redaction, command
analysis and enforcement, background jobs, Python REPL, and RTK reduction.
Background delegation extends the reference. Combined parity acceptance remains
tracked in [the nine-feature parity plan](extension-parity.md).

The native extensions are a workspace crate of this repository and use only
Crabber's public APIs. Consumers depend on `crabber-extensions` from the crabber
Git repository at the same immutable `rev` as `crabber` and the standard tools.
Default tests and examples use scripted providers and synthetic data and need
no credentials. The opt-in `live-opencode-go` suite skips unless
`OPENCODE_GO_API_KEY` is set.

```sh
cargo test --workspace --locked
DELEGATE_TASK_REQUIRE_OPENCODE_GO=1 cargo test -p crabber-extensions --locked --features live-opencode-go --test delegate_task_live -- --ignored --test-threads=1 --nocapture
env -u OPENCODE_GO_API_KEY cargo test -p crabber-extensions --locked --features live-opencode-go --test delegate_task_live -- --ignored --nocapture
cargo run -p crabber-extensions --example protected_workspace --locked
```

## Workspace instructions

`workspace_instructions::WorkspaceInstructions` registers typed
`SystemPromptMiddleware`. Mount it with `AgentBuilder::shared_extension` and
configure `AgentBuilder::workspace_reader_resolver` with a host-authorized reader.
The [protected workspace example](../crates/crabber-extensions/examples/protected_workspace.rs)
shows both registrations. Options include ordered basenames, finite limits and
an explicit `ReaderRoot`; the middleware never opens filesystem paths itself.

With `ReaderRoot::Workspace`, set `boundaries` to an empty list and root each host
reader at its workspace directory. Only root basenames are requested. With
`ReaderRoot::Boundary`, configure one to sixteen canonical absolute boundary
strings and root the reader at the longest matching boundary. The middleware
uses the persisted directory only for string prefix arithmetic, then requests
ancestor-chain paths in boundary-to-workspace order. An unmatched boundary or a
context with neither workspace field withholds instructions. A partial workspace
identity fails the attempt. Neither case falls back to AgentConfig defaults.

Optional adapters in `workspace_instructions::host` provide
`WorkspaceRootReader::open`, `BoundaryReader::for_workspace` and an exact
`AllowListResolver` keyed by both workspace ID and canonical directory. Admission
uses ambient filesystem authority only in the host constructors. The boundary
reader permits only the admitted workspace's ancestor directories, refusing
sibling paths before filesystem access. Directory handles pin the root; each
read walks child components without following symlinks and accepts only regular
files. Missing files are skipped; symlinks, denied reads, invalid paths and I/O
errors fail closed. Host admission races, hard links and trusted contents remain
host concerns.

Hosts must match the declared rooting mode. A custom reader rooted at a workspace
while declaring Boundary can inject a root file under an ancestor display path
and silently omit missing chain files. The middleware cannot infer a custom
reader's root. Persist canonical directories, test host routing, and rotate the
Agent behavior version when reader trust/routing policy changes.

Files are read afresh for each agent-turn attempt, including provider retries and
post-compaction attempts; compaction requests bypass this middleware. The stable
`workspace/instructions` registration and version-2 descriptor seal names, order,
rooting mode, boundaries and limits. A session mount shadows the global mount.
Invalid UTF-8, NUL-bearing and empty content is skipped. An oversized file emits
an explicit omitted marker without partial contents; a full section omits whole
files. Relative display paths avoid echoing absolute roots. File text is trusted
prompt content, with the same `crabber-v1` frame.

Bounds remain at most 16 names, 64 directories, 16 KiB per file, 32 KiB per
section, 256 in-flight operations and five seconds caller wait. Sections need
at least 256 bytes. Capacity covers resolver and read work; saturation and reader
or deadline failures stop the attempt. Tracked workers retain capacity and host
reader futures until they actually exit, even after caller cancellation or a
deadline. A bounded close can report pending work; a later close can observe its
join. Hosts must keep async readers cooperative and own their spawned work.

## Tool result redactor

`tool_result_redactor::ToolResultRedactor` also implements `Extension`. It
registers in Crabber's **final-redaction phase**, after ordinary reducers
regardless of their registration order. Runtime identity and sticky error status
remain authoritative; the redactor changes only JSON result values.

The built-in catalog matches PKCS#8 private/encrypted-private-key PEM blocks,
Authorization Bearer headers, GitHub prefixed tokens and stateless GitHub tokens,
with token-boundary checks. Hosts can add bounded non-empty regex patterns.
Patterns that can match zero bytes are rejected. These rules are explicit
pattern protection, not a guarantee that arbitrary secrets will be recognized.

Strings are scanned recursively. A secret-bearing or unsafe object key replaces
its containing object with `[REDACTED]`, avoiding key renaming/collisions. Oversized
or NUL-bearing strings become the placeholder. Depth, node or total-byte
exhaustion replaces the entire result, preventing partially scanned output.
Finite bounds apply before and after replacement; byte caps must leave room for
the fixed placeholder. Nodes include keys; the total
byte budget counts decoded strings/keys and JSON primitives, not wire escaping.

Exact tool exclusions deliberately bypass protection. Configuration hashes cover
patterns, exclusions, bounds and order. Capacity is shared across mounts of the
same extension instance. Scanning during runtime dispatch runs in
tracked blocking work, with capacity held until that work ends. Saturation or
worker failure returns a placeholder; runtime cancellation/failure settlement
remains Crabber's responsibility. The standalone synchronous `redact` helper
uses the same scan policy without runtime lifecycle/concurrency admission.

The redactor protects result payloads before persistence and next-model input.
It does not erase inputs, tool names, model output, logs, binary artifacts or
filesystem contents. Crabber supplies parsed JSON values; no Eino attachment or
raw-JSON envelope is introduced. Removing protection requires host policy and
settling active runs; use `Agent::close_extensions` for terminal registry close.

## Command guard

`command_guard::Policy` is a pure, deny-only analysis library for hosts:

```rust
use crabber_extensions::command_guard::{default_bindings, Limits, Options, Policy, Rule};

let policy = Policy::new(Options {
    bindings: default_bindings(),
    rules: vec![Rule {
        id: "host-git-push".into(), executable: "git".into(),
        arg_prefix: vec!["push".into()],
    }],
    limits: Limits {
        max_bindings: 8, max_rules: 32, max_rule_bytes: 2048, max_prefix_args: 16,
        max_json_depth: 16, max_json_nodes: 256, max_command_bytes: 4096,
        max_analysis_bytes: 8192, max_ast_nodes: 2048, max_ast_depth: 16,
        max_words: 512, max_word_bytes: 4096, max_wrapper_depth: 8, max_in_flight: 4,
    },
})?;
let outcome = policy.analyze("shell", &serde_json::json!({"cmd": "git push origin"}));
assert!(outcome.denies());
```

An abstention is never an approval. Unsupported syntax and exhausted budgets
deny without returning command text. The hand-written parser has no new
dependencies and caps nesting at 32, supported by 512 KiB debug-stack tests.
`config_hash()` covers every setting and versioned analysis behavior. No process,
filesystem, environment or network access occurs during analysis.

The integrated durable-host runtime requires multi-thread workers with at least
2 MiB stacks for the hard-cap analysis journey. The direct parser stack proof
does not include the runtime frames.

Mount the deny-only wrapper with host-selected scope:

```rust,ignore
use std::sync::Arc;
use crabber::{Agent, extension::Scope};
use crabber_extensions::command_guard::CommandGuard;

let guard = Arc::new(CommandGuard::new(options)?);
let builder = Agent::builder()
    .behavior_version("host-application.v1")
    .shared_extension(guard.clone(), Scope::Global);
let stats = guard.stats();
```

Denials persist as Crabber's fixed `permission denied`, without a reason.
Hosts re-run `guard.policy().analyze(tool_name, &stored_arguments)` for the
analysis class; capacity and internal failures appear only in `stats()`.
Scope and permissions remain host-owned. Use one instance per tenant to
isolate capacity and counters; shared mounts share both. Each synchronous
check blocks its worker. Use multi-thread runtimes and size `max_in_flight`
at or below the workers the host can afford to block. Panics are caught only
with unwinding enabled; the host-owned panic hook runs before containment.
Policy, wrapper behavior identity or crate-version changes invalidate paused
run fingerprints. Settle unfinished runs before upgrading or rolling back.

This is trusted syntax inspection, not a sandbox. Known non-matches include
`git -C . push`, `nohup`, `nice`, `ionice`, `xargs`, `find -exec`, `busybox`, `ssh`,
`python -c`, `perl -e`, `node -e`, `make`, host-defined shell functions/aliases,
and different executable basenames. See [parity differences](extension-parity.md#command-syntax-analysis)
and the [reference fixture procedure](../crates/crabber-extensions/tests/command_guard/fixtures/README.md).
Unix shell tests require Bash 5+; set `COMMAND_GUARD_REQUIRE_SHELLS=1` to require
execution instead of an explicit skip. CI configuration remains a host decision.

## Delegated tasks

`delegate_task::DelegateTask` sends a bounded task and opaque non-secret profile
to a trusted host `Runner`. Hosts provision any child Agent, provider, sandbox,
credentials and policy. The extension consumes authoritative parent session,
run, call and workspace identities; it creates no child run from response text.
Use the Session-scoped `delegate_task::Factory` for owner isolation, or a shared
instance when shared capacity and all-or-nothing extension close are intended.

Inline `delegate_task` remains an awaited, extension-owned runner. It returns
`completed`, `failed`, `rejected`, `unavailable` or `timed_out`. Interruption,
deadline and caller exit signal its cancellation token, while a retained runner
keeps its slot until exit. Shutdown waits at most `shutdown_grace` for inline
work. Native interruption can leave Crabber's durable outcome uncertain even
when workers have exited; a public `cancelled` activity does not settle that
uncertainty. Hosts inspect close reports and reconcile the durable phase.

`delegate_task_start` registers an independent **runtime-owned session
invocation** before dispatch. The parent can finish, pause or be interrupted
while that job continues. Start returns `started` with a process-local `task_id`,
or `unavailable`; standalone contexts without session-job support fail with the
fixed `session-jobs` error. Session and workspace keys restrict status/list/wait
reads. Rows include `detached: true` and use `running`, `completed`, `failed`,
`rejected`, `unavailable`, `timed_out`, `cancelled` or `error`.

Hosts call `Agent::drain_session_invocations(session, false, bound)` to observe
jobs and pass `true` to request cooperative cancellation. A noncooperative
runner can retain capacity after its row says cancelled or timed_out. Only an
actual joined report proves the runtime job joined. Extension/owner shutdown
cancels retained records and refuses background starts; it joins only inline
work. A fresh controller's empty local drain does not imply durable completion.
The private registry is process-local and cannot resume a prior process's job.
A cold host inspects Crabber's stored session owners for reconciliation. The
[scoped cleanup workflow](extension-parity.md#scoped-cleanup-and-host-control)
explains owner close, runtime drains, repeated deadlines and shared-mount limits.

Both execution modes share `max_in_flight`; optional factory `SharedCapacity`
also bounds the aggregate. Background permits remain held through runner disposal
and the final activity-publication attempt. At most 64 pending or unresolved
registration requests reserve reconciliation slots. Unknown Reserve/Begin
acknowledgements dispatch no runner, release its permit/record and retain the
exact deterministic request; exhausting reconciliation slots returns unavailable.
The deterministic owner ID hashes the authoritative session/run/call tuple;
private task IDs remain process-local. An acknowledged local replay returns its
retained ID before reserving new capacity. Pending registrations cannot replay
as started or become eviction candidates. Refusals restore a reserved terminal
result. If a caller disappears before factory dispatch, the reservation rolls
back and a later factory performs no business work; if dispatch already happened,
that actual job and its reconciliation request stay retained.

Status/list/wait retain bounded records until pruning: `max_tracked` is at most
256, with oldest own terminal records pruned before another owner's terminal
records. A wait pins resolved records across eviction. `max_background_duration`
is at most 24 hours; parent completion does not signal its token. No model-visible
cancel tool or timestamps are supplied. Targeted operations inside shared
instances and per-owner admission remain in `crabber-extensions-4rz6`.

Wait accepts up to 64 unique IDs and an optional positive whole-second timeout
up to floor(`max_wait`); omission preserves the configured fractional duration.
It returns `done` when rows are terminal or `timed_out` with current rows. Output
bytes are bounded before JSON escaping; over-budget and later outputs carry
`output_omitted: true` and can be read individually with status. The saturating
`worst_case_wait_bytes()` bound includes 160 bytes per detached row plus a
64-byte outer allowance. `worst_case_retained_bytes()` includes tracked output
plus up to min(max_tracked, 64) terminal outputs held for registration rollback
and max_in_flight outputs pinned by live lifecycles through final publication;
it excludes metadata and records pinned by waits across eviction. Reads are retry-safe; inline/start are not.

Attribution uses the runtime-bound `PublicSourceWriter`: phase-bound inline
activities and invocation-bound background activities, with real parent call and
nested invocation identities. `Request::session_invocations()` enables nested
jobs while that owner is active; a separate child Agent does not inherit the
link. Activities contain status, mode and profile, with hash-derived IDs, never
task text or output. Cancellation publishes cancelling/cancelled; there is no
upstream public cancel event kind. Publication errors are nonfatal and counted
in `OwnerReport::publication_failures`. Unsupported standalone writers skip.
A failed business response still joins; a future-disposal panic leaves an
uncertain owner and an InvocationError instead of a successful join.

The [coverage matrix](extension-parity.md#delegation-attribution-coverage)
records the unavailable child-run/inline-invocation identities. Permission
metadata remains `session.subagent` for inline/start and `session.subagent.read`
for reads; the host decides access. Use tool names to distinguish inline from
background policy. The v3 fingerprint seals runner identity, all limits, session
job component ID and activity type; earlier frozen plans refuse strict resume.
Mount the final result redactor for output protection; it does not rewrite
private inputs, public activity profile metadata or host-owned logs.

The live suite uses a test-owned child Agent with Crabber's OpenCode Go
adapter. It defaults to `gpt-5.6-luna` with Responses; override through
`OPENCODE_GO_TEST_MODEL` and `OPENCODE_GO_TEST_PROTOCOL` (`responses`, `messages`
or `chat-completions`). Set `DELEGATE_TASK_REQUIRE_OPENCODE_GO=1` to forbid
skipping. A recorded pass requires two `live: executed` markers. Hosted CI only
compiles this suite and verifies its credential-free skip path.

## Background jobs

`background_jobs::BackgroundJobs` owns bounded, non-interactive POSIX shell
jobs on Linux and macOS. Hosts provision the shell and explicitly choose the
frozen environment, policy identities and resource bounds:

```rust,ignore
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use crabber::{Agent, extension::Scope};
use crabber_extensions::background_jobs::{
    BackgroundJobs, Environment, EnvironmentMode, Limits, Options,
};

let jobs = Arc::new(BackgroundJobs::new(Options {
    shell_path: "/bin/sh".into(),
    shell_identity: "host-posix-shell-v1".into(),
    environment: Environment {
        mode: EnvironmentMode::ExplicitOnly,
        overrides: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
        identity: "host-job-environment-v1".into(),
    },
    limits: Limits {
        max_running: 2, max_tracked: 16,
        max_command_bytes: 4096, max_working_directory_bytes: 1024,
        max_output_bytes_per_stream: 65536,
        max_environment_entries: 64, max_environment_bytes: 16384,
        default_timeout: Duration::from_secs(30),
        max_timeout: Duration::from_secs(300),
        terminate_grace: Duration::from_millis(500),
        kill_wait: Duration::from_secs(3),
        shutdown_grace: Duration::from_secs(5),
    },
})?);
let builder = Agent::builder()
    .behavior_version("host-application.v1")
    .shared_extension(jobs.clone(), Scope::Global);
// Supply the host's store, provider, permission policy and AgentConfig.
```

| Tool | Behavior | Permission metadata |
| --- | --- | --- |
| `background_job_start` | Start `command`, optional relative `working_directory` and whole-second `timeout_seconds`; return an opaque job ID immediately. | `background.process.start` |
| `background_job_status` | Read one owned ID, terminal exit status and bounded stdout/stderr tails. | `background.process.read` |
| `background_job_list` | List tracked jobs for the current owner in start order. | `background.process.read` |
| `background_job_kill` | TERM the whole group, then KILL/reap; repeated terminal kills report `newly_accepted: false`. | `background.process.kill` |

Owners are the authoritative durable session ID and workspace ID. The initial
working directory resolves beneath the persisted workspace directory, including
symlink checks. This is launch validation only, not a sandbox: commands can
access anything allowed by the host OS. Hosts own filesystem/network trust,
credentials, provisioning and permission decisions. `Ask` pauses for durable
approval under Agent.
Mount CommandGuard to inspect starts and the result redactor to protect returned
tails before persistence. Raw in-memory tails and command arguments remain
unredacted.

| State | Meaning |
| --- | --- |
| `running` | Owned job still awaiting completed cleanup. |
| `succeeded` | Natural exit 0, with exit code. |
| `failed` | Natural nonzero exit, or unavailable exact status/output; exit code when known. |
| `killed` | Explicit kill, close or cancelled unpublished start selected the cause. |
| `timed_out` | Automatic timeout selected the cause. |

Timeout omitted or zero uses `default_timeout`; a zero default disables it.
Capacity counts starting and hidden cleanup jobs until reaping. Finished records
retain bounded tails and are evicted oldest-completed-first only when admission
needs tracked space. `Limits::worst_case_*` helpers budget result escaping and
raw retained bytes. Tails and the registry are memory-only: after restart, old
IDs are not found. Paused pending starts may execute once on resume, while
Running calls and unsafe pending calls outside paused runs follow Crabber's
interrupted-recovery rule.

Rotate environment identity whenever effective environment values or policy
change. Values are excluded from hashes and Debug; `InheritAndOverride` freezes
the host environment once at construction. Changing shell identity, canonical
shell path, limits or other fingerprinted policy refuses strict paused resume.
Finish or settle runs before upgrading or rolling back.

Use a Tokio runtime with `enable_all()` and an I/O driver. Process support
installs a process-wide SIGCHLD handler. Closing any mount closes every job of
that instance; use separate instances for independent tenants. Interrupt active
runs before close. Shutdown has a bounded manager-close phase followed by a
separate bounded cleanup join; inspect `live_jobs()` for survivors. Host SIGKILL
can orphan commands and anchors, so hosts own external supervision.

Tests use synthetic commands in temporary directories. Set
`BACKGROUND_JOBS_REQUIRE_SHELL=1` to require `/bin/sh`, `ps` and `python3` instead
of allowing explicit fixture skips. CI requires these fixtures on Linux.
Non-Unix targets compile but reject construction as unsupported.
See [background-job parity differences](extension-parity.md#background-jobs).

## Python REPL

`python_repl::PythonRepl` owns one host-provisioned Python 3.11–3.14 interpreter
per durable (session, workspace) pair. Calls on the same owner serialize through
a bounded FIFO queue; different owners can execute concurrently. Globals persist
between turns, including after Python exceptions, until a reset, clear or close.
The owner budget applies over the instance's lifetime, including failed setup.

```rust,ignore
use crabber_extensions::python_repl::{PythonRepl, Options, Environment, EnvironmentMode, Limits};
use std::{collections::BTreeMap, time::Duration};

let python = PythonRepl::new(Options {
    python_path: "/usr/bin/python3".into(), // host-provisioned; no discovery or installer
    python_identity: "host-python-v1".into(),
    temp_root: "/var/tmp/my-host".into(), // trusted, existing directory
    environment: Environment {
        mode: EnvironmentMode::ExplicitOnly,
        overrides: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
        identity: "python-environment-v1".into(),
    },
    limits: Limits {
        max_sessions: 32,
        max_queued_per_session: 8,
        max_code_bytes: 65536,
        max_output_bytes_per_stream: 65536,
        max_result_bytes: 65536,
        max_exception_bytes: 16384,
        max_environment_entries: 128,
        max_environment_bytes: 65536,
        default_timeout: Duration::from_secs(30),
        max_timeout: Duration::from_secs(300),
        runner_start_timeout: Duration::from_secs(10),
        terminate_grace: Duration::from_millis(500),
        kill_wait: Duration::from_secs(5),
        shutdown_grace: Duration::from_secs(10),
    },
})?;
```

| Tool | Arguments | Permission metadata |
| --- | --- | --- |
| `python_repl` | Nonempty `code`; optional whole-second `timeout_seconds` (zero uses default) | `process.python.execute` |
| `python_repl_clear` | Empty object | `process.python.manage` |

Both tools are retry-unsafe. Permissions never grant execution: the host decides,
and `Ask` pauses for durable approval under `Agent`. Registration is atomic and
construction never executes Python. Use a Tokio runtime with process/I/O support (`enable_all()`);
Tokio installs a process-wide SIGCHLD handler. Linux and macOS execute; non-Unix
construction rejects with `unsupported-platform`.

Execute returns `status` (`completed` or `python_error`), bounded `stdout`,
`stderr`, trailing-expression `result` and trimmed `exception` fields, each
`{text, truncated}`, plus `generation`, `state_reset` and `state_reset_reason`.
The Rust `ExecuteResult.status` uses the `ExecuteStatus` enum; its JSON values
are `completed` and `python_error`. Reset reasons are `canceled`, `timed_out`, `cleared` and `runner_failed` (empty
when no notice exists). A successful clear returns `{had_state, generation}`;
a stateful clear leaves a `cleared` notice for one subsequent execute. Clearing
an owner with no live runner consumes an older notice without recreating Python.
`Limits::worst_case_execute_bytes()` and `worst_case_clear_bytes()` help hosts
size the inline tool-result text, including JSON escaping. Events and snapshots
that embed that text again as a JSON string need additional outer escaping.
Execution timeout starts at request write; queue wait and runner startup are
additional. OS argument/environment limits also apply: on Linux a single
environment entry including its terminating NUL must fit within 128 KiB, or
runner spawn fails with `runner-start`. Hosts must not repoint `python_path`
while an instance is live; its canonical target is fingerprinted at construction.

This is trusted Python with host-user authority, not a sandbox. There is no venv,
installer or implicit package provisioning. The interpreter uses `-I -u -B`:
host site-packages remain visible, bytecode writes are disabled, and the workspace
is absent from `sys.path` unless user code adds it. Each owner has mode-0700
`HOME`, five XDG directories and `TMPDIR`/`TMP`/`TEMP` under the trusted temp root.
Those nine keys are reserved in overrides; inherit mode replaces host values.
Python's standard input and user-visible fds 0/1/2 point at `/dev/null`; `input()`
raises EOFError. Captured Python writers carry bounded output. Trusted code can
still tamper with private protocol descriptors through `sys.modules`, OS APIs or
forking. Interpreter stderr is discarded; site hooks must stay silent because
output before bootstrap corrupts readiness. The runner source is visible in `ps`.

Crabber persists tool results; interpreter globals are memory-only. Rotate host
policy identities when interpreter or environment behavior changes, and settle
unfinished runs before adopting or reverting fingerprint changes. Environment
keys/values and temp root are excluded from the hash. Compose with
`ToolResultRedactor` to protect outward results before persistence; it does not
protect source code supplied in persisted tool arguments.

Cancellation and failed delivery reset state on tracked cleanup tasks. Close
interrupts startup/in-flight calls and terminates all owners concurrently under
one deadline, including private-directory removal on the blocking pool. A
missing directory counts as cleaned; removal still running at a deadline may
finish in the background, and close can be retried. Shutdown applies the grace
separately to close and cleanup join, so it can take up to twice that bound. The recommended cleanup defaults
above are TERM grace 500 ms, kill wait 5 s and shutdown grace 10 s; shutdown grace
must cover TERM grace plus kill wait. A TERM-honoring runner ends the grace early.
An accepted clear remains committed if its caller cancels; its `cleared` notice
is left for the next execution. Inspect `live_runners()` after close: quarantined cleanup failures remain visible,
although Crabber shutdown returns Ok. A runner that dies while idle stays counted
until its owner's next execute, clear or close. Descendants that deliberately leave the
runner group are outside cleanup ownership. A killed host can leave orphaned
interpreters inside user code and stale private directories; hosts own that
cleanup. No parent-death watchdog or stale-directory sweep is provided. Host
SIGCHLD auto-reaping or `waitpid(-1)` is outside the contract.

Tests use synthetic data and provisioned Python; `PYTHON_REPL_REQUIRE_PYTHON=1`
forbids interpreter skips and requires 3.11–3.14. CI selects 3.11 and 3.14 on
Linux. The former satellite macOS CI coverage is no longer run. See [the parity boundaries](extension-parity.md#python-repl).
Full nine-feature parity is not claimed.

## RTK reducer

`RtkReducer` registers one ordinary-phase transform for explicitly bound tools.
Hosts provision [RTK v0.51.0](https://github.com/rtk-ai/rtk/releases/tag/v0.51.0),
review its Apache-2.0 license, verify the downloaded archive against the release's
`checksums.txt`, then compute the extracted executable's SHA-256. Release checksums
identify archives, not extracted binaries. Keep the executable immutable for the
mount's lifetime: construction streams its digest without executing it, and later
spawns do not recheck it. Digest verification proves identity, not provenance or
execution-time integrity. RTK is lossy trusted native code with host-user authority;
private directories do not form a sandbox. This library never installs RTK.

```rust,no_run
use crabber_extensions::rtk_reducer::*;
use std::{path::PathBuf, time::Duration};

let reducer = RtkReducer::new(Options {
    order: DEFAULT_ORDER,
    executable_path: PathBuf::from("/host/provisioned/rtk"),
    executable_sha256: "<64 lowercase hex characters for extracted binary>".into(),
    temp_root: PathBuf::from("/host/private/rtk-temp"),
    bindings: vec![Binding {
        tool_name: "shell".into(),
        match_input: Some(InputMatch {
            field: "cmd".into(), equals: vec!["cargo test --workspace".into()],
        }),
        selection: Selection::JsonFields(vec![Field {
            name: "stdout".into(), filter: Filter::CargoTest,
        }]),
    }],
    limits: Limits {
        max_bindings: 8, max_fields_per_binding: 8, max_input_matches: 8,
        max_config_bytes: 16 * 1024, max_input_bytes: 4 * 1024,
        max_field_bytes: 1024 * 1024, max_concurrent: 2,
        max_stdout_bytes: 256 * 1024, timeout: Duration::from_secs(10),
        kill_wait: Duration::from_secs(3), shutdown_grace: Duration::from_secs(30),
    },
})?;
# Ok::<(), crabber::ExtensionError>(())
```

The illustrated `shell` tool must return a top-level string `stdout`, as the
standard-tools shell does. Compatible host tools can use the same binding.
`Selection::Text(Filter)` accepts one JSON string. `JsonFields` accepts named
top-level string members, processed in sorted name order, and moves unselected
members unchanged. Background jobs and Python
return object-valued `stdout`/`stderr` members (`{"text", "truncated"}`), so these
results cannot be selected without future nested-path support. `InputMatch` compares
one normalized top-level input string byte for byte. Hosts assert the format.
Filters are `CargoTest`, `GoTest`, `GitDiff`, and `Log`; `cargo-test` is an addition
beyond the Go reference. No format discovery, project TOML filters or passthrough
mode is provided.

An accepted string starts with `[Reduced by RTK <filter>; original_bytes=<n>]`
and a newline. The marker counts original UTF-8 bytes. The replacement must be
strictly smaller in compact JSON encoding, including quotes, escapes and marker.
This metric is a persisted-size proxy, not a provider-token count. Identity output
and insufficient gain keep that field unchanged. A later field's process failure
rolls back every candidate in the call; the whole value is never serialized to
decide acceptance.

| Condition | Behavior |
| --- | --- |
| Failed, denied/asked, unknown, prepare-failed or earlier error-marked call | Original value; no child |
| Unresolved/non-normalized input, unmatched binding/input, invalid shape, NUL or oversized selected field | Original value; no child |
| Empty selected string | Skip that field without a child |
| Capacity, closing/quarantine, spawn/write/exit failure, timeout, overflow, empty or invalid UTF-8 output | Original whole value; remaining fields skipped |
| Cancellation while the reducer is the first ordinary handler | Crabber drops the callback and settles its fixed interrupted text |
| Invocation directory removal fails or exceeds kill_wait after reaping | The valid result can still be accepted; quarantine blocks reuse and close retries removal |

Crabber can retain a value already accepted by an earlier ordinary handler on
interruption. Mount `ToolResultRedactor`: its final phase follows every ordinary
handler regardless of numeric order or mount order. A lossy filter can truncate
a secret into an unrecognizable prefix, so redaction protects recognizable secrets
in the reduced text. It does not protect persisted tool arguments or host artifacts.

One capacity permit and timeout deadline span all selected fields; capacity never
queues. Selected fields have a hard 1 MiB ceiling. `Stats` distinguishes accepted
calls, no-gain fields, shape/class passes, capacity, timeout, process and cancellation
failures. Counters wrap and are read independently; no paths, PIDs or text are exposed.
`live_children()` includes starting children, unproved reaps and directories awaiting
removal. Owner tasks run on Crabber's mount cleanup tracker and hold the permit until
reaping, or forever when reaping cannot be proved. Inspect quarantine and live counts
after close; Crabber ignores extension shutdown errors.

The temporary root must be existing, absolute, owned by the host euid, and without
group/other write permission. A conventional world-writable `/tmp` fails these checks;
create a private subdirectory. At most sixteen instance roots per process ID can exist
in one temporary root; stale roots count. Exhaustion quarantines a new instance, and
the host must remove stale roots or choose another temp root before constructing a
replacement instance.
Ancestors are not inspected. Each invocation has mode-0700 `work`, `home`, `config`,
`data`, `state`, `cache`, `claude`, and `tmp` directories. The environment is rebuilt
with private `HOME`, `XDG_CONFIG_HOME`, `XDG_DATA_HOME`, `XDG_STATE_HOME`,
`XDG_CACHE_HOME`, `CLAUDE_CONFIG_DIR`, and `TMPDIR`, plus
`RTK_TELEMETRY_DISABLED=1`, `NO_COLOR=1`, `TERM=dumb`, `LANG=C`, and `LC_ALL=C`.
No `PATH` is passed. Stderr is discarded. Invocation directories are removed after
reaping, with removal bounded by `kill_wait`; a removal still running at expiry may
finish in the background. Bounded close removes the instance root and can retry
directory failures. Failed or timed-out calls settle after reaping and removal:
allow up to `timeout + 2 * kill_wait` on Linux, or `timeout + 3 * kill_wait` on
macOS, whose process-group absence confirmation can consume an additional reap bound.
Close has its own `shutdown_grace` deadline and can report incomplete cleanup while
owners finish. Crabber's default mount drain bound is five seconds; hosts using longer
cleanup bounds should set a correspondingly longer registry mount-close timeout.
A killed host may leave roots or children; hosts own stale-state cleanup. Descendants
that leave the process group and host-owned SIGCHLD auto-reaping are outside the
cleanup contract. Mount one reducer per registry; repeated mounts run every handler.
Shared instances share capacity, and closing any mount permanently closes the instance.

The fingerprint includes the digest, RTK release, environment/marker versions,
canonical bindings, order and every limit. Executable and temporary-root paths are
excluded; relocating identical bytes permits strict resume. Settle unfinished runs
before changing fingerprinted policy or reverting it. There is no data migration.
Operational limits such as `max_concurrent` and `shutdown_grace` are fingerprinted
too; settle paused runs before tuning them.

Synthetic `/bin/sh` tests prove plumbing in CI; `BACKGROUND_JOBS_REQUIRE_SHELL=1`
forbids skips. Real-filter semantics are local-only:

```sh
RTK_REDUCER_TEST_EXECUTABLE=/absolute/path/to/rtk \
RTK_REDUCER_TEST_SHA256=<extracted-executable-sha256> \
RTK_REDUCER_REQUIRE_RTK=1 cargo test --workspace --locked real_rtk -- --nocapture
```

First verify the archive against the published checksum, then supply its extracted
binary's digest. Without the executable variable the suite skips loudly; require mode
requires both the executable and expected SHA-256 variables. It verifies `rtk 0.51.0`, sentinel preservation, size reduction and
ambient RTK directory isolation. CI downloads nothing. Filter behavior is verified
only on the platform that runs this suite. See the compile-checked
[`rtk_reducer` example](../crates/crabber-extensions/examples/rtk_reducer.rs), which reads
`RTK_EXAMPLE_EXECUTABLE` and `RTK_EXAMPLE_SHA256` and runs without provider credentials.
Nine-feature composition and Running-call recovery sweeps remain tracked under
`crabber-extensions-2bed`; full parity is not claimed.

## Development

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked
```

The containing workspace uses Linux CI and its stable `rust-toolchain.toml`. Changing frozen extension policy changes the run-plan
fingerprint; finish or settle unfinished runs before adoption or rollback.
No data migration is introduced.

## External Git qualification

[`testdata/extensions-consumer/`](../testdata/extensions-consumer/README.md) creates a
fresh project outside this workspace, generates its own lock once, and checks
composition, public-contract journeys and a two-process SQLite restart using
immutable Git dependencies. Supply one pushed `CRABBER_REV` for both runtime
and native extensions. The [standard tools consumer](../testdata/standard-tools-consumer/README.md)
qualifies the combined runtime, tools, command guard and redactor at that same
revision. Both scripts record lock hashes and check every resolved Crabber
package's source identity. See the [durable host adoption record](extension-parity.md#durable-host-adoption-record)
for the qualified revisions and remaining limits.
