# Native extension parity

Reference: `eino-agent-extensions` commit
`5389549b1f156013a0f82bc936ce4f10edb1ce9f`, using Eino Agent v0.3.3.
Crabber is the containing workspace. Historical pin evidence below describes
the source snapshot before consolidation.
Requests `crabber-r-m5pf`, `crabber-r-u7l3`, and `crabber-r-0f7c` are resolved
with consumer acceptance recorded in Beans.

The original extension implementation plan is `crabber-extensions-plan-pygp`,
with rollup `crabber-extensions-6ynl` in the satellite tracker. Historical issue
IDs below preserve that provenance. Consolidation migrates remaining work with
pointers after its CI acceptance; use `bn ready` from the Crabber checkout for
current Crabber work. Local agent plans remain ignored.

| Feature | Scope | Work |
| --- | --- | --- |
| Workspace instructions | Typed middleware; runtime-authorized readers; bounded fresh chain reads and host adapters | `crabber-extensions-0hzm` |
| Tool result redactor | Built-in/custom patterns, exact exclusions, bounded JSON traversal, final-redaction phase | `crabber-extensions-ux3i` |
| Ask user | Durable UserInput suspension; Store-validated host answer and explicit continuation | `crabber-extensions-isy8` |
| Delegated tasks | Host-owned child runner, authoritative parent/workspace context and tracked cleanup | `crabber-extensions-fvhv` |
| Background delegated tasks | Beyond the reference: start/status/list/wait, session-plus-workspace records, shared capacity, deadline and shutdown cancellation | `crabber-extensions-rzop`, `crabber-extensions-ettm`, `crabber-extensions-hfac`, `crabber-extensions-o479`, `crabber-extensions-xt09`, `crabber-extensions-5qh7` |
| Web search | Host-owned backend callback; bounded queries/results and sanitized failure | `crabber-extensions-g6mu` |
| Command guard | Bounded syntax analysis and conservative wrapper/dialect policy, then runtime integration | `crabber-extensions-iodk`, `crabber-extensions-jlr5` |
| Background jobs | Bounded session/workspace process ownership, four atomic tools and durable journeys | `crabber-extensions-nasz`, `crabber-extensions-477p` |
| Python REPL | Implemented session-scoped lifecycle, REPL/clear tools and resume/reset proofs; merge acceptance pending | `crabber-extensions-b6qy`, `crabber-extensions-ajk3` |
| RTK reducer | Digest-verified host-provisioned RTK v0.51.0, four fixed filters, ordinary-phase reduction before final redaction, held permits through reaping | `crabber-extensions-rja2`, `crabber-extensions-9srp` |

Infrastructure is `crabber-extensions-iaa6`; shared process lifecycle work is
`crabber-extensions-gurf`; final nine-feature acceptance is
`crabber-extensions-2bed`.

## Operating decisions

One native extension library crate lives in the workspace. Feature modules have separate options,
configuration hashes and registration surfaces. Further crate splits should
follow actual dependencies rather than anticipating abstractions. There are
no active consumers requiring compatibility shims or feature flags.

Hosts own UI, trust, process policy, executable artifacts, credentials and
adapter lifecycle. Native extensions are trusted code. The library does not
bundle providers, installers, shell tools or network backends. Crabber owns
identity, fencing, dispatch, cancellation precedence and durable settlement.

Configuration hashes cover behavior-bearing limits, patterns, names, order
and applicable host policy identities. Instruction hashes include reader rooting
mode and boundaries; file contents remain dynamic. Hosts rotate Agent behavior
version when reader trust/routing policy changes. Mounts have Crabber's
frozen-plan fingerprint and strict resume semantics.

## Execution and acceptance

The original delivery sequence, before consolidation, was:

1. Bootstrap the then-pinned workspace and Linux/macOS verification workflow.
2. Deliver workspace instructions and final result redaction with public-facade
   tests, including protected durable messages/events/results and next model input.
3. Port the three host callback bridges and command policy.
4. Build bounded process ownership and port jobs, Python and RTK in small tasks.
5. Verify nine-feature composition, permissions, session isolation, resume,
   cancellation, cleanup and fingerprint changes. Document representation and
   platform differences before claiming full parity.

Crabber uses `serde_json::Value` tool results rather than Eino's raw JSON,
metadata and attachment envelope. This redactor scans JSON strings and keys;
it does not create an Eino envelope or rewrite durable inputs/call identities.
Instruction rendering uses an explicit Crabber format version and smaller
bounds compatible with Crabber's 32 KiB contribution limit.

Workspace instructions use typed `SystemPromptMiddleware` with a version-2
`MiddlewareDescriptor` and hash `workspace-instructions-v2`. Bytes come only from
the runtime-attenuated `WorkspaceReaderResolver`. In Boundary mode, canonical
persisted directory strings select the longest configured boundary and derive
cumulative relative paths; Workspace mode reads root basenames only. No ambient
filesystem authority is exercised during a middleware attempt. Both workspace
fields absent or an unmatched boundary withhold; a partial identity, missing
resolver, denied/invalid read or I/O failure stops the attempt. NotFound skips;
invalid UTF-8, NUL and empty content skip. TooLarge emits an omitted-file marker
with `truncated="true"`, never a partial prefix. Section omission and the
`crabber-v1` relative-path frame remain bounded and deterministic.

Host adapters provide boundary/root directory capabilities and an allow-list
resolver matching both persisted fields. The boundary reader rejects siblings
before accessing the filesystem and walks directories/files without following
symlinks. Readers must be rooted as declared: a mis-rooted custom reader may
inject workspace content as an ancestor and silently skip missing paths (R8).
The middleware cannot detect this; supplied adapters, canonical persisted roots
and host routing tests are the mitigations. Every model attempt resolves and
reads again; contents are not part of the sealed descriptor.

Resolver/read work is retained on the attempt's cleanup tracker until it exits.
Capacity is acquired before resolver entry, preserving the previous bound across
all retained work, and held across deadline/cancellation. Caller timeout is not
a join; a later host close can observe actual completion. This strengthens the
proposed resolve-before-permit sequence so a stalled resolver cannot bypass the
configured bound. The declared deadline covers both resolution and file reads.

Ask-user is a durable `ResumableTool`, with configuration identity `ask-user-v2`
and checkpoint schema `crabber-extensions/ask-user` version 1. It pauses with one
`UserInput` interaction named `answer`; its checkpoint stores normalized question
and choices. There is no responder callback, capacity semaphore or local timer.
Hosts call `tool_continuation_snapshot`, authenticate the operator, submit
`answer_tool_interaction` with the declared authority and current revision, then
`continue_tool`. Preserve operation IDs and targets for identical retries; the
Store enforces authority, schema, revision and replay. A cold Agent over the same
Store can continue without repeating `start`. Provider text cannot answer a pause.

Results remain `selected`, `custom`, `dismissed`, or `timed_out`; `unavailable`
is removed. Deny and Cancel dismiss; timeout requires a host `Expire` operation
after the Store deadline, followed by continuation. Expiration alone never
answers or resumes. Defaults are 4096 question bytes, 256 label bytes, 512
description bytes, 4096 custom-answer bytes and ten minutes. Hard caps are 8192,
512, 1024 and 10240 bytes respectively, with expiry from one second to 24 hours.
The durable schema limit prevents reaching the reference's larger option caps.
Questions, labels and descriptions reject controls except newline and tab.
Response schemas carry choice titles/descriptions and an automatic Other branch;
custom replies are checked again in UTF-8 bytes after character-based Store
validation. The custom hard cap is 10 KiB so even six-byte JSON escapes fit the
Store's separate 64 KiB serialized response bound. The schema template compiles under the Store's jsonschema 0.58
configuration, with at most five maximally escaped choices below 16 KiB.

`interaction.ask` remains permission metadata: Deny prevents execution and Ask
pauses for durable approval before `start`. `retry_safe: false` prevents recovery
from repeating start; lost unstarted continuation claims require explicit recovery
after their lease. The Store records accepted interaction answers verbatim before continuation.
The final result redactor protects only completed tool results and subsequent
provider input; it does not erase the durable interaction answer record. Questions/options remain visible to host policy and the Store;
this tool supplies no UI or secret-collection facility. Do not also register a
Crabber input tool under `ask_user`. Configuration changes, including authority
or bounds, change the sealed continuation fingerprint.

Inline delegated tasks remain extension-owned tracked futures. Cancellation fires
on interruption, deadline and executor exit, and capacity remains held until the
runner future exits. Shutdown joins inline work for at most shutdown_grace;
retained stragglers keep their permits. Background runners instead belong to
Crabber's independent session-invocation controller. Private terminal state,
resource exit and durable settlement are separate observations. Hosts must keep
runners cooperative, stop ingress before closing, and drain runtime jobs
explicitly. Native interruption can retain PhaseOutcomeUnknown and a
NativeOutcomeUnknown close issue after all workers exit. Inline cancellation
activity is display metadata, not proof that this durable uncertainty settled.

The extension provides no child Agent, sandbox, provider or credential. Profiles
are opaque non-secret host routing data. Results remain bounded per call before
JSON escaping; hosts size those bounds with their conversation budgets. Shared
instances share capacity and global shutdown; contextual factories isolate
instances per admitted owner. Business outcome, capacity count and actual
runtime join reports must not be substituted for one another.

Delegation's `required_permissions` is metadata; host `PermissionPolicy` decides
access and `Ask` suspends for durable approval under `crabber::Agent`. Go's
`delegate-profile:<profile>` approval pattern has no equivalent; hosts read the
profile argument in their policy. `retry_safe: false` is declared so Crabber
recovery interrupts pending calls rather than re-running them; run-level
recovery proof remains under `crabber-extensions-2bed`. Guards, policy,
approver and store see admitted task and profile. The pinned Crabber runtime
rejects NUL-bearing durable arguments before execution; direct executor calls
also reject NUL in both fields. Valid runner response text is
host-trusted and stored verbatim within the result bound. Runner errors,
panics and invalid output become one sanitized failure; error payloads are
discarded, so host-visible failure text belongs in `failed` or `rejected`
responses. Hosts needing redaction mount the result redactor. Inline delegation has no
retention policy. Parsed JSON resolves duplicate keys and lone surrogates
before the extension sees input; the configuration hash is not the Go hash.

### Background delegated tasks

This mode exceeds the reference; complete nine-feature parity remains tracked
under `crabber-extensions-2bed`. Hosts still own the child runner and policy.

| Key | Rust boundary |
| --- | --- |
| `tools` | Exactly five tools: inline delegate_task plus delegate_task_start/status/list/wait. Start returns started or unavailable; reads use running/completed/failed/rejected/unavailable/timed_out/cancelled/error; wait adds done/timed_out at top level. |
| `owner` | Persisted session plus workspace ID; missing workspace fails owner; unknown and foreign IDs fail task_id. |
| `ownership` | Start uses ToolContext::session_invocations with deterministic SHA-256 session/run/call request identity. Runtime owns detached jobs and real lifecycle events. Default standalone contexts fail the fixed session-jobs error before runner dispatch. |
| `eviction` | Instance-wide max_tracked≤256; oldest own terminal record first, then any owner's oldest terminal record. Refused registrations restore reserved terminal records; pending rows cannot be pruned. Nondispatched caller drop rolls back and prevents late business dispatch; an actually dispatched uncertain job stays owned. |
| `capacity` | One max_in_flight permit per runner future; optional aggregate SharedCapacity. Background permits remain held through disposal and final publication attempt. A terminal row can precede actual join. Up to 64 pending/unresolved registrations reserve reconciliation slots. |
| `ids` | Private task_ epoch/counter IDs remain process-local (54 bytes). Runtime owner ID is delegate-task. plus the first 32 lowercase hex characters of SHA-256 over serde_json::to_vec(&(session_id, run_id, call_id)); reserve/begin/join operation suffixes are distinct. |
| `retention` | Read-many until pruning; waits retain resolved records across eviction. Fresh instances start empty. Unread cross-owner terminal output may be evicted; reads then fail with generic invalid task_id. No disk persistence. |
| `deadline` | Positive max_background_duration≤24 h settles timed_out and signals the record token; late replies cannot overwrite terminal state. Parent completion, interruption and suspension do not cancel a detached job. |
| `shutdown` | Any shared mount/owner close refuses starts, settles running records cancelled and signals tokens. Shutdown joins only inline work under shutdown_grace. Hosts drain_session_invocations(session, true, bound) to cancel/observe runtime jobs; stragglers retain permits. Reads and inline calls remain available, so stop ingress. A bounded close is not a join. |
| `recovery` | Private records/diagnostics are process-local. Cold status/wait fails for old IDs and a fresh controller has an empty local drain while durable owners can remain Active. Inspect Store::get_session_owner using deterministic identity; no resumption is claimed. Known-Started local replay returns its exact owner/request record before capacity or pruning; pending/unknown cannot claim Started. Runtime replay never dispatches a second factory. Unknown Reserve/Begin acknowledgements dispatch nothing, release permit/record and retain the original request in at most 64 reconciliation slots; caller-drop uncertainty also retains its request. |
| `permissions` | session.subagent is intentionally shared for inline/start, session.subagent.read for reads; host policy decides. Match tool name to allow inline without background. Ask suspends for durable approval under Agent. |
| `attribution` | Phase-bound source for inline, invocation-bound source for background. Activities publish running, cancelling and terminal status, mode/profile only; task/output text stays private. IDs hash session/run/call plus task ID or inline. Revision conflicts refresh once; unknown acknowledgements retry the identical original CAS once, never a changed CAS after unknown. Unsupported skips; other failures increment OwnerReport publication_failures without changing business outcome. |
| `hash` | delegate-task-v3 hashes five names, two permissions, runner identity, all ten limits, the session-job component ID and attribution activity type. Earlier frozen plans intentionally refuse strict resume. |
| `wait-bounds` | 1..max_wait_ids≤64 unique IDs; omitted timeout uses max_wait≥1 s, explicit whole seconds positive and ≤floor(max_wait), while omission preserves fractional duration. Output bytes≤max_wait_reply_bytes≤320 KiB before escaping; first over-budget and later outputs set output_omitted. Outputs larger than the wait budget always require status. worst_case_retained_bytes bounds tracked output plus up to min(max_tracked,64) reserved terminal outputs and max_in_flight lifecycle-pinned outputs (including evictions awaiting final publication), excluding metadata and evicted records pinned by waits. Read individually with status. worst_case_wait_bytes saturates at max_wait_reply_bytes×6+max_wait_ids×160+64 (inline JSON; the typed envelope adds escaping). |
| `cancel-tool` | Absent. Host session-invocation drain provides cooperative session-job cancellation; no public cancel event kind exists. Activities signal cancelling then cancelled. Shared-instance targeted operations and per-owner admission remain tracked in crabber-extensions-4rz6. |
| `timestamps` | Absent; ordering by ID. |
| `live-suite` | Cargo live-opencode-go feature and required-features test target, then credential-presence check. Without key, two loud skips; require flag forbids skip. Default workspace tests and examples need no key. |

### Delegation attribution coverage

| Identity/outcome | Inline delegate_task | Background delegate_task_start |
| --- | --- | --- |
| Parent session/run/call | Phase-bound source carries all three | Invocation-bound source carries all three and parent_call_id |
| Invocation ID/generation | Unavailable upstream for a joined call | Actual runtime invocation identity |
| Child run ID | Unavailable; host runner creates no Crabber child run automatically | Same limitation |
| Nested parent | Jobs spawned through Request::session_invocations are attributed to the inline call | Nested jobs also carry the actual parent invocation |
| Start | Activity running | InvocationStarted plus activity running |
| Business finish/error | Activity completed/failed/rejected/unavailable/error | InvocationFinished after join, including failed business responses, plus terminal activity |
| Cancel | Activity cancelled; native durable outcome can remain uncertain | Activity cancelling then cancelled before join; no public cancel event kind |
| Timeout | Activity timed_out | Activity timed_out before actual join |
| Disposal failure | Original runner contract contains panic; native uncertainty remains host-owned | InvocationError and uncertain owner; no false successful join |
| Output | Completed tool result only | Private status/wait result only |

The immutable-pin MemoryStore probes demonstrate both inline and background
nested attribution, interleaved sessions, hostile provider call IDs, real cancel
publications before runner exit and settlement before InvocationFinished. A
failed business task still joins; a disposal panic does not. Unsupported writers
skip; stale/rejected/unknown publication failures remain visible in diagnostics
and do not fabricate successful delivery. Child-run identity and an inline child
invocation identity remain upstream limitations reported as amendments.

### Live acceptance record

This historical run predates the session-invocation migration. The migration
qualifies the credential-absent skip path; it does not claim a new live run.

| Date | Backend | Model | Protocol | Outcome | Command |
| --- | --- | --- | --- | --- | --- |
| 2026-10-11 | OpenCode Go | gpt-5.6-luna | Responses | Both journeys passed: `live: executed test=live_background_child_completes_and_wait_returns_reply child_status=Completed output_tokens=8`; `live: executed test=live_close_interrupts_streaming_child_and_joins child_status=Interrupted output_tokens=0` | `bash -lc 'cd ~/git/crabber-extensions && DELEGATE_TASK_REQUIRE_OPENCODE_GO=1 cargo test -p crabber-extensions --locked --features live-opencode-go --test delegate_task_live -- --test-threads=1 --nocapture'` |

The model listing contained the selected model before execution. The completion
journey returned CRABBER_OK through a durable parent wait; the close journey
observed streaming, interrupted the child and joined its runner. Skip and require
paths were separately proved with the key removed. Interrupted streams may
report zero output tokens even after text deltas. The adapter's `crabber/0.1`
user agent is Crabber-owned; customization remains a non-blocking follow-up.

### Live adapter dependency additions

Activating Crabber's optional opencode-go adapter adds these 54 lockfile
packages; existing package versions and checksums are unchanged. Licenses are
from each package's Cargo metadata. No development path override is used.

| Package | Version | License |
| --- | --- | --- |
| atomic-waker | 1.1.2 | Apache-2.0 OR MIT |
| base64 | 0.22.1 | MIT OR Apache-2.0 |
| base64 | 0.23.1 | MIT OR Apache-2.0 |
| cc | 1.8.0 | MIT OR Apache-2.0 |
| cfg_aliases | 0.2.2 | MIT |
| chacha20 | 0.10.2 | MIT OR Apache-2.0 |
| cpufeatures | 0.3.1 | MIT OR Apache-2.0 |
| find-msvc-tools | 0.1.14 | MIT OR Apache-2.0 |
| getrandom | 0.2.17 | MIT OR Apache-2.0 |
| http | 1.5.0 | MIT OR Apache-2.0 |
| http-body | 1.1.0 | MIT |
| http-body-util | 0.1.5 | MIT |
| httparse | 1.10.1 | MIT OR Apache-2.0 |
| hyper | 1.12.0 | MIT |
| hyper-rustls | 0.27.10 | Apache-2.0 OR ISC OR MIT |
| hyper-util | 0.1.21 | MIT |
| log | 0.4.34 | MIT OR Apache-2.0 |
| lru-slab | 0.1.3 | MIT OR Apache-2.0 OR Zlib |
| quinn | 0.11.12 | MIT OR Apache-2.0 |
| quinn-proto | 0.11.19 | MIT OR Apache-2.0 |
| quinn-udp | 0.5.16 | MIT OR Apache-2.0 |
| rand | 0.10.3 | MIT OR Apache-2.0 |
| rand_core | 0.10.1 | MIT OR Apache-2.0 |
| rand_pcg | 0.10.2 | MIT OR Apache-2.0 |
| reqwest | 0.12.28 | MIT OR Apache-2.0 |
| ring | 0.17.14 | Apache-2.0 AND ISC |
| rustc-hash | 2.1.3 | Apache-2.0 OR MIT |
| rustls | 0.23.45 | Apache-2.0 OR ISC OR MIT |
| rustls-pki-types | 1.15.1 | MIT OR Apache-2.0 |
| rustls-webpki | 0.103.15 | ISC |
| ryu | 1.0.23 | Apache-2.0 OR BSL-1.0 |
| serde_urlencoded | 0.7.1 | MIT/Apache-2.0 |
| shlex | 2.0.1 | MIT OR Apache-2.0 |
| socket2 | 0.6.5 | MIT OR Apache-2.0 |
| subtle | 2.6.1 | BSD-3-Clause |
| sync_wrapper | 1.0.2 | Apache-2.0 |
| tinyvec | 1.13.3 | Zlib OR Apache-2.0 OR MIT |
| tokio-rustls | 0.26.6 | MIT OR Apache-2.0 |
| tower | 0.5.3 | MIT |
| tower-http | 0.6.11 | MIT |
| tower-layer | 0.3.3 | MIT |
| tower-service | 0.3.3 | MIT |
| tracing | 0.1.44 | MIT |
| tracing-core | 0.1.36 | MIT |
| try-lock | 0.2.5 | MIT |
| untrusted | 0.9.0 | ISC |
| want | 0.3.2 | MIT |
| wasm-bindgen-futures | 0.4.79 | MIT OR Apache-2.0 |
| wasm-streams | 0.4.2 | MIT OR Apache-2.0 |
| web-sys | 0.3.106 | MIT OR Apache-2.0 |
| web-time | 1.1.0 | MIT OR Apache-2.0 |
| webpki-roots | 1.0.9 | CDLA-Permissive-2.0 |
| windows-sys | 0.52.0 | MIT OR Apache-2.0 |
| zeroize | 1.9.1 | Apache-2.0 OR MIT |

Web search awaits a drop-safe host searcher inline. Deadline,
interruption and a settled reply drop the future and release capacity immediately;
the reference holds its slot until the backend goroutine returns. Drop is the
only cancellation signal. Hosts must not block while polled, own spawned work,
and supply network access, credentials, rate limits, caching and freshness.
`max_in_flight` bounds awaited futures only. Capacity exhaustion, deadline and
a reply at or after the deadline are fixed tool failures (`capacity`,
`timed_out`); backend errors and call/poll panics become `searcher`. Panic in
future destruction is contained and preserves the selected outcome. Invalid
source records are dropped; an all-invalid reply is an empty success.

Queries are trimmed before the byte bound and callback delivery. Durable call
arguments retain the model's raw text. There is no `max_raw_input_bytes`;
parsed JSON resolves duplicate keys and lone surrogates before the extension.
The pinned Crabber runtime rejects NUL-bearing durable arguments before execution;
the extension also rejects NUL queries when called directly.
Only the first `max_results` sources are inspected, without refilling invalid
records. Titles and snippets are truncated at a UTF-8 boundary; URLs are stored
verbatim without repair. NUL in a title or snippet drops that record, whereas
the reference passes it through. Rust strings cannot represent invalid UTF-8.

URL validation uses the WHATWG `url` parser behind raw guards for control bytes,
exact lowercase HTTP(S) schemes, nonempty authorities without userinfo, the
reference's authority alphabet excluding all percent escapes, and valid percent
escapes everywhere. URLs are never re-serialized. The executed Go-reference
corpus agrees or is stricter on every tested row; this is no universal claim.
Stricter cases include percent escapes in authorities and invalid query escapes,
`<` and `>` and a mid-host `]`, ports above 65535, IPv4-like hosts with too many
parts, IPv6 zone identifiers, and IDNA compatibility mapping such as U+00A0.
The dependency adds 26 lock packages under MIT, Apache-2.0 and Unicode-3.0.

Search requests add authoritative session, run and call identity and workspace
routing data absent from the reference callback. Bounds apply to each persisted
result and cannot prevent host vector/string allocations. There is no retention
policy; `Limits::worst_case_result_bytes()` includes worst-case JSON escaping.
Parallel execution can carry `max_in_flight` such results, and subsequent calls
in a turn can accumulate more after capacity frees. Permission metadata is
`network.web.search`; host `PermissionPolicy` decides access and `Ask` suspends for durable approval
under `crabber::Agent`. Go's `Pattern` hook has no equivalent. `retry_safe: false`
interrupts pending calls on recovery rather than resuming them once; run-level
proof and redactor composition remain under `crabber-extensions-2bed`. The hash
is not the Go hash and covers limits and backend identity, never the closure.
Hosts must change that identity when backend routing or behavior changes.
Shared instances share capacity; hosts interrupt active runs before closing.

## Command syntax analysis

`command_guard::Policy` delivers the pure analysis library for
`crabber-extensions-iodk`. Hosts provide explicit tool/command-field/dialect
bindings, basename and positional-prefix deny rules, and fourteen positive
limits. `Policy::analyze` takes a parsed JSON object; `analyze_script` takes a
script directly. Both return fixed outcomes without retaining input or touching
processes, files, environment, network or credentials. `Abstain` is never an
approval. `CommandGuard` registers an inline synchronous Crabber `ToolGuard`, takes
atomic capacity permits from `Limits::max_in_flight`, contains unwinding analysis
panics with `catch_unwind`, and exposes per-class counters. Denials persist as
Crabber's fixed `permission denied`. Every check blocks its Tokio worker for the
analysis duration (up to the two-second debug worst case); capacity denial does
not unblock a worker already analyzing. Hosts use multi-thread runtimes and keep
`max_in_flight` at or below the workers they can afford to block.

The parser is hand-written and fail-closed, with one context-sensitive lexer
owned by a recursive-descent parser and a postorder AST walker. No dependency,
manifest, lockfile, Crabber pin or CI change is needed. The user selected this
approach after testing brush-parser 0.4.0: 2,000 nested substitutions overflowed
a 2 MiB stack; its word parsing also uses a global cache and its normal
dependencies add 86 lock packages. Tree-sitter-bash requires a C build and has a
Bash-only, error-tolerant grammar; yash-syntax is GPL-3.0-or-later. The measured
Rust hard depth cap is **32**, lowered from the proposed 128 after a debug
analysis overflowed a 512 KiB thread at 128. Hard-cap nesting, wrapper,
backquote, shell and large-word/case generators now return on that thread.
Depth is shared by syntax and wrappers, and bytes/nodes/words are shared by
nested scripts. Tests require each worst-case analysis to finish within two
seconds in debug builds, including 256 rules comparing 64-token prefixes.

The source reference is still `eino-agent-extensions` at
`5389549b1f156013a0f82bc936ce4f10edb1ce9f`, with mvdan shell parser v3.14.1.
The differences below are explicit analysis boundaries; every fixed outcome
except abstention denies. The generated reference corpus verifies all rows
agree or deny more conservatively, with each stricter row tied to a key.

| Key | Analysis behavior and difference |
| --- | --- |
| `parser` | An AST for the admitted POSIX/Bash grammar replaces the full mvdan parser and walker. Functions, arithmetic, arrays, extended tests, `time`, `coproc`, `select` and other unsupported constructs deny. |
| `json-bytes` | Aggregate key and string bytes are bounded by `max_analysis_bytes` before NUL scans. This is a separate extraction budget; parsed script byte accounting remains shared across nested scripts. The plan's node-only sibling walk otherwise permitted unbounded scanning of one giant JSON string. |
| `raw-input` | Parsed `serde_json::Value` has no raw-input bound or duplicate-key/invalid-UTF-8 channel. All sibling values and keys charge JSON budgets. Sorted key order can change the first denial class for a multi-fault object. |
| `bindings` | Bindings are explicit; an empty vector is an error. `default_bindings()` supplies `shell/cmd/posix` and `background_job_start/command/posix`. Keys are literal, including periods. |
| `limits` | Fourteen limits replace fifteen; no raw-input limit. Hard caps and AST/word/depth accounting are library-defined; byte budgets include re-parsed backquotes and nested shells. |
| `outcomes` | Five fixed analysis outcomes and `Outcome::code()`. Runtime denials persist Crabber's fixed `permission denied`; counters replace the Go message and capacity code. |
| `hash` | A canonical Rust tuple includes versioned behavior identity, every binding, rule and limit. Binding/rule order is ignored. Configuration and corpus-outcome digests are pinned. It is not the Go hash. |
| `identifiers` | 1–256 ASCII alphanumerics, `_`, `-`, `.`, `:`; dots alone are permitted by this exact alphabet. Executable basenames may not be empty, `.`, `..`, or contain slash, backslash or NUL. |
| `cr` | CR remains an ordinary word byte; no mask around the parser is needed. |
| `comment-continuation` | A comment ends at its first newline even after backslash. The reference abstains on `echo a # c \<LF>blocked`; dash and Bash execute `blocked`, and Rust returns `RuleMatch`. Escaped backslashes do not erase a following newline. |
| `heredoc-in-substitution` | Heredocs inside command/process substitutions or re-parsed backquotes are unanalysable. |
| `heredoc-line` | Multiline quotes or substitutions on a pending heredoc operator's line are unanalysable. |
| `heredoc-continuation` | Unquoted heredoc backslash-newline groups are unanalysable because Bash and dash disagree on joined closing delimiters; quoted heredoc bodies remain literal data. |
| `heredoc-delimiter` | Expansions in the delimiter are unanalysable instead of a reference parse error. Quoted bodies are data; unquoted bodies use the same lexer; `<<-` strips leading tabs before parsing even within quotes. |
| `brace-expansion` | Unquoted literal parts containing `{` are unknown. Inspection of pinned `SplitBraces` showed it returns true even for unmatched or escaped braces, contrary to the plan's description; preserving this behavior prevents weaker verdicts. Quoted braces remain known. |
| `posix-ansi-quote` | ANSI and locale quote parts are unknown in both dialects. The reference treats POSIX `$'..'` as literal text and can miss `$'blocked'` or `git $'push'` executed by Bash-as-sh. |
| `ansi-quote-backslash` | A backslash before the first closing ANSI quote denies in both dialects, because Bash and dash disagree about where that quote ends. |
| `reserved-out-of-place` | Reserved words the grammar cannot consume in command position deny, including a second `!` and out-of-place `in`. |
| `posix-time-coproc` | Both are unanalysable in both dialects; the reference's POSIX abstention on `time blocked` misses execution by dash and Bash-as-sh. |
| `posix-arith-subshell` | Adjacent `((` in command position denies under either dialect, including the POSIX ambiguity with nested subshells. Spaced `( (` remains supported. |
| `posix-extended-test` | `[[` denies under either dialect, closing the reference POSIX ordinary-word interpretation. |
| `posix-bracket-arith` | `$[` denies under either dialect; Bash-as-sh evaluates it arithmetically while the reference POSIX parser treats it as literal. |
| `posix-append` | Append assignments deny in both dialects instead of being ordinary words under the reference's POSIX parser. |
| `select` | Bash `select` denies instead of walking a for-clause. |
| `fd-variable-redirect` | Adjacent `{name}>`/`{name}<` variable descriptor forms deny. |
| `deny-class` | POSIX process substitution and pipe-amp are invalid; other Bash-only redirect operators are unanalysable. Different denial classes all deny. |
| `extglob` | Extended-glob openers deny as unanalysable under either dialect. |
| `redirect-target-fd` | A numeric redirect target immediately followed by another redirect operator denies. Reference abstentions such as `2>&1>>file` fail real-shell syntax checks in dash and Bash. |
| `posix-indexed-word` | Incomplete command-position `NAME[` words deny in both dialects; Bash-as-sh rejects forms the reference's POSIX parser treats as ordinary commands. |
| `cancellation` | Analysis is synchronous and bounded; no cooperative context cancellation. |
| `capacity` | Atomic permits are taken only for bound tools; saturation denies immediately and is counted. Checks never overlap within one run task, including parallel execution mode; capacity matters across runs or threads. Each check blocks its worker; size the limit at or below the workers the host can afford to block. |
| `internal` | Analysis panics are caught, counted and denied; the reference returns an internal error that fails the call. Stack overflow, allocation failure and `panic = "abort"` abort. Hard-cap runtime proofs use default and explicit 2 MiB workers. The durable-host pin overflowed an explicit 1 MiB worker in Linux and the macOS CI target aborted; use multi-thread workers with at least 2 MiB stacks. Direct parser proofs at 512 KiB do not include runtime frames. Current-thread runtimes and arbitrary smaller host stacks are not covered. |
| `diagnostics` | No per-call diagnostic message is added. `Stats` and host re-analysis replace Go's code/message; capacity/internal are counters only. The host-owned panic hook may print a caught panic's payload before denial, unlike Go recovery. The user accepted this deviation on 2026-10-06. Module lints and the zero-hit G1 audit of text-bearing panicking std calls support sanitization, without a hook-output test or universal proof. Counter snapshots read fields independently and individual counters wrap at `u64::MAX`. |
| `scope-order` | Scope is the host's Crabber mount argument and is included in frozen-plan identity; guards have no order and all run. Go scope/order configuration and duplicate-mount rejection have no equivalent. Shared instances share counters and capacity across mounts and registries. |
| `resume` | Pending calls of paused runs are guarded again on stored normalized arguments without rerunning `ToolPrepare`. Any rule, binding or limit drift refuses resume with `PlanChanged` before mutation. Crabber marks calls Running before guards; a crash during analysis settles Interrupted on recovery without re-guarding (D10), although the executor never ran. `recover()` sweeps and Running-call settlement proofs remain `crabber-extensions-2bed`. |
| `resume-scope` | A different mount scope with equal options refuses resume with PlanChanged before mutation; the reference also rejects scope/order drift. |
| `guard-layer` | Delivered deny-only `CommandGuard` registers one guard, with a versioned wrapper hash over policy identity; no tools or shutdown resources. |

Basename/prefix rules do not match options before a subcommand (`git -C . push`),
`nohup`, `nice`, `ionice`, `xargs`, `find -exec`, `busybox`, `ssh`, `python -c`,
`perl -e`, `node -e`, `make`, host-defined shell functions or aliases, or
executables with a different basename. This is trusted syntax inspection, not a
sandbox. Hosts own execution environment, provisioning, trust and permissions.

## Background jobs

`background_jobs::BackgroundJobs` delivers bounded process ownership and the
four start/status/list/kill tools. The host supplies a canonical executable
shell, shell identity, frozen environment policy and twelve Limits fields.
Permission metadata never grants execution; the host PermissionPolicy decides.
All commands and retained output are trusted host workloads, not sandboxed data.
The process module remains private and shared with future process consumers.

| Key | Reference behavior and Rust boundary |
| --- | --- |
| `supervisor` | The reference uses status/readiness/gate FDs 3/4/5. Under `unsafe_code = "forbid"`, Rust uses a fixed POSIX supervisor, stdin `G` gate, exit status and a `command -p sleep` group anchor. No readiness byte or extra descriptors; exec failure is `spawn-failed`, gate failure is `gate`. Shell `128+n` cannot distinguish a signal from explicit exit of that value. The anchor preserves the group ID during normal cleanup; externally killing an unanchored leader leaves a narrow group-ID reuse window. Host SIGKILL can orphan commands/anchors. |
| `host-subprocess` | Crabber's public Subprocess service is one-shot without streaming/group/kill contracts, so jobs use safe Tokio process APIs and rustix signals directly. Hosts still own subprocess provisioning and policy. |
| `workspace-root` | Initial root is authoritative WorkspaceContext.directory verbatim, canonicalized at start. Relative working directories must resolve beneath it. This replaces Eino WorkspaceRoot routing; it constrains launch only. |
| `retention` | Memory-only raw-byte stdout/stderr tails and tracked terminal records replace Eino retention/transcript handling. Saturating worst_case_status/list/retained_bytes helpers include result escaping or raw retention as documented; maximum raw tails reach 80 MiB at hard caps. No spill or retrieval API. |
| `limits` | Twelve explicit bounds have library hard caps; max_running≤64, max_tracked≤256, command≤64 KiB, relative directory≤4 KiB, each tail≤160 KiB, environment≤1024 entries/256 KiB. Timeout max is 24 hours; default zero disables it. TERM grace and kill_wait are positive ≤60s; added shutdown_grace is positive ≤120s and applies separately to close and join. |
| `hash` | Versioned Rust hash, not the Go hash. Includes tool names, all permission identifiers, canonical shell OS bytes, shell identity, supervisor protocol/digest, every limit, environment mode/identity. Environment values are excluded; hosts rotate identity when values or policy change. Non-UTF-8 Unix paths hash losslessly. |
| `timestamps` | UTC RFC3339 with nine fractional digits replaces the reference encoding. Civil-date tests pin epoch, leap day, negative time and dates beyond 2038. |
| `environment-utf8` | ExplicitOnly passes only overrides. InheritAndOverride freezes the host environment once, then applies overrides. Non-UTF-8 inherited keys/values, NUL and invalid keys reject construction. Debug omits keys/values and shell paths. |
| `recovery` | Crabber D10 governs pending/running calls: paused pending start may execute once on resume; Running calls and unsafe pending calls outside Paused settle Interrupted. Start/kill retry_safe=false, status/list=true. Registry/output are process-local, so fresh instances see job-not-found/empty lists. recover() sweep proofs remain 2bed. |
| `permissions` | Constant metadata is background.process.start/read/kill; host policy owns access and Ask suspends for durable approval under Agent. Denied starts have PermissionDenied with fixed permission denied and do not spawn. No Eino approval pattern adapter. |
| `cancellation` | A tracked start task owns the reservation, child and publish decision. Cancellation before publication withholds the launch gate; gate failure retains hidden cleanup ownership until reaping. Dropping a live-token caller may still publish. Call cancellation does not kill published jobs. Accepted termination continues after the kill caller cancels, with first-cause precedence and persisted escalation phases. |
| `close` | Closing one mount closes the shared instance. Manager close waits for starts, requests termination and joins completed jobs under its bound; cleanup joins under a second bound. Crabber shutdown itself is unbounded, and mount close returns Ok once shutdown returns even when jobs survive. live_jobs() exposes remaining starting/running/hidden work. Interrupt runs before closing; finished jobs never block close. |
| `runtime` | Requires Tokio process/I/O support and a runtime such as Builder.enable_all(). Tokio installs a process-wide SIGCHLD handler. Real-child tests use multi-thread runtimes and real clocks. |
| `platform` | Linux and macOS execute POSIX jobs and group signals. Non-Unix builds compile but BackgroundJobs::new rejects unsupported-platform. No shell discovery or Windows runner. |
| `inputs` | Parsed JSON accepts only exact tool fields, typed values, bounded UTF-8 bytes and lowercase 53-byte IDs. null timeout is rejected by schema; zero/omitted uses default. Parsed duplicate keys/surrogates are serde_json's boundary. The newer Crabber pin can reject NUL before execution; direct tests preserve the extension's own validation contract. |
| `dependencies` | Tokio adds process/io-util; Unix rustix adds process, without unsafe calls. Lock additions: mio (MIT), signal-hook-registry (Apache-2.0 OR MIT), wasi (Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT). No shell/parser/runtime discovery dependency. |
| `scope` | Owner is durable session plus workspace ID; other owners cannot read/list/kill a job. Shared mounts share running/tracked capacity and closing. Finished records are pruned oldest-completed-first only for admission, never evicting running jobs. List sorts started_at then ID. |

Configuration and internal seam tests cover every limit/hash input, group
escalation and reaping retries, publication cancellation/drop, hidden cleanup,
coalesced kills and timestamp boundaries. Public tests cover exact registration,
atomic collision rollback, session visibility, durable values/events/model input,
permission classes, directory containment, input/output bounds, all lifecycle
paths, an eighteen-row paused-resume drift matrix and unchanged persistence after
refusal. Redactor and command-guard composition protect outward results while
preserving in-memory output. They do not redact persisted command arguments;
marker fixtures therefore supply synthetic values through the environment.

Strict fixture execution requires `/bin/sh`, `ps` and `python3`; the two detached
output-holder tests publish PID ownership before detaching and readiness after
setsid, and verify holder disappearance after cleanup. CI sets
BACKGROUND_JOBS_REQUIRE_SHELL=1 on Linux/macOS. Host-process restart and nine-feature
composition acceptance remain separate work; this is not complete parity.

## Python REPL

`python_repl::PythonRepl` implements the owner-scoped interpreter lifecycle
(`b6qy`) and atomic execute/clear tools with public runtime proofs (`ajk3`).
The host supplies Python, a trusted temp root, environment policy and fourteen
limits. Results are persisted by Crabber; Python globals are process-local.
This feature is implemented on its branch; issue closure requires human merge
and the implementing merged SHA. Full nine-feature parity is not claimed.

| Key | Reference behavior and Rust boundary |
| --- | --- |
| `venv` | No venv. Direct `-I -u -B` interpreter with private HOME, five XDG directories and TMPDIR/TMP/TEMP. Prefix/executable belong to the supplied host interpreter; base site-packages are visible and there is no per-owner writable site-packages. A supplied venv interpreter retains its own configuration. Workspace is off sys.path. No venv-create timeout; neither approach is a security boundary. |
| `interpreter-path` | The validated absolute normalized path is executed; its canonical target enters the hash. A venv bin/python keeps pyvenv.cfg semantics; repointed symlinks refuse strict resume. Hosts must not repoint the path while an instance is live; the canonical target is hashed once and the unresolved path is executed for each start. Construction never executes Python. |
| `supervisor` | No Python supervisor. The runner leads its group. Rust holds the unreaped leader to anchor the PGID until the final KILL, probes exit with waitid(NOWAIT) during every wait, ends TERM grace early on leader exit, and directly KILLs the leader after the group signal. Forked pipe holders cannot hide death. The -c source is visible in ps. |
| `host-crash` | A killed host can orphan a runner inside user code and leave private directories; no stale-directory sweep or parent-death watchdog. Hosts own stale-state cleanup. Host SIG_IGN/SIGCHLD auto-reaping and waitpid(-1) are out of contract. |
| `protocol-fds` | Bounded 4-byte big-endian JSON frames use duplicated fd 0/1; user fd 0/1/2 point at /dev/null and input() raises EOFError. Trusted code can reach private descriptors through sys.modules["__main__"], /proc/self/fd, closerange or fork. Interpreter stderr is discarded, so crash diagnostics are unavailable. Sitecustomize/.pth output before bootstrap corrupts readiness (runner-readiness). Fixed failure codes and resets replace leaked diagnostics; none is a security boundary. |
| `bytecode` | -B disables bytecode writes beside host modules. |
| `environment` | ExplicitOnly or frozen InheritAndOverride. Nine reserved keys reject in overrides and are dropped from inherited entries before counting bounds, then replaced with per-owner mode-0700 directories. Invalid UTF-8 inherited entries reject. A non-UTF-8 temp root cannot be represented in the runner environment and execution fails private-dirs; Python paths hash losslessly. Debug hides paths, keys and values. |
| `hash` | Versioned Rust tuple includes both tool names/permissions, protocol/source digest, interpreter flags and canonical argv layout, private-key table, derived frame bounds/readiness cap, canonical interpreter OS bytes/identity, every limit and environment mode/identity. Scope, temp root and environment keys/values are excluded; the reference includes temp root and keys. Hosts rotate identities for behavior changes. |
| `limits` | Fourteen bounded fields: sessions≤256, queue≤64, source≤1 MiB, each retained stream/result/exception≤100 KiB, environment≤1024 entries/256 KiB, whole-second execution≤24 hours, startup/TERM/reap≤60 s, close≤120 s. Shutdown grace is added, venv-create timeout removed. Inline bound helpers saturate and cover tool-result text escaping; embedding that text as a JSON string requires outer escaping. Execution timeout excludes queueing and startup. OS limits still apply at spawn; a Linux environment entry including its terminating NUL must fit within 128 KiB. |
| `recovery` | Both tools retry_safe=false. A paused pending execute can run once with fresh state on resume; Running/unsafe unfinished calls obey Crabber D10. Running-call recover() sweep proofs remain 2bed. Remounts start empty. |
| `permissions` | process.python.execute/manage are constant metadata. Host policy decides; Ask suspends for durable approval under Agent. No credentials or approvals are owned by this extension. |
| `cancellation` | Tracked tasks own reset after Crabber drops the executor future. Writes/reads sit inside cancellation/deadline selects. Terminating a pre-existing runner advances generation with a notice, even before request execution; failure of a newly started runner or close terminates silently. Committed response delivery is acknowledged before releasing the owner gate; an undeliverable response resets canceled, unlike the reference's committed-wins rule. Queued canceled or dropped calls never reach Python. Accepted clear remains committed even if its caller cancels, leaving its cleared notice for the next execute. |
| `clear-notice` | The plan's durable journey expects a cleared notice after stateful clear; Rust delivers it once on the next execute. The reference consumes it in clear. Clearing without a runner consumes older pending notices and admits no unknown owner. |
| `close` | Close cancels every owner lifecycle, interrupts startup/in-flight calls, and concurrently terminates owners and removes directories through the blocking pool under one deadline. Shutdown grace applies separately to close and cleanup join. It must cover TERM grace + kill wait. Private directories remain across resets and are removed/absence-checked at close, including partial setup failures; already-missing roots count as cleaned, and a removal running at deadline may finish in the background. Deadlines can be retried; quarantine blocks reuse/close and live_runners exposes survivors plus idle exits awaiting the next owner operation. Crabber shutdown ignores cleanup errors and returns Ok. |
| `platform` | Linux/macOS execution. Non-Unix construction rejects before paths or limits are touched; Windows compilation is checked locally. |
| `inputs` | Exact fields, bounded nonempty UTF-8 code without NUL, whole-second optional timeout; zero/omitted uses positive default and null is rejected. Parsed duplicate keys are serde_json's input boundary. Crabber can reject schema/NUL before the executor; direct executor tests cover fixed extension codes. |
| `scope` | Owner is durable session plus workspace ID with a fixed canonical root; max_sessions is a mount-lifetime admission budget, including failed setup. Clear of unknown/canceled owners consumes none. Each owner has a FIFO gate and bounded waiters excluding the holder; separate owners progress concurrently. Closing any mount closes its shared instance. Detached descendants are outside cleanup. |

Public tests prove durable values/events/provider input, permission outcome
classes, inline maximum fields, timeout/cancellation resets, queue and lifetime
budgets, atomic registration, session visibility, remount, redactor/jobs
composition, all 20 paused-resume drift rows before persisted mutation, and
successful resume with changed environment entries or temp root. Crabber fixes
workspace identity at session admission: workspace-key independence and root
drift are additionally exercised through its public executor/context API.
Parallel Agent settlement preserves call order, so queue overflow is observed
through a public result transform before releasing the holder, then checked
in durable records. Unit seams prove cancellation commit boundaries, unreaped
leaders, quarantine and cleanup failures. These local Linux results do not
claim hosted CI or macOS execution. CI enforces Python 3.11/3.14 on both OSes
with PYTHON_REPL_REQUIRE_PYTHON=1. Remaining recovery/composition work is tracked
under crabber-extensions-2bed.

## RTK reducer

| Key | Rust boundary and deliberate deviation |
| --- | --- |
| `version` | v0.51.0 instead of the reference v0.48.0. The opt-in suite asserts `rtk 0.51.0`. The official `checksums.txt` checks archives; hosts verify the archive and pass the separately computed extracted binary digest as `RTK_REDUCER_TEST_SHA256`. CLI names were verified against v0.51.0 source. |
| `filters` | Closed CargoTest/GoTest/GitDiff/Log enum; `cargo-test` is added beyond the reference. Hosts assert formats. |
| `representation` | Parsed Value with Text or top-level JsonFields strings; no raw byte mirror, nested paths or whole-result serialization. Unselected members move unchanged. Jobs/Python stdout/stderr objects are not selectable. |
| `registration` | Exactly one typed ordinary transform, explicit order, no order-too-late rule. Final redaction follows ordinary handlers regardless of order. The transform never returns Err or marks errors. |
| `classes` | Only Succeeded, resolved, normalized-input calls with is_error=false reduce. Failed, denied/asked, unknown and prepare-failed values pass through without spawning. |
| `limits` | Eleven positive fingerprinted fields; bindings/fields/matches≤256, concurrent≤64, config/input/field/stdout≤1 MiB, stdout≤field, timeout/reap≤60 s, close≤120 s and ≥timeout+reap. MaxJSONDepth/MaxJSONNodes/MaxStderrBytes/MaxResultBytes removed; shutdown_grace added. |
| `lease` | One non-queueing permit and deadline across all selected fields; retained through reaping, or forever on unproved reap/panic. |
| `environment` | Seven private keys and five fixed values, env_clear, no PATH. XDG_STATE_HOME/XDG_CACHE_HOME added beyond the reference. The no-helper inference was exercised by all four real filters on Linux aarch64. |
| `stderr` | OS-level discard; RTK panic-passthrough warnings cannot be observed. |
| `never-worse` | RTK may echo raw input under its token-estimate guard; identical output is no gain. Acceptance requires marker-inclusive compact JSON string encoding to be strictly smaller, not fewer tokens. |
| `input-cap` | RTK's 10 MiB stdin cap exceeds our hard 1 MiB selected-field ceiling. |
| `hash` | Versioned Rust tuple includes digest, release, marker/environment contracts, canonical bindings, order and all limits. Executable/temp-root paths excluded. Different from the Go hash. |
| `digest` | Construction-only streaming identity check, ≤128 MiB regular file with at least one execute bit; executable write permissions and ownership are not restricted. No spawn recheck, provenance or architecture assertion. Host keeps bytes immutable; synthetic scripts pass by design. |
| `temp-root` | Host euid ownership and no group/other write required; conventional world-writable /tmp fails these checks. Ancestors not inspected. Non-UTF-8 roots fail invocation environment construction. |
| `host-crash` | Stale .rtk-reducer roots are never swept. Root collisions retry suffixes (up to sixteen attempts); host owns orphan and stale-state cleanup. |
| `diagnostics` | Pull-only independently read wrapping Stats counters and quarantine/closed flags replace silent passthrough. Reduced counts accepted calls; no_gain counts fields, rather than successful subprocess output. No paths/PIDs/text exposed. |
| `cancellation` | Callback drops; tracked task kills, reaps and removes before releasing its permit. With the reducer first, Crabber persists fixed interrupted text. Earlier ordinary handlers may already have established an accepted value. The metadata probe therefore follows the reducer in interrupt proofs. |
| `close` | Bounded engine-owned closing token plus mount tracker joins. Invocation removal is bounded by kill_wait; a timed-out removal may finish in the background and is retried by close. Linux settlement can take timeout+2×kill_wait; macOS can add another kill_wait for group absence confirmation. Hosts size the registry mount-close timeout above cleanup (default five seconds). Directory failures quarantine and remain visible for retry; unproved reaping remains a survivor even after directory cleanup. Shared instances are single-use after any mount closes. |
| `cleanup` | Descendants leaving the group and host SIGCHLD auto-reaping/waitpid(-1) are out of contract. Private trees are not a sandbox. |
| `recovery` | Paused Pending calls execute once on resume with reduction; Running calls settle Interrupted without transform under Crabber D10. No retry_safe metadata because this is not a tool. Running-call sweep proofs remain open. |
| `platform` | Unix execution only, non-Unix construction rejects first. Linux CI proves synthetic plumbing; the historical Windows check was compile-only and macOS is no longer in CI. Real RTK behavior has been verified on Linux aarch64, not macOS. |
| `scope` | Install one reducer per registry. Crabber permits repeated mounts and runs each handler; shared instances share capacity and close together. |
| `proof` | Synthetic /bin/sh fixtures prove plumbing, lifecycle and acceptance arithmetic, not filters. Opt-in official v0.51.0 Linux aarch64 run preserved all sentinels and ambient RTK directories; encoded bytes: cargo 1426→535, go 17070→199, diff 201→161, log 14799→202. Hosted CI is tracked separately. |

Lossy filters may truncate secrets into prefixes redaction cannot recognize; mount
a final redactor to protect recognizable secrets in reduced text. This does not
protect arguments or raw host artifacts. No installer, auto-detection, passthrough
mode or project TOML filters exist. Nine-feature composition, nested-path candidates,
and Running-call recovery sweeps remain outside this slice; final acceptance is
tracked under `crabber-extensions-2bed`, and full nine-feature parity is not claimed.

## Verification and limits

The live suite is compiled with the feature in Clippy on Linux CI and
runs only its two-skip credential-free path on Linux CI. Hosted CI does not run
live journeys or receive a provider secret. Local require-flag execution is
recorded above; a skipped test is never counted as live acceptance.

The containing workspace runs formatting, Clippy, tests and documentation
through `cargo xtask check` on Linux. Consumer qualification fetches the public
Crabber repository at one revision over HTTPS while preserving its SSH Cargo
source identity. macOS CI coverage from the satellite is dropped by owner
decision; the timing defect tracked as `crabber-extensions-6tg5` remains open
until its migration is recorded. Local Linux results do not claim hosted or
macOS acceptance.

Instruction tests cover exact typed frames, regular/missing/invalid/oversized
files, longest boundary selection, symlink refusal, pinned root handles, reader
errors, live refresh, wrong-root omission, concurrent workspace isolation and
retained deadline/cancellation cleanup. Existing redactor tests cover
JSON bounds, token boundaries, pattern validation, identity hashes, composition
and protected persistence. Further feature tasks remain open until their own
contracts and combined acceptance pass.

Ask-user tests cover Store-compatible response schemas and escaped size bounds,
configuration identity, cold-agent continuation, exact replay, authority and
revision refusal, foreign targets, selected/custom/dismissed/expired outcomes,
permission preflight, final result redaction, parallel complete-set barriers and
explicit recovery of a lost unstarted claim. Expiry uses a controlled Store clock;
these are scripted tests without credentials.

Delegated-task tests cover configuration, authoritative request/workspace routing,
durable outcomes, permissions, sanitized faults, input and output bounds,
parallel capacity, cancellation, slot retention until exit, bounded shutdown,
shared mounts, and host-owned child-agent completion and interruption.

Web-search tests cover configuration and fingerprints, persisted runtime results,
permissions, input byte bounds, sanitized faults, URL reference verdicts and
field bounds, cancellation/deadline/drop/capacity lifecycle, and session routing.

Command-analysis suites cover contract, input, reference corpus, differential,
shell, budgets and robustness. The [fixture reproduction instructions](../crates/crabber-extensions/tests/command_guard/fixtures/README.md)
describe the overlay generation of 599 original expanded cases and the 10,888-row
Go differential corpus. The Unix suite checks accepted syntax, exact argv and
synthetic execution canaries, and skips loudly if Bash 5+ is absent or `/bin/sh`
is an older Bash. `COMMAND_GUARD_REQUIRE_SHELLS=1` forbids skipping. The PS4
implicit-execution proof uses Bash 5 as `sh`; this machine's dash does not expand
that inherited external substitution. The 512 KiB debug stack and two-second
wall-clock checks are local Linux evidence, not macOS or hosted-CI results.

Command guard integration adds deterministic unit seams for saturation and panic,
public registration/contract and concurrency checks, durable runtime journeys,
a 22-row paused-resume drift matrix and a Unix `/bin/sh` canary. Hard-cap Agent
proofs use default and explicit 2 MiB workers. During durable-host qualification,
the explicit 1 MiB case overflowed locally on its first hostile-nesting input;
macOS CI also aborted in this integration target. The earlier 1 MiB result does
not qualify the new pin. The fixture now asserts the supported 2 MiB bound while
retaining direct parser proofs at 512 KiB. Two plan
expectations were corrected against the immutable implementation: exact-limit
word fixtures abstain, and a two-call turn prepares twice before pause, zero
times on resume. The depth-31 admitted substitution matches a deny rule; depth
32 exhausts the depth budget. Full nine-feature composition remains open.

## Pin 3ff793c dependency additions

This historical durable host API migration adopted Crabber
`3ff793c982aa1fc31e5d3a2bbe459f753d7bbf8e` (tree
`7918c51e0ef65d9c2188bbea35341aa0cad7e6d0`) before repository consolidation.
At that point six Crabber packages resolved to one immutable Git source and
Rust was 1.99.0. Current dependencies use the containing workspace.

| Added normal dependency | License | Purpose |
| --- | --- | --- |
| base64 0.22.1 | MIT OR Apache-2.0 | Existing resolved package, newly used by Crabber core |
| json-patch 4.2.0 | MIT / Apache-2.0 | Crabber public-source state patches |
| jsonptr 0.7.1 | MIT OR Apache-2.0 | JSON pointers used by json-patch |

No packages were removed except the six old Crabber source entries. The other
lock changes reflect Crabber's changed dependency edges; jsonschema was already
in this workspace's resolved graph.

Every JSON tool uses Crabber's explicit `JsonToolExecutor` adapter. Results
remain literal JSON text, with the same bytes for unchanged values. The final
redactor scans JSON values and plain text blocks; any non-text output is
replaced by the quoted JSON string `"[REDACTED]"`, preserving the error bit.
RTK passes non-JSON typed output through unchanged.

Crabber bounds JSON depth at 32 and serialized typed output at 4 MiB. The
following lower hard caps account for worst-case JSON escaping and the outer
text envelope. Their configuration hash versions changed, so old frozen plans
must be replaced. The upstream interaction/resource contract versions also
prevent resuming plans from the previous pin.

| Tool | New hard cap | Conservative inline JSON bound | Depth |
| --- | --- | --- | --- |
| delegate_task_wait | 320 KiB aggregate raw output, 64 IDs | 1,974,336 bytes | 4 |
| background_job_status | 160 KiB per stream | 1,967,104 bytes | 3 |
| python_repl | 100 KiB each stream/result/exception | 1,844,224 bytes | 3 |
| web_search | 12 results; field caps unchanged | 1,843,633 bytes | 3 |

Other tools remain within the bounds: inline delegation/status use at most
256 KiB raw output; delegation lists carry at most 256 fixed-size metadata rows;
job lists carry at most 256 fixed-size summaries; ask_user returns at most
10 KiB answer text; start/kill/clear return small fixed envelopes. These all have
JSON depth below 32. The largest RTK selected field remains 1 MiB, while
unchanged ordinary tool outputs already passed Crabber's typed-output validation.

The adopted runtime evaluates guards during preflight before an interrupt-policy
pause, then evaluates them again on resume. Registration scope now contributes
to the frozen plan, so changing Global to Session refuses resume with PlanChanged.
PermissionDecision::Ask pauses with a durable approval and does not execute a
tool until authorized; it is not a completed denial.

Interrupting an executing native tool can return PhaseOutcomeUnknown and leave
its call Running without a result, with the exact phase Uncertain. This differs
from an interrupt during a result transform, which can still settle Interrupted.
A joined callback, reaped process or cancelled runner proves local cleanup only;
it cannot establish a durable outcome. Hosts retain the phase target, reconcile
its outcome through Crabber's phase APIs, and record cleanup separately before
resuming or reusing the session. Tests preserve this distinction rather than
fabricating interrupted completions.

## Contextual ownership

`background_jobs::Factory`, `python_repl::Factory` and `delegate_task::Factory`
freeze their options once and create fresh state for each `FactoryScope::Session`
owner. Register them with `AgentBuilder::extension_factory`. Workspace scope
instead shares one instance and its capacity across sessions in that workspace;
its internal jobs, interpreters and task records remain keyed by session as well
as workspace. Factory creation and installation start no business work.

Bound instances reject a foreign workspace or session before inspecting mutable
state. Process tools also require the admitted directory; Python retains its
canonical-root drift check. No process working directory is changed.

| Registration | Ownership and capacity |
| --- | --- |
| WorkspaceInstructions, ToolResultRedactor, RtkReducer, CommandGuard, WebSearch, AskUser via shared_extension | Shared immutable policy; any semaphores/counters are deliberately shared |
| BackgroundJobs, PythonRepl, DelegateTask via shared_extension | Existing shared capacity and instance-wide close; records remain owner-keyed |
| The three Factory adapters with Session scope | Independent state/capacity and close per session |
| The three Factory adapters with Workspace scope | One instance per workspace; session close leaves that workspace instance alive |

An optional `SharedCapacity` pool adds a nonqueueing aggregate bound across
factory-created instances. Job permits last through reaping; Python permits last
for the interpreter's lifetime, including idle state, reset and quarantine;
delegation permits last until the runner future exits. Exhaustion uses the
existing unavailable/capacity-exhausted outcome. The pool's limit enters factory
identity; the pool object and owner identity do not enter instance configuration
hashes. Without a pool, hosts size the multiplied per-owner budgets themselves.

`owner_report()` and `Factory::owner_report(workspace, session)` expose only
counts and lifecycle flags. Workspace-scoped reports take `None` for session.
The observer keeps weak references to the inner policy so retained runner tasks
remain observable after a bounded owner close. Owner cleanup remains retained
until its bounded close attempts converge; an observer deadline is not a join.
Shared-instance targeted cancellation and per-owner admission policy remain
`crabber-extensions-4rz6`.

## Scoped cleanup and host control

Use Session-scoped factories when one session must close independently. Stop
new ingress before cleanup, then:

1. Join that session's execution workers with
   `Agent::drain_session_execution_tasks(session, bound)`. Interrupt active
   runs separately when needed; draining does not request interruption.
2. Cancel and observe its detached jobs with
   `Agent::drain_session_invocations(session, true, bound)`. Inspect each
   report's status, `local_joined`, and pending/failed resources. Repeat while
   work remains; cancellation acceptance and terminal task rows are not joins.
3. Call `Agent::close_session_resources(context, bound)` for factory-owned
   jobs, interpreters and delegation records. The caller bounds observation;
   each instance also bounds its own close/join steps. A
   `RuntimeError::ExtensionPlan(ExtensionError::ContextCleanupIncomplete)`
   retains cleanup ownership for a later call. Repeated/concurrent close is
   safe; neither an expired waiter nor an instance grace abandons ownership.
4. For whole-host shutdown, inspect `Agent::close(bound)`. Only a Complete
   observation with a drained report proves local cleanup. Native interruption
   may leave NativeOutcomeUnknown even after workers join; report and reconcile
   that durable uncertainty instead of claiming a clean stop.

`Factory::owner_report(workspace, Some(session))` keeps weak observations of
retained policies: a noncooperative runner stays Some/live≥1 after bounded
close. Closing/cancelled records can still own permits and process groups.
After actual cleanup, reports are absent or closed with live=0. These are
metadata snapshots, not durable owner acknowledgements.

Workspace-scoped factories close with their workspace. A shared instance has
instance-wide close; it cannot target one session independently. Factory mounts
deliver targeted session cleanup, and runtime session-invocation drains control
detached delegation. `SharedCapacity` bounds the aggregate without supplying a
per-owner admission policy. Targeted operations within a shared instance and
per-owner limits remain open in [`crabber-extensions-4rz6`](https://github.com/mattsp1290/hub/blob/main/projects/crabber-extensions/issues/crabber-extensions-4rz6-follow-up-host-control-of-background-delegation-by-session.md).

Private registries and interpreter state are fresh after restart. They cannot
report an old process as resumed or stopped. A fresh controller's empty local
drain is not completion: durable detached owners may remain Active. Inspect
`Store::get_session_owner` using the deterministic identity; only the controller
that owns a live job can join it locally. No automatic same-original recovery or
exactly-once side effects are promised. Crabber's
[session-owner contract](session-owners.md)
describes the PreparationRecoveryRequired liveness limit. This adoption does
not resolve the upstream crabber-btrf constraint.

The cleanup integration target proves process-group disappearance for one owner
while a second session remains usable, a slow TERM handler surviving the first
200ms observation, later successful close, retained noncooperative capacity,
truthful whole-host observations, and detached-child survival across parent
interruption. The delegation detached journey separately proves cold-cache
absence while the stored owner remains Active; the external consumer records its
two-process SQLite qualification separately.

## Durable host adoption record

The historical Linux qualification on 2026-10-20 used Rust/Cargo 1.99.0, scripted providers,
synthetic data and no provider credentials. It does not claim complete feature
parity or downstream consumer acceptance.

| Identity | Qualified revision |
| --- | --- |
| Crabber | `3ff793c982aa1fc31e5d3a2bbe459f753d7bbf8e` |
| Crabber tree | `7918c51e0ef65d9c2188bbea35341aa0cad7e6d0` |
| Extensions production baseline | `ce6044d12e7c54e5a84a14b77961a5391c1eaa18` (WP1–WP6) |
| Compatible standard tools | `d982ab75e6cf507690fb2d0d92f2ee61dd5a7b28` |
| Exact Crabber source URL in all three repositories | `ssh://git@github.com/mattsp1290/crabber.git` |

The [external consumer](../testdata/extensions-consumer/README.md) creates a fresh
Cargo workspace outside this repository and pins runtime and extensions to one
pushed `CRABBER_REV`. It generates its lock once and uses `--locked` thereafter.
It checks every resolved Crabber package shares that source, each package has
one version/source identity, and no duplicate Crabber roots exist. HTTPS fetch
rewrites preserve the SSH Cargo source identity. Qualification of later feature
heads and their exact generated lock hashes is recorded in the PR; this table
names the pre-consolidation production baseline rather than a self-referential documentation SHA.

The dedicated host composes all three Session factories with aggregate capacity,
shared instructions/redaction/AST guard/AskUser and a custom extension, across
three concurrent sessions in two workspaces. It asserts inline, detached and
nested delegation, instruction roots, Python state, permission Ask followed by
host denial, durable-answer replay, scoped close, actual process-group absence
and unchanged process cwd. The separate [standard tools consumer](../testdata/standard-tools-consumer/README.md)
qualifies tools, command guard and final redaction together at the same Crabber
revision. Six public-contract integration targets are exported from that
immutable revision and compiled against Git dependencies:
These tests cover durable input edge cases, attribution, isolation and cleanup.
These reuse the repository's public test journeys; they are not a second
implementation of those contracts.

SQLite restart runs in two separate processes. The first commits a detached
owner and paused question, then exits without graceful cleanup. The second
answers/continues with replay checks and observes the old owner still Active
with zero local invocations or resumed runners. It cannot claim that an empty
cold registry joined old work. No external business side effect is run by that
pending restart runner.

Local workspace qualification includes formatting, both default/live-feature
Clippy, the complete default test suite, the credential-absent live skip path, rustdoc with
warnings denied, dependency identity and whitespace checks. Process fixtures are
required. Tests use two harness threads on the qualification host; earlier
unchanged RTK timing failures under default concurrency remain unexplained, so
this record does not assert those failures were fixed. Credential-present live
suites and macOS runs are not claimed here; the consolidated CI matrix is Linux-only.

The [attribution coverage matrix](#delegation-attribution-coverage) remains authoritative:
inline calls have phase/call attribution, detached jobs have invocation identity,
and neither provides an independent child run identity. Cancellation appears in
activity statuses and InvocationFinished rather than a new public cancel event.
Hosts must supply correctly rooted readers and canonical directories; mismatched
custom roots can silently omit files. SharedCapacity is aggregate admission;
shared-instance targeting and per-owner admission remain 4rz6. The upstream
`crabber-pxdk`, `crabber-09ku` and `crabber-btrf` constraints remain, including
no automatic original-job recovery or exactly-once effects.

Post-merge adoption replies are tracked by `crabber-extensions-h5ro`. They require
the actual merge SHA and tree comparison and keep requests open with an `amend`
verdict pending maintainer confirmation of inline attribution, aggregate-capacity
scope and the public diagnostics/reader adapters. Opening this PR does not
constitute consumer acceptance.


Qualification also corrected synthetic RTK fixtures that exited without reading
stdin: the intended natural-exit, exit-status and overflow cases now drain input
before producing output. Dedicated early-close and large-input write-failure
cases remain. This removes that fixture classification race without changing
production failure precedence; it does not explain the older Spawn failures.
Delegation capacity uses field ownership through final publication, with a gated
regression covering completed and interrupted publication.
