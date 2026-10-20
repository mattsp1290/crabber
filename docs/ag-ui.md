# Live AG-UI embedding

Add `crabber-agui` alongside `crabber`. The adapter pins `ag-ui-core` 0.2.0 at
[revision 705303b26b6b900cdf9bdfa456fbd92a7003ca6b](https://github.com/mattsp1290/ag-ui/tree/705303b26b6b900cdf9bdfa456fbd92a7003ca6b).
The example pins `ag-ui-client` and the adapter pins `ag-ui-encoder` at that same revision. Root Cargo's git/revision
patch unifies the client and encoder registry dependencies with the adapter's core types;
there are no external checkout paths. See [adapter API usage](../crates/crabber-agui/README.md).

Subscribe with `run.events()` before consuming `run.done()`. Create a `Projector`
using the actual runtime session/run IDs and separately selected wire aliases.
Feed each received `EventRecord` into `push`; encode returned typed SDK events
with `encode_sse` (SDK live encoding); use `encode_sse_recorded` when replaying
recorded events. Drain the receiver and await `done`, then call `finish` using
`Completion::{Completed, Cancelled, Paused, Failed, LeaseLost}`. The host retains
the handle while draining so it can interrupt on receiver/transport faults.
A task error outranks an earlier durable success hint. Call `fail` on lag;
never restart an executor to repair a stream. One terminal is emitted, and
repeated finish emits nothing.

| Source event | Wire behavior |
| --- | --- |
| Native attempt / phase / run-control update | Bounded, named ACTIVITY_SNAPSHOT containing only public target/state fields; opens the wire exchange if necessary |
| RunResumed | A typed acknowledged resume initializes current state and exact frozen-call identity; null emits nothing and never opens an exchange or implies a fresh native start |
| RunStarted | Observe actual source startup; emit RUN_STARTED only if the exchange is not already open |
| MessageStarted | Remember the actual assistant ID; no empty text event yet |
| TextDelta | First nonempty delta opens TEXT_MESSAGE_START; then CONTENT |
| ReasoningDelta | Only explicitly attributed public summaries may be displayed with the opt-in; each actual reasoning item has separate boundaries |
| ToolCallStarted | Open the assistant text container once, then TOOL_CALL_START with actual call and parent IDs |
| ToolCallArgsDelta / ArgsCompleted | Original provider argument fragments / one TOOL_CALL_END |
| MessageStreamEnded | Close text/reasoning; failed public attempt permanently faults projection |
| MessageCommitted | Validate the persisted assistant identity; no duplicate text/snapshot |
| ToolCallSettled | TOOL_CALL_RESULT with stored tool-message ID and post-transform content |
| ContinuationWaiting / RunPaused / RunSettled | Use the committed waiting summary and terminal hints; await task completion before terminal |
| TurnStarted / TurnCompleted | STEP_STARTED / STEP_FINISHED using the actual turn identity |
| Typed public state snapshot / delta | STATE_SNAPSHOT / STATE_DELTA, retaining whole null and complete RFC6902 operation objects |
| Typed activity snapshot / delta | ACTIVITY_SNAPSHOT / ACTIVITY_DELTA; only explicit snapshots may retype an identity |
| Admission, permissions, running calls, epochs, legacy extension/custom records | No public output; no raw payload pass-through |

Tool result `content` uses the SDK's typed message parts. Text and explicitly
typed image, audio, video and document sources retain their ordering and their
Data/URL/File interpretation. The adapter never fetches media or guesses a source
from an untyped URI. The actual tool error flag is carried in event metadata at
`crabber.isError`; a failed tool result can belong to a successful run. Nested
result parts retain their originating call/error metadata. ProviderState and
private/unspecified reasoning are omitted recursively. A generic reasoning display
flag cannot make private data public; only an explicit public-summary provenance
and item identity qualify for the opt-in. Associated provider continuation and
signature data are always private. `public_tool_content` remains a legacy JSON
inspection helper; it is not the TOOL_CALL_RESULT wire representation.

Native activity projection is an allowlisted presentation, not a native audit or
authorization check. It excludes authority tokens, resolver claims, private
settlement/checkpoint material and source cursors. A wire RUN_STARTED opens the
selected exchange; it does not manufacture a Crabber RunStarted record or permit
execution. Message, text, tool and turn presentation still require the actual
source RunStarted or an explicit acknowledged continued-resume source. Native failure before that point produces an honest RUN_ERROR,
not successful completion. A second actual source start is an order fault even
when an earlier native prelude already opened the wire.

Public-state deltas require an actual preceding snapshot in the exchange. Required
whole state null, scalar/array state and nested null remain unchanged. Legal patch
extension members survive encoding; reserved-member shadowing is a permanent
malformed-source fault. Activity content must be an object, as required by the
pinned SDK. An unrepresentable legacy activity or disappearing activity in a
legacy full replacement fails explicitly rather than leaving silently stale UI
state. Hosts may bind a trusted `PublicSourceIdentity` with `bind_source` before
opening the exchange. Workspace, session, run and attempt changes cannot rebind
an already selected source; this identity data confers no native authority.

Message and call IDs come from runtime/provider identities. Each provider attempt
gets a different assistant message ID. A tool-only response has an empty text
container so the pinned client does not create duplicate assistant messages.
Concatenated successful text equals the stored assistant text; normalize an
absent stored text block and an empty protocol container equivalently.
Argument stream end proves presentation completion, not execution or persistence.
MessageStreamEnded is separate from MessageCommitted. Text and argument deltas
are append-only chunks for their original message/call IDs, not replacement
values. Feed each source event once in its original order; the projector does not
deduplicate repeated deltas or supply reconnect replay. A failed attempt with
public text, tools or displayed reasoning faults the wire even if runtime retry
later succeeds. An attempt with only filtered reasoning can retry. Interrupted
partial assistant messages retain the streamed ID. Compaction summaries remain
internal.

Timestamps use checked Unix milliseconds, omitted outside JavaScript's safe
integer range. Unselected raw events, source cursors, backend errors, provider state,
claims and private configuration are never forwarded. Wire aliases carry no
runtime recovery or admission authority.

Defaults cap whole SSE frames, message text and each call's arguments at 1 MiB, open
unsettled calls at 64, message attempts and activity identities at 1024 and batches at 256. Public reasoning item identities also have a bounded count. Configuration
can reduce these limits; zero or values above the defaults are invalid. Aliases
must be nonempty, at most 256 bytes, and contain no controls. Construction checks
that fixed startup/error/cancellation terminals fit the event limit; variable
interrupt summaries and closures undergo the same frame and batch checks. Metadata and
counters are bounded; complete transcripts and streamed text/arguments are not retained. The current public-state contents and one waiting summary are retained within the event bound.
Closed identities remain for bounded duplicate detection. Oversized JSON is
rejected by the non-allocating `frame_budget` check before SDK normalization or
frame allocation. This conservative check includes delimiters; optional-null
normalization can make the final frame smaller. The example reserves this
conservative byte budget before encoding and holds it until the frame is dropped;
queues that charge the encoded size must account for `frame.len()`. No truncation
is used.

Completed -> RUN_FINISHED success; cancellation -> cancelled. A pause requires
an actual typed `ContinuationWaiting` summary; a bare RunPaused cannot invent an
interrupt. Human interrupts expose the actual interaction IDs and explicitly
declared public prompt/schema. Object schemas retain their members; boolean
schemas use the equivalent JSON Schema object `{"allOf":[originalBoolean]}` at
the pinned SDK's object-only boundary. Other schema shapes fail explicitly.
A frontend-only wait returns success with the exact authoritative pending tool
call IDs, without claiming the native run completed. A mixed human/frontend wait
returns interrupts and repeats the authoritative frontend set on each interrupt
at `metadata.crabber.pendingToolCallIds`. Clients must preserve both controls;
answering a question does not supply a frontend result. The metadata continuation
is only the public `{id, generation}` identity, never a checkpoint or credential.
A backend-pending set without a declared human question is not converted into a
synthetic interrupt or a completed tool result; an unrepresentable pause ends in
a finite projection error.

Interrupt `expiresAt` is currently omitted. Native interaction snapshots carry
the actual expiry, and the host must check that deadline when answering; wire
absence is not a promise that the question remains valid indefinitely. The
adapter does not derive an expiry from a run lease or presentation timestamp.

A new continued exchange consumes typed RunResumed with actual frozen calls and
committed parent identities. It does not emit duplicate TOOL_CALL_START events.
The host must supply the canonical input history to the SDK client and commit
native frontend/human continuation operations; browser results and IDs are never
execution authority. Malformed/foreign summaries fail permanently, and legacy
null RunResumed cannot make business presentation legal. Supplied storage message
and part IDs are store-global across sessions and tenants. Validate incoming
history against host-owned canonical history, then admit only its new suffix
with globally unique storage IDs. Retain the exact original envelope, admission
key and captured revision across an uncertain retry; do not strip or regenerate
IDs to reconcile it. A generic conflict does not identify a particular collision.

Runtime failure/lease loss -> RUN_ERROR. Missing/invalid boundaries, lag,
source-attempt failure and limits -> finite `crabber_*` error codes with a generic
message. Valid opened boundaries close before terminal when possible. If a host
cannot deliver a terminal because its socket is closed or exhausted, it must
report transport failure locally rather than claim delivered success.

### State, activities and child exchanges

State and activity identities belong to the session, so their SDK events remain
root-owned. Allowlisted metadata preserves the actual publishing run, attempt,
call and invocation identities/generations. A different child publishing an
activity delta cannot change that activity's SDK owner. Activity IDs may not
collide with assistant, result or native-activity messages.

Source-tagged child text has independent message slots and actual invocation
lifecycle events. A parent exchange emits scoped SUBAGENT events. If an actual
parent finishes while an independently detached child remains active, emitted
child presentation closes with `crabber_projection_scope_closed`; this does not
claim the child's execution finished. Late facts require a session-tail reader
or a separate child exchange. A host can select that exact invocation and
generation with `bind_source`; only its matching actual InvocationStarted opens
the child exchange, and actual Finished/Error supplies terminal truth. Selection
alone is not startup, permission, recovery, Main or resource ownership.

MESSAGES_SNAPSHOT replaces global SDK history. Partial child snapshots on a
parent exchange are rejected. Root or explicitly bound-child snapshots require
closed presentation scopes and representable public text history; prior tool,
media or displayed-reasoning history cannot be silently replaced by text-only
source data. The adapter preserves its current activity messages in snapshots,
so later activity deltas retain their real base. It does not reconstruct missing
parent history. RAW/CUSTOM hooks require explicit bounded `raw_sources` /
`custom_events` allowlists and typed source-bearing events; arbitrary legacy
custom records never pass through. Tool progress requires a known current call. Runtime `ToolContext::progress` is
text-only and best-effort: it drops non-text or oversized content. Separately,
authenticated native public-source progress validates text only. The adapter can
represent supported media through its typed-content conversion and private-data
filtering, but this supplies no native media-progress publication route.
Progress does not imply tool completion.

## Typed wire input

`convert_run_input` uses the pinned SDK's `normalize_run_input` after a 4-MiB,
depth-32 preflight. The HTTP transport must bound its buffer and JSON parser
before calling it. Input state absence or whole null normalizes to `{}`; nested
null is preserved. Required output-state null follows a different rule and stays
null. The returned normalized wire value and SDK diagnostics remain accessible.
Strict conversions reject destructive `UnknownVariant` and `UnknownEnumValue`
diagnostics rather than silently choosing a lossy policy.

The host supplies the captured revision and `AdmissionKind` when constructing
the runtime envelope. Wire thread/run/parent IDs, resume entries and forwarded
properties do not establish workspace, session, resolver or execution authority.
UI activities remain separate from provider messages; child activities are not
accepted by the input conversion. Ordinary User part IDs are retained. Result
part IDs, developer/reasoning/named/encrypted/subagent messages and file-provider
hints have no supported runtime representation and fail explicitly. URL/file
media require their declared MIME type; it is never inferred or fabricated.

Frontend tool parameters describe inputs, not output schemas. Tool-result
message IDs remain available in normalized wire data and the envelope for host
canonical-history validation. `frontend_results` returns call-bound response
data only; a host must still validate the exact authoritative pending set and
commit its native continuation operation. An output message or browser resume
entry is never an execution permit.

## SDK pin verification

At revision `705303b26b6b900cdf9bdfa456fbd92a7003ca6b`, a scratch checkout's
`cargo test` under `sdks/community/rust` passed the core, encoder and client
suites on 2026-10-15. Four client tests were marked ignored upstream. This is
an observation of the pinned SDK suites, not closure of its consumer requests.
`cargo xtask check` verifies that every AG-UI Git dependency in repository Cargo
manifests selects this full revision, including independent consumer workspaces.
The example's client check covers ASCII HTTP runs; its independent byte-first
decoder checks Crabber framing separately.

## HTTP example

```sh
cargo run -p agui-sse -- --listen 127.0.0.1:3000
```

In another terminal:

```sh
curl -N -X POST http://127.0.0.1:3000/run \
  -H 'Content-Type: application/json' \
  --data '{"threadId":"demo","runId":"wire-run","messages":[{"id":"client-user","role":"user","content":"hello"}]}'
```

Expect RUN_STARTED, TEXT_MESSAGE_START, TOOL_CALL_START/ARGS/END for each echo,
TEXT_MESSAGE_END, TOOL_CALL_RESULT, a later assistant text START/CONTENT/END,
and RUN_FINISHED with success. Each compact UTF-8 JSON event occupies a single
`data: ...\n\n` frame. JSON escapes embedded CR/newlines. No SSE `id`, `[DONE]`,
Last-Event-ID, reconnect or replay is implemented.

The request profile accepts exactly one text User message up to 64 KiB, IDs up
to 256 bytes, protocolVersion absent or 1.0, empty/absent tools/context, and
null/empty/absent state/forwardedProps. It rejects unknown top-level properties,
parentRunId/resume, transcript arrays, client tools and multimodal parts.
The client message ID is not imported into storage. Malformed JSON -> 400;
unsupported profile -> 422; request/admission deadline -> 408; body over 128 KiB -> 413; busy thread -> 409;
capacity/shutdown -> 503; startup failure -> generic 500. Admission occurs
before successful SSE headers.

`threadId` becomes the stored SessionId. Before preparing the first admission,
the example's `ensure_thread` commits an exact deterministic `ManageSession`
creation and captures its resulting revision. Its ThreadStore wrapper forwards
the original requests and capabilities to the same Memory store; it does not
rewrite admission or mint execution authority. Ordinary runtime
`prompt(Some(session))` requires an existing session. The example owns one
workspace/directory and process-local history.
POSTs are not idempotent; later repeated requests can start another turn.
Production retries use [keyed admission](admission-receipts.md).

The host caps owned requests/runs at 8, queued data frames at the configured
`max_batch` (256 by default), total pending
SSE bytes at 2 MiB (8 KiB reserved for final control frames), and cleanup at
5 seconds. One absolute 30-second deadline covers body reading, initialization,
admission and execution. Shutdown closes the ingress gate and joins request
owners as well as workers; admission that outlasts its HTTP response stays
tracked until its result is known and any late handle is interrupted and joined.
Oversized final closure batches become one finite limit RUN_ERROR. Queuing never blocks the runtime observer. Overflow/lag
permanently faults projection, interrupts, drains and awaits completion. A
reserved terminal path avoids relying on the full data queue. Dropping the
response signals cancellation. Noncooperative work remains in the host task
registry after the cleanup deadline; shutdown reports unresolved workers and
exits unsuccessfully if they cannot be joined. Embeddings supply authentication,
thread authorization, trusted tools, idempotency, public-content policy and
transport scheduling.

`cargo run -p agui-sse -- --check` executes real loopback HTTP, a real fake-provider
runtime, native tools, the pinned ASCII HttpAgent and an independent byte-first
Unicode decoder. The decoder deliberately splits every byte, including inside
Unicode scalars and across delimiters. Runtime/store reconstruction is checked.
`cargo test -p agui-sse` covers failure and host lifecycle cases.

The pinned client's SSE parser decodes HTTP chunks independently; split UTF-8
scalars can fail there. ASCII pinned-client interoperability and independent
Unicode transport correctness do not prove arbitrary Unicode client support.
The non-blocking maintainer request is local at
`$HOME/.agents/projects/ag-ui/requests/2026-10-01-rust-sse-utf8-boundaries.md`;
no upstream fix or acceptance is claimed. Any future pin must be immutable and
rechecked. Browser/Dojo interoperability is not established by that HTTP check. The example
request profile remains narrower than the typed adapter API; native continuation
authorization and canonical transcript policy remain the host's responsibility.
