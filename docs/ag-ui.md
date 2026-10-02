# Live AG-UI embedding

Add `crabber-agui` alongside `crabber`. The adapter pins `ag-ui-core` 0.2.0 at
[revision 5cc34b11fb41c6f068c27410b5756dbebcb367dd](https://github.com/mattsp1290/ag-ui/tree/5cc34b11fb41c6f068c27410b5756dbebcb367dd).
The example pins `ag-ui-client` at that same revision. Root Cargo's git/revision
patch unifies the client's registry dependency with the adapter's core types;
there are no external checkout paths. See [adapter API usage](../crates/crabber-agui/README.md).

Subscribe with `run.events()` before consuming `run.done()`. Create a `Projector`
using the actual runtime session/run IDs and separately selected wire aliases.
Feed each received `EventRecord` into `push`; encode returned typed SDK events
with `encode_sse`. Drain the receiver and await `done`, then call `finish` using
`Completion::{Completed, Cancelled, Paused, Failed, LeaseLost}`. The host retains
the handle while draining so it can interrupt on receiver/transport faults.
A task error outranks an earlier durable success hint. Call `fail` on lag;
never restart an executor to repair a stream. One terminal is emitted, and
repeated finish emits nothing.

| Source event | Wire behavior |
| --- | --- |
| RunStarted | RUN_STARTED, wire aliases and protocolVersion 1.0; no request input |
| MessageStarted | Remember the actual assistant ID; no empty text event yet |
| TextDelta | First nonempty delta opens TEXT_MESSAGE_START; then CONTENT |
| ReasoningDelta | Omitted by default; opt-in REASONING_START/message events/END with ID `<assistant-id>:reasoning` |
| ToolCallStarted | Open the assistant text container once, then TOOL_CALL_START with actual call and parent IDs |
| ToolCallArgsDelta / ArgsCompleted | Original provider argument fragments / one TOOL_CALL_END |
| MessageStreamEnded | Close text/reasoning; failed public attempt permanently faults projection |
| MessageCommitted | Validate the persisted assistant identity; no duplicate text/snapshot |
| ToolCallSettled | TOOL_CALL_RESULT with stored tool-message ID and post-transform content |
| RunPaused / RunSettled | Validate terminal hints; await task completion before terminal |
| Admission, turns, permissions, running calls, epochs, extension/custom records | No public output |

Tool result `content` is a JSON string representing `{ "content": [public content
blocks], "is_error": boolean }`. Compare it with `public_tool_content` applied
to stored tool-result content. ProviderState is omitted recursively; nested
reasoning obeys the display opt-in and never includes provider state. Media is
structured public metadata, never downloaded. A failed tool result can belong
to a successful run. Hosts must authorize their tool/provider content and apply
redaction before settlement; the adapter cannot infer which text is sensitive.

Message and call IDs come from runtime/provider identities. Each provider attempt
gets a different assistant message ID. A tool-only response has an empty text
container so the pinned client does not create duplicate assistant messages.
Concatenated successful text equals the stored assistant text; normalize an
absent stored text block and an empty protocol container equivalently.
Argument stream end proves presentation completion, not execution or persistence.
MessageStreamEnded is separate from MessageCommitted. A failed attempt with
public text, tools or displayed reasoning faults the wire even if runtime retry
later succeeds. An attempt with only filtered reasoning can retry. Interrupted
partial assistant messages retain the streamed ID. Compaction summaries remain
internal.

Timestamps use checked Unix milliseconds, omitted outside JavaScript's safe
integer range. Raw events, source cursors, backend errors, provider state,
claims and private configuration are never forwarded. Wire aliases carry no
runtime recovery or admission authority.

Defaults cap event JSON, message text and each call's arguments at 1 MiB, open
unsettled calls at 64, message attempts at 1024 and batches at 256. Configuration
can reduce these limits; zero or values above the defaults are invalid. Aliases
must be nonempty, at most 256 bytes, and contain no controls. Construction checks
that configured startup and finite terminals fit the event limit. Metadata and
counters are bounded; text/arguments and complete transcripts are not retained.
Closed identities remain for bounded duplicate detection. Oversized JSON is
rejected before allocating its output frame; fields are checked before copying.
No truncation is used.

Completed -> RUN_FINISHED success; cancellation -> cancelled; pause -> interrupt
with stable `<wire-run-id>:pause` and reason `crabber_paused`. Browser resume is
unsupported. Runtime failure/lease loss -> RUN_ERROR. Missing/invalid boundaries,
lag, source-attempt failure and limits -> finite `crabber_*` error codes with a
generic message. Valid opened boundaries close before terminal when possible.
If a host cannot deliver a terminal because its socket is closed or exhausted,
it must report transport failure locally rather than claim delivered success.

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
unsupported profile -> 422; body read deadline -> 408; body over 128 KiB -> 413; busy thread -> 409;
capacity/shutdown -> 503; startup failure -> generic 500. Admission occurs
before successful SSE headers.

`threadId` becomes the stored SessionId. The example's ThreadStore wrapper lets
the first POST create that exact session via the existing atomic memory-store
admission transaction; ordinary runtime prompt(Some(session)) otherwise requires
an existing session. It owns one workspace/directory and process-local history.
POSTs are not idempotent; later repeated requests can start another turn.
Production retries use [keyed admission](admission-receipts.md).

The host caps active runs at 8, queued frames at 32/2 MiB, runs at 30 seconds and
cleanup at 5 seconds. Queuing never blocks the runtime observer. Overflow/lag
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
rechecked. Browser/Dojo, shared state/activity/subagents and full transcript
reconciliation are outside this adapter's profile.
