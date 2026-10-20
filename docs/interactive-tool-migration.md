# Durable input and interactive tools

Tools no longer receive a live `UserPrompter::ask` or approval callback. Choose the boundary that describes the operation:

- Use the normal pre-effect permission/guard decision when a tool needs approval before doing work. The whole pending batch is frozen before any call starts.
- Register `ToolDefinition::input` when the model should ask for user input. It publishes durable questions, pauses the run, and produces one final tool result after the required response set is recorded.
- Implement `ResumableTool` when a tool performs work before asking a question or needs several stages. Its private checkpoint identifies the next stage; resume never calls start again.

An input answer supplies data. It does not grant permission to execute another tool. Normal tool policies and result transforms still apply.

## Register the input tool

```rust
use crabber::{ToolDefinition, core::AuthorityRequirement};
use std::sync::Arc;

let input = ToolDefinition::input(
    "request_input",
    AuthorityRequirement::new("signed-in-user", "1")?,
)?;
// Add .shared_tool(Arc::new(input)) to the Agent builder.
```

The policy identity is a host declaration. Authenticate the responding caller in the host and provide the resolver identity and authority assertion when recording each answer. A matching policy name alone does not authenticate a user.

The model calls the tool with named questions:

```json
{
  "questions": [
    {"name": "color", "prompt": "Choose a color", "choices": ["blue", "red"]},
    {"name": "notes", "prompt": "Any notes?"}
  ]
}
```

There are at most 16 questions. Names are unique; prompts and optional string choices are bounded. Declarations have a 32 KiB UTF-8 budget before checkpoint encoding, names at most 128 bytes, prompts 2048 bytes, and each of at most 16 choices 256 bytes. Free-text answers have an 8192-character limit. Arbitrary model-supplied response schemas and external schema references are not supported by this built-in tool; custom state machines can declare their own validated interaction schemas.

Inspect `Agent::tool_continuation_snapshot`, render its required interactions, and record responses with `answer_tool_interaction`. Runtime supplies interaction IDs. Answer validation uses the frozen response contract: the `color` answer above must be `blue` or `red`. A rejected answer does not advance the session revision.

After all required responses are recorded, call `continue_tool` with the exact session, run, continuation, current revision and a stable operation ID. A fresh acknowledged claim starts one worker. An identical retry returns metadata and starts no worker. Reconstructing the Agent requires the same behavior, input-tool name and policy declaration for fresh continuation. Keep the original operation ID to reconcile a lost acknowledgement, including after the Agent closes.

The final result is a JSON array of `{name, state, value}` entries in question order. Questions remain separate from tool results while waiting. Non-answer states contain no fabricated input and produce an error tool result. This built-in tool does not set an expiry; hosts can cancel unanswered interactions. Cancelling the entire run uses the separate durable run-control API.

## Migrate a tool that already did work

A callback wait after an external write cannot be replaced by replaying the original function. Split it at the wait:

1. `start` performs the first stage once, then returns `ToolPhaseOutcome::Suspended` containing a bounded versioned `ToolCheckpoint` and required `InteractionDescription`s.
2. The runtime joins started phases and resources, records the checkpoint and barrier atomically, and returns a paused outcome.
3. The host records the correlated answers. `resume` receives the exact checkpoint and accepted responses, performs only the next stage, then completes or suspends again.

Declare a stable component/version/configuration and checkpoint schema through `ResumableTool::identity`. Register it with `ToolDefinition::resumable`. A changed behavior/schema cannot silently restore an old checkpoint. Dropping an awaiter or losing a started worker does not authorize repeating earlier effects; uncertain started work requires explicit resolution.

The compiled public fixtures [tool_continuation.rs](../crates/crabber/tests/server_host/tool_continuation.rs) and [runtime_input.rs](../crates/crabber/tests/server_host/runtime_input.rs) demonstrate native multi-stage and built-in input flows, cold Agent reconstruction, answer validation, and metadata-only replay. The separate [explicit continuation guide](explicit-tool-continuations.md) describes storage and recovery semantics.
