# Explicit tool continuations

Ordinary `ToolExecutor` implementations run to completion. Their `ToolContext::request_interaction` call returns `DurableInteractionUnsupported`; it does not open a live prompt or capture a running future. Pre-effect permission approval remains a separate host interaction.

A native tool may opt into `ResumableTool` and register with `ToolDefinition::resumable(info, tool)`. This interface makes each resumable step explicit:

- `identity()` declares the component version, configuration fingerprint, and checkpoint schema/version.
- `start(context, arguments)` executes the first step once.
- `resume(context, arguments, checkpoint, responses)` executes the next step from an acknowledged private checkpoint and the exact accepted responses.
- Either method returns `ToolPhaseOutcome::Completed(output)` or `ToolPhaseOutcome::Suspended(ToolSuspension::new(checkpoint, questions)?)`.

The checkpoint must contain the state needed by the next step. It must not contain a future, lock guard, task, socket, worker token, or a promise that the previous step can be repeated safely. A checkpoint is bounded to 64 KiB; a suspension requires one to 64 distinct named interactions, with at most 64 across the suspended batch. Question schemas are compiled offline and host answers are checked against them. Checkpoint and accepted-response values are private Store/runtime evidence. Safe host snapshots and replay receipts do not expose them.

The registration identity is captured in the execution plan. A reconstructed host must register the same implementation, configuration, and checkpoint schema. Changing any of them refuses continuation before a new worker claim. A tool must keep the identity stable throughout its registration lifetime; replacing the handler requires a new registration and cannot reinterpret an older checkpoint.

A suspension is not a tool result. Result protection runs when the tool returns `Completed`, and the logical call then receives its single terminal output. A sequential batch pauses before starting its next call. A parallel batch stops scheduling more calls and waits for already-started siblings to finish and join their resources. Successful siblings settle normally; unrelated never-started calls remain pending. The run becomes resumable only after native phase suspension, cleanup, the private checkpoint, required questions, run pause, and admission barrier commit atomically. An unresolved sibling or failed join leaves uncertainty to reconcile; it does not authorize replaying the completed effect.

The host reads `Agent::tool_continuation_snapshot(session, run)` to obtain required interaction IDs, schemas, authority requirements, and the current revision. It authenticates the responder itself, then submits `ToolInteractionAnswer` with an exact continuation target, question ID, revision, resolver identity, authority claim, and answer/deny/cancel/expire choice. The Store checks the declared authority requirement and schema; it does not authenticate a human on the host's behalf. Expiration uses the Store clock. The host must explicitly expire an overdue interaction.

After all required interactions are settled, `Agent::continue_tool(ContinueTool { ... })` claims the exact frozen next attempt and phase generation. Only a fresh `Started` result returns a run handle. An identical retry returns `Replayed(receipt)`, which is metadata and cannot dispatch the tool. Reads, answers, and receipt replays remain usable after extension shutdown.

If a claim commits but its acknowledgement is lost, an identical retry cannot recover authority. After the lease expires, the host may request `Agent::recover_tool_continuation(RecoverTool { original, ... })`. Recovery requires positive persisted evidence that the reserved parent is still `Unstarted` and each resumed phase is still `Claimed`; it rotates authority without repeating a tool step. A `Started` or uncertain phase, or a missing checkpoint, cannot use this recovery route. Reconcile its outcome through host controls instead.

Migration from a live `UserPrompter` therefore requires choosing the boundary explicitly: move a permission choice before dispatch, or rewrite the native tool as this versioned state machine. A live callback is not converted automatically. Frontend runtime input and full workflow continuations have their own ownership and execution contracts; this tool interface does not implement those paths.
