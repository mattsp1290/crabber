# Durable interactions for embedding hosts

The host owns authentication, authorization, transport, and retry intent. Crabber
stores exact interaction targets and immutable receipts; a receipt replay grants
no executor. These APIs are unstable until the independent dependency and consumer
gates are qualified. The credential-free [server-host probe](../examples/server-host-probe/README.md)
is an executable public embedding with injected storage, scoped tools, two
contexts, two question pauses, and replay.

## Choose the interaction boundary

A normal `ToolExecutor` runs to completion. Calling
`ToolContext::request_interaction` returns `DurableInteractionUnsupported`; it
neither opens a live callback nor captures a future. Use pre-effect permission
approval when the choice authorizes execution. Use an explicit `ResumableTool`
when the tool must preserve an intermediate result and ask a question. Use a
frontend descriptor in `InputEnvelope` when an external host supplies the result
instead of a backend executor.

A resumable tool declares component/configuration identity and a checkpoint schema
version. Its `start` and `resume` return either `Completed(ToolOutput)` or
`Suspended(ToolSuspension)`. Keep the private checkpoint bounded and sufficient for
the next step; never put a future, task, guard, socket, or worker authority into it.
Private phase checkpoint persistence and quiescent continuation readiness are
distinct guarantees. A retained checkpoint is not proof that cleanup completed.
The runtime publishes a ready waiting continuation only with acknowledged native
suspension, required questions, run pause, barrier and joined resource evidence.
A crash between checkpoint persistence and cleanup requires reconciliation; it
does not authorize the next phase. Suspension is not an output; protection and
redaction apply once to the eventual completion.

## Answer and resume

1. Read `Agent::tool_continuation_snapshot(session, run)` and retain its exact
   continuation generation, question IDs, schemas, authority requirements and revision.
2. Authenticate the responder outside Crabber. Submit `ToolInteractionAnswer` with
   a stable operation ID, original target/revision and `ApprovalResolverClaim`.
   Answer values are checked against the frozen schema. Deny, cancel and explicit
   expiration remain distinct choices; the Store clock governs expiration.
3. Retain the complete request before sending it. Reconcile a lost response by
   looking up or retrying that exact operation. `None` while a request may be in
   flight is not proof that it rolled back. Changed choices under the same key conflict.
4. Once every required question is settled, submit `ContinueTool`. Only fresh
   `Started` grants a handle. An identical `Replayed` result is metadata, even when
   the original worker has progressed to another pause or terminal state.

Reconstruct the same tool registration/configuration/checkpoint version before
fresh continuation. Recovery rotates only a positively retained Unstarted parent
and Claimed phases after lease expiry. It never repeats a Started effect or
infers a checkpoint from missing data. Started or uncertain work requires explicit
host reconciliation. A resolver claim records the host's assertion; Crabber does
not authenticate the person behind it.

## External frontend results and controls

Frontend descriptors freeze input and optional output contracts separately. Read
the frozen required call set, authenticate the external responder, then supply
`ContinueFrontend` with one typed result for every required frontend call and an
exact `FrontendResolverClaim`. Partial, duplicate or foreign results fail closed.
A fresh native result-processing phase protects the output; no backend tool
executor runs. Retries and Unstarted recovery follow the same metadata/authority
separation. See [frontend continuations](frontend-tool-continuations.md).

Pre-effect approval has its own `approval_snapshot`, answer and continuation
controls. Durable run cancellation and ordered steer controls use exact run
identity, operation keys and revision CAS. Cancellation acceptance is distinct
from actual worker observation and joined cleanup. Cancelling a waiting batch
records interrupted logical results and cancels unanswered questions without
inventing human answers or rewriting historical suspended phases.

Pure state reads, answers and keyed metadata replays remain usable after
`close_extensions`. Close rejects fresh ingress; dropping an event subscription
or RunHandle does not cancel owned work. Drain retained tasks and reconcile durable
outcomes explicitly. Local emptiness does not establish cleanup on another host.

For exact bounds, parallel-batch behavior and recovery contracts, see
[explicit tool continuations](explicit-tool-continuations.md),
[interactive-tool migration](interactive-tool-migration.md), and
[configured hosts](server-host.md). The probe reconstructs Agents in one process;
separate public integration fixtures exercise actual fresh-process boundaries.
