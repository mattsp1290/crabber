# Durable run cancellation and steer

`Agent::control_run` persists an exact session/run/binding target and a stable
`ControlId`. `Agent::run_control_binding` captures that target and its coherent
session revision without reading private execution data. The embedding host
authorizes the supplied session and run before calling either API.

```rust,ignore
use crabber::core::{ControlId, InputEnvelope};
use crabber::session::{RunControlAction, RunControlRequest};

let binding = agent.run_control_binding(&session_id, &run_id).await?;
let request = RunControlRequest {
    id: ControlId::new(), // Retain this exact request in the host's outbox.
    target: binding.target,
    expected_revision: binding.revision,
    action: RunControlAction::Steer {
        input: InputEnvelope::text("Use the smaller example", binding.revision)?,
    },
};
let receipt = agent.control_run(request.clone()).await?;
```

Steer accepts ordinary user messages, retaining supplied message, parent and part
IDs in their original order. It rejects system/assistant/tool roles, state changes,
context additions and frontend declarations. Those operations require their own
authority and APIs. The input's expected revision must equal the control request's
revision. A run accepts at most 64 controls, each at most 64 KiB; its aggregate
budget is 256 KiB including reserved metadata growth.

For cancellation, supply `RunControlAction::Cancel { resolver }` with a bounded
`ApprovalResolverClaim` containing the actual controller identity, authority
requirement and host authority claim. This retained claim attributes the host
decision. It proves neither worker shutdown nor external resource cleanup.

`Applied` and `Replayed` return the original acceptance receipt. Exact retries
reconcile before revision and Agent ingress checks; changing any original choice
under that key conflicts. A replay never grants worker authority or dispatches
work. Cancellation and administrative polling/progress remain available after
Agent close. A fresh steer requires open ingress. Custom Stores that do not
implement these controls return `Unsupported`.

`run_controls` returns a coherent bounded metadata page. It excludes steer text,
resolver claims, private checkpoints and worker credentials. Its current status
distinguishes persisted `Accepted`, fenced-worker `Observed` cancellation,
`Incorporated { turn_id }` steer, `UnappliedTerminal` steer and
`TerminalCommitted` cancellation. Incorporation atomically appends input at the
next real context boundary; it does not promise that the provider follows it.
Accepted input stays targeted to its original run. It never moves to a later run.

The retained worker polls under its current fence, and its owned execution future
observes cancellation without a detached dispatcher. `TerminalCommitted` proves
the logical Store transaction. Awaiting the execution handle's `done()` also
joins that local task. Neither status proves external resources have stopped.
An unknown terminal write remains an incomplete result even if the worker exits.

A cancellation can arrive while a worker is committing a pause. The runtime
checks the original accepted cancellation at its joined pause boundary. A host
supervisor can also rediscover it through `run_controls`, then call
`settle_waiting_cancellation` with the original Cancel ID, exact target and latest
page revision. This progresses the original intent rather than accepting another
cancel. The Store independently authenticates the original resolver and waiting
proof, commits explicit Interrupted call results and releases eligible barriers
atomically. Terminal reconciliation precedes CAS, so a lost acknowledgement can
be retried after restart or Agent close. Missing legacy suspension proof or an
ineligible/active phase returns `Busy`; it does not manufacture cleanup evidence.

Memory, PostgreSQL and SQLite use the same contract harness. Native fault seams
cover rollback and lost acknowledgements at acceptance, worker observation,
incorporation and waiting progress; process tests kill controllers at the actual
transaction boundaries and reconcile with a cold Store.
