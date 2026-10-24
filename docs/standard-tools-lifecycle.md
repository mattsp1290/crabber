# Tool lifecycle and cleanup

Workspace identity is routing data. Hosts authorize workspaces through
`WorkspaceAdmission`; `ExactRoots` matches a configured workspace ID and exact
directory string. Tools use admitted capabilities and check the persisted session
directory before execution. They never change process cwd. File operations use
the host uid; `run_as` affects children only. Hosts own permissions, environment,
command_guard analysis and tool_result_redactor redaction.

`StandardToolsFactory` creates a workspace-scoped instance. Sessions in that
workspace share its capacity. Distinct workspace instances have independent
capacity; writers at the same canonical root share a process-global lock. The
factory hash covers policies and enabled schemas; a shared single-root instance's
hash also includes its canonical path. Closing a session does not close its
workspace owner. A closed workspace ID cannot be re-admitted on the same Agent. To remap an ID
or replace its directory, close the Agent extensions and build a new Agent with
the updated admission table. A cached capability can still point at the old inode.

Blocking closures and process tasks register before starting work. An ownerless,
sealed or full resource scope returns `unavailable`. Blocking work retains its
permits and actual join after executor cancellation; filesystem walks poll the
cancellation token. Single-file operations may finish after interruption. A panic
becomes a sanitized failed/unknown result without a cleanup failure.

Process tasks observe cancellation, owner close and result-receiver drop. They
kill the process group and reap the leader before joining, retaining pipe readers
inside the task. Command deadlines remain in capture so timeout results preserve
partial output. Reaping and inherited-pipe grace are bounded at five seconds.
Detached descendants and children that close their pipes remain a host concern.

The runtime normally waits one second for owned joins. Slow filesystem work or a
process surviving SIGKILL can outlive that bound. Inspect
`drain_execution_tasks(bound).resources`: sealed records the close boundary,
joined records actual completed tasks, pending retains unfinished joins and
failures records cleanup failures. A later drain observes those same joins.
Never infer cleanup from EOF, timeout, a dropped waiter or returned result.

At the pinned Crabber revision an interrupted native phase returns
`PhaseOutcomeUnknown` and records phase state Uncertain, with separate Complete or
Incomplete quiescence. The unfinished tool call has no acknowledged result.
Cleanup can finish while the durable outcome remains uncertain; hosts must use
Crabber's phase reconciliation controls rather than assume Interrupted or retry
an effect. Cleanup failures also prevent a normal success result. The older
`AttemptCleanupIncomplete` description alone does not cover this phase protocol.

Outputs retain each tool's caps and additionally fit Crabber's 4 MiB serialized
ToolOutput bound. Shell stdout/stderr and URL content may be halved on UTF-8
boundaries with `*_truncated` markers; other oversized envelopes yield too_large.
Every failed envelope sets is_error; shell nonzero exits remain successful calls.

A durable user_interact question records a checkpoint and one required host
response. Restore the same policy, behavior version and store to continue it;
completed sibling effects are not restarted. The host must explicitly Expire
overdue questions. A crash after continue_tool returns Started is uncertain and
requires Crabber recover_tool_continuation/host controls; that path is not tested
by this adoption. Reconstructing an Agent never proves an old external process
stopped. Retained upstream limitations include crabber-pxdk, crabber-09ku and
crabber-btrf; this consumer makes no automatic same-original recovery promise.

Questions and answers cross unredacted. Do not collect secrets. Hosts should
mount tool_result_redactor; library-generated error messages contain neither host
paths nor raw command output. All fixture hosts are scripted and use no credentials.

Parallel siblings not yet started when another question suspends may run only
after continuation. Hosts must answer the current snapshot, continue, and repeat
while the run returns Paused. Every question must settle exactly once across these
pauses. Interruption may hide an already completed blocking effect, so reconcile
the durable phase and observed effects rather than blindly retrying.

SQLite reopen qualification lives in the standalone consumer to avoid adding
SQLite features to every workspace test. CI runs the consumer on every push
after the workspace check. Local qualification uses `CRABBER_REV` with
[`check.sh`](../testdata/standard-tools-consumer/check.sh).
