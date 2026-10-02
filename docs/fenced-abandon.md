# Fenced abandonment host protocol and acceptance evidence

`Agent::abandon(AbandonRequest)` is a settlement operation. It does not load the
persisted executable configuration, resolve a provider, mount extensions or hooks,
execute tools, or interpret a paused continuation. Pending, Running and Paused are
eligible persisted states; each unfinished Pending/Running tool receives one
Interrupted result, matching tool message/part and ToolCallSettled event. The run
receives one Interrupted RunSettled event. Completed/Failed tools, previous history,
usage, diagnostics, checkpoints and immutable admission receipts are retained.

## Agentcraft host decisions

The host chooses abandonment when persisted work must stop without execution. Use
the existing resume/recover APIs when the host intends their documented execution
or continuation behavior. Crabber imposes no automatic or bulk abandonment policy.
No Agentcraft repository changes are included in this milestone.

1. Read and persist the original run ID, owner and claim token and the intended
   authority as an exact `AbandonRequest` **before** submitting it. Keep this
   request after ambiguous failures and after a subsequent run is admitted.
2. Prefer `ExpiredLease`. Eligibility uses the store clock inside the ownership
   transaction, after acquiring the run lock. A live lease is `LiveLease`, even
   for Paused; a paused state is not evidence that a worker has stopped.
3. For `HostStoppedOwner`, first stop the real worker's provider/tool execution,
   renewal loop and any coordinator that could replace its ownership. Obtain
   authoritative process/coordinator evidence: kill/cancel plus confirmed exit
   and join/wait, or the coordinator's equivalent authoritative stopped state.
   Persist that evidence in the host. A token, PID alone, expected owner string,
   elapsed timeout, or inability to contact a worker is not evidence of death.
   Crabber trusts this external assertion; it cannot verify process death. Its
   transaction still checks the exact owner and token. A replacement owner makes
   the assertion stale. The disposable example worker demonstrates kill **and
   wait** before assertion; it does not establish production coordinator policy.
4. Call `Agent::abandon(original_request.clone())`. A successful outcome means all
   settlement artifacts and private replay provenance have committed together.
   Read `outcome.run`, `outcome.interrupted_tools` and
   `outcome.terminal_event.cursor` for durable correlation.
5. On `AbandonError::Store`, transport loss, host crash or unknown response,
   reconnect and retry the **identical** saved request. Never synthesize a request
   from the newly rotated fence and never switch its authority. Exact replay
   returns the original committed outcome without new messages/events or effects.
   If the transaction rolled back, that same request can perform the settlement.
   A commit error is not success. Stop reporting completion until reconciliation
   returns a durable complete outcome.
6. `LiveLease` means wait for eligibility or independently stop the worker before
   choosing and persisting an administrative request. `StaleOwner` means ownership
   or replay authority changed; refresh evidence and make a new policy decision.
   `NotFound` means no such persisted run. `AlreadyTerminal` means an unrelated
   settlement, **not** successful abandonment. Inspect history rather than treat
   these denials as success. Error/event payloads are not replay credentials.

Fence rotation, unfinished-call settlement, the terminal run/event, and private
`abandonment_commits` provenance publish in one transaction. A stale execution
handle cannot append messages/parts/events, create/claim/settle tools, renew, change
epochs, pause/settle runs, change extension state, or consume inbox rows. PostgreSQL
locks serialize renew/resume/abandon; Memory publishes a cloned candidate only
when every write succeeds. There is no externally observable staged settlement.
Memory rollback proofs inject failures after each meaningful candidate write;
PostgreSQL proves statement failure, precommit rollback and committed response
loss, and physically kills an actual writer inside its abandonment transaction.

The session remains available for subsequent admission. Abandonment leaves steer
and follow-up inbox rows unconsumed; the next execution can claim them, including
claiming follow-ups into history. Admission receipts continue identifying the
original run, not the session's most recent run. Memory persistence lasts only for
that store instance; PostgreSQL preserves it across fresh processes. Apply the
additive schema 4 migration explicitly; connecting does not migrate or authenticate
old event markers. Ordinary owner-written RunSettled payloads never grant replay
provenance.

## Noninteractive demos

```sh
cargo run --quiet -p fenced-abandon
# Configure CRABBER_POSTGRES_URL for a dedicated PostgreSQL 14+ database.
cargo run --quiet -p fenced-abandon --features postgres -- --postgres
```

Both modes pause a persisted run containing unsafe Pending and Running tool work
with unavailable configuration. They demonstrate ordinary live rejection, wait
for authoritative child exit, administratively abandon and exactly replay. Output
reports compilation Git SHA and runtime Git SHA (asserted identical), backend, run
ID, terminal event cursor, Interrupted, and measured zero provider/tool/extension
mount counters. Extension installation panics if invoked, so executable hooks
cannot be mounted. PostgreSQL launches a fresh executable process that reads the
durable Interrupted run, confirms no unfinished calls, retries the saved request,
and verifies unchanged messages/events. Rebuild after source commits; an identity
mismatch fails instead of printing a misleading source revision.

Tests use `CRABBER_TEST_POSTGRES_URL`; demos use `CRABBER_POSTGRES_URL`. Set
`CRABBER_REQUIRE_POSTGRES=1` for mandatory service tests. CI provisions a dedicated
PostgreSQL 14 trust service, requires the URL and runs session/facade contracts plus
admission and abandonment demos without provider credentials.

## Request-to-assertion map

Names below are Rust test names; `facade` means `cargo test -p crabber --features
postgres --test fenced_abandon`, and `session` means `cargo test -p crabber-session
--features postgres`. The facade target privately compiles the Memory fixture
module to seed Pending and inject candidate failures; no product fault API exists.
All acceptance clauses remain in scope.

| Request acceptance clause | Named assertions and artifacts |
| --- | --- |
| Public Pending/Running/Paused abandonment, unsafe unfinished work, matching Interrupted results/messages/parts/events, one new terminal event, retained completed/failed calls/history/usage/checkpoint/receipts and subsequent admission | facade `public_memory_abandon_running_and_paused_without_execution` (includes Pending and both authorities with PID-bound stopped-worker evidence), `postgres::public_postgres_pending_running_paused_zero_execution`; session `memory::shared_abandonment_contract`, `postgres::abandon_tests::shared_abandonment_contract` cover both authorities, exact part/result equality, nonzero usage and diagnostics. |
| Zero provider/tool/mount/hooks, unavailable executable configuration and paused continuation | Both public facade matrix tests use instrumented panic-on-invocation Resolver, ToolExecutor and Extension::install. Every count must remain zero. Facade `postgres::abandonment_process_helper` repeats these assertions in fresh hosts/processes. Both demo modes print measured counters. |
| Atomic expiry, ordinary live/paused denial, stale administrative owner/token and authoritative stopped process, all stale owner writes revoked | facade `public_host_stopped_requires_exact_owner_and_fence`, `postgres::postgres_stopped_process_and_error_semantics` prove actual kill/wait; public matrix calls `assert_all_owner_writes_revoked` for every mutating ExecutionStore method. Session shared contract checks one nanosecond before expiry and at expiry; `memory::abandon_tests::replacement_owner_and_unrelated_terminal_are_not_abandonment_replay` tests owner replacement. |
| Both backend parity and one winner for renew/resume/abandon races | session shared contracts plus `postgres::abandon_tests::independent_pools_renew_resume_and_abandon_serialize`, `postgres::abandon_tests::clock_is_read_under_lock_and_abandonment_wins_queued_recovery`; stale writes fail after either takeover and exact simultaneous abandonment replays the same result. |
| Rollback at every meaningful atomic boundary, physical abandon crash, exact retry, committed response loss and no false/partial terminal success | facade `memory::abandon_tests::facade_each_memory_candidate_boundary_rolls_back_including_private_commit` (rotation, message/parts, tool event, tool result, terminal run, terminal event, private commit); `postgres::each_atomic_write_failure_rolls_back_private_commit_and_retries` (fence, tool row/result, message, parts, tool event, terminal run/event and private commit trigger failures); `postgres::physical_abandonment_writer_crash_then_fresh_process_retry` kills/waits a child blocked at private commit insertion after all writes, compares all original rows from a fresh process before retry. |
| Unknown committed response, durable authenticated provenance, no duplicate messages/events or unfinished tools; fresh process rather than reconnect alone | facade `response_loss::memory_committed_response_loss_exact_request_reconciles`, `postgres::facade_committed_response_loss_fresh_host_process_exact_replay`, `postgres::abandonment_process_helper`; session `postgres::abandon_tests::rollback_unknown_response_and_fresh_process_replay` injects after actual SQL commit; `denials_and_precommit_failure_release_locks_before_return` proves no lingering row lock on immediate fresh process. Snapshots include `runs`, `tool_calls`, `messages`, `parts`, `events`, and private `abandonment_commits`. |
| Missing/unrelated terminal and changed owner/token/authority retries are honest denials, untrusted history cannot fake abandonment | facade `facade_memory_missing_and_unrelated_terminal_are_honest`, `postgres::postgres_stopped_process_and_error_semantics`; both public matrix tests change each request field after real commit and require StaleOwner. Both session `untrusted_terminal_markers_are_never_replay_authority` tests retain unfinished calls yet require AlreadyTerminal for forged markers; forward migration never backfills authority. |
| Retained steer/follow-up/session and immutable original receipt; next work admitted without automatic execution policy | Both public matrix tests verify two unconsumed inbox rows after abandonment and claim steer plus follow-up into history under next admission; shared session contract retains keyed receipt through later admission and exact abandoned-run replay. |
| Demos, exact source/runtime identity, durable IDs and required PostgreSQL CI, host/Agentcraft protocol | `examples/fenced-abandon` and this document; `.github/workflows/ci.yml` runs required facade/session coverage and fresh-process PostgreSQL demo alongside admission gates. Session `required_url_is_enforced_in_fresh_process` proves missing required URL fails. |
| Existing live interruption, resume/recover and keyed/unkeyed admission receipt behavior preserved | `cargo xtask check`: runtime `interrupt_cancels_blocked_compaction_summary`, `interrupt_cancels_summary_stream_acquisition`, `resume_reexecutes_only_retry_safe_pending_call`, `recover_interrupts_expired_running_call`; facade admission `concurrent_and_terminal_retries_execute_once`, `identity_and_unkeyed_compatibility`, `stale_claim_cannot_hide_changed_inspectable_semantics`; required session PostgreSQL admission regressions remain mandatory. |

Required final gates, all from repository root:

```sh
cargo xtask check
env CRABBER_REQUIRE_POSTGRES=1 cargo test -p crabber-session --features postgres
env CRABBER_REQUIRE_POSTGRES=1 cargo test -p crabber --features postgres --test fenced_abandon
cargo run --quiet -p fenced-abandon
cargo run --quiet -p fenced-abandon --features postgres -- --postgres
cargo clippy -p crabber -p crabber-session -p fenced-abandon --all-targets --features postgres -- -D warnings
```

Local review logs live under ignored `.agents/reviews/dgzo/`; the coordinator records
the frozen commit and exact gate results. PostgreSQL sequence gaps after rollback
are allowed; persisted rows and terminal event identity must not change. Fault
triggers are scoped to unique run IDs and removed by the tests. These proofs cover
Crabber's atomic store contract and host protocol, not external worker termination
or arbitrary custom Store implementations.
