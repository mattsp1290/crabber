# Public result-transform probe

This standalone Rust 2024 crate has its own workspace and lockfile. It consumes
only the public `crabber` facade from
`ssh://git@github.com/mattsp1290/crabber.git`. The pin is the `rev` of the
`crabber` dependency in `Cargo.toml`; it is valid once that commit is reachable
from `origin/main`. The crate has no path dependencies, workspace test helpers,
WASM, provider calls, or runtime credentials. Existing authorized SSH Git
access fetches the private source; `.cargo/config.toml` uses the Git CLI.

From this directory, run exactly:

```sh
cargo test --locked
```

No task-specific environment variables are required. A complete run reports
10 passed tests: 2 in `public_contract`, 5 in `paths_binding_tamper` and 3 in
`interruption`. Fewer means the checkout predates these proofs.

The tests run real `Agent`/`Orchestrator` execution with public
`FakeProvider`, `MemoryStore`, extensions and tools. The runtime contains
panics inside callbacks, so every proof is asserted after the run from what
the callbacks recorded and from durable state.

`tests/public_contract.rs`:

- `exact_normalized_binding_reducer_then_final_redactor_protects_next_request`
  binds reduction to the exact tool and normalized input, with wrong-input and
  wrong-tool controls. Reduction escalates success to an error; the final
  redactor follows it despite a lower registration order. The executed secret
  is absent from the next full provider request and from durable tool results,
  messages and events.
- `prepare_failure_reports_unavailable_input_and_cannot_downgrade_error`
  checks `PrepareFailed` with `Unavailable { PrepareFailed }` input, no
  execution, and error status kept when both handlers return `mark_error: false`.

`tests/paths_binding_tamper.rs`:

- `execution_error_keeps_class_and_normalized_input`: an executor error is
  `ExecutionFailed` with the normalized input and persists as failed.
- `permission_denial_keeps_class_and_never_executes`: a policy denial is
  `PermissionDenied` with the normalized input; the executor never runs.
- `unknown_tool_reports_unresolved_name_and_raw_input`: an unregistered name
  is `UnknownTool`, unresolved, with the raw provider arguments.
- `parallel_calls_keep_their_own_tool_input_and_ids_under_reverse_completion`:
  three calls are in flight at once and finish in reverse order; each context
  matches its own durable call, session and run IDs and each call persists its
  own value.
- `tampered_envelope_fails_closed_with_fixed_text`: a JSON handler that changes
  any context field or adds a top-level key settles the fixed failure text;
  no later handler or final redactor runs and no executed, intermediate or
  handler-authored value is persisted. An honest envelope is the control.

`tests/interruption.rs`:

- `run_interrupt_with_active_child_settles_interrupted_fixed_text`: the reducer
  starts a child process and hands it to its mount's cleanup tracker. After the
  child reports ready the test calls `RunHandle::interrupt`. The call settles
  `Interrupted` with the fixed text within `INTERRUPT_SETTLEMENT_BOUND`; the
  permit stays held until the child is reaped; `Agent::close_extensions` joins
  the cleanup; the PID is gone (Linux) and the pipe is closed.
- `run_interrupt_after_accepted_fallback_persists_only_final_redaction`: a value
  accepted before cancellation persists only as the final redactor's output,
  still with status `Interrupted`.
- `interruption_support::child::reduction_child` is the child's entry point and
  does nothing when run as an ordinary test.

For the durable PostgreSQL proofs of both interruption cases, run:

```sh
cargo test --locked --features postgres -- --nocapture
```

The `interruption` binary then reports 5 passed tests, and each PostgreSQL
test prints which database it used. With `CRABBER_TEST_POSTGRES_URL` set to a
non-empty value, the tests use that disposable PostgreSQL 14+ database.
Without it they start their own `postgres:14` container, which needs a
reachable Docker daemon; the container is removed when the test ends or the
run is interrupted with Ctrl-C (an interrupt during container start-up takes
effect once the start completes). A killed run (`SIGKILL`) can leave a
`postgres:14` container to remove by hand. They never skip: with neither a
database nor Docker, they fail.

The interruption proofs time `interrupt()` to the run handle resolving, which
happens after durable settlement, and print the measured time. They require
it to stay within `INTERRUPT_SETTLEMENT_BOUND` (1 second), and within
`FINAL_REDACTION_DEADLINE` (500 milliseconds) when a fallback was accepted.
This shows a prompt bounded settlement; that the runtime enforces the
redaction deadline against a slow redactor is proven by the tests in the
repository, not here. Use a database on the same host or network; a slow link
to a remote database can exceed the bounds. The process-gone check uses
`/proc` and runs on Linux only.

`--locked` makes a lockfile that disagrees with the manifest an error instead
of a silent rewrite.

This probe is excluded from `cargo xtask check` and from CI. Run it at the
merge commit that published the pin, not at the pinned commit itself, whose
manifest still names the earlier `rev`. Confirm the tested revision with:

```sh
cargo tree -i crabber
```

The pin is the last content commit before the commit that changes only the
manifest `rev` and the lockfile (`docs/embedding.md`, Recorded answer 6). The
pull request is merged with a merge commit so the pin stays reachable from
`main`. Merge, tags or releases, the request reply and acceptance are human
gates.
