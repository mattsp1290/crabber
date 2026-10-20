# Public result-transform probe

## Source and acceptance

This standalone Rust 2024 workspace uses only public `crabber` facade APIs.
Its `Cargo.toml` and committed `Cargo.lock` must resolve the Crabber dependency
to the published crate-content candidate **C**,
`04e31b1cf7110f44fe1bbc73668111884f291508`. No development path override is used. Authorized Git
access is required; the scripted tests use no provider credentials.

Record the actual checkout revision, `rustc --version`, `cargo --version`, locked
commands/results and resolved Crabber Git SHA with the qualification evidence.
These instructions describe the checks; they do not themselves record a pass.
`--locked` rejects a manifest/lockfile mismatch rather than silently rewriting it.

From the Crabber repository root, `cargo xtask check-probes` is a separate local
static API check. It copies the actual probe source, tests and manifest into
disposable outside-repository workspaces and changes only the disposable
`crabber` dependency to the local checkout path. It seeds resolution from the
workspace `Cargo.lock`; additional registry dependencies may still resolve.
It checks all targets with default features, without running the probes or
fetching their tracked Crabber Git dependency. PostgreSQL cfg is off. With all
dependencies cached it can run with `CARGO_NET_OFFLINE=true`; cold CI may access
the registry without provider credentials. It does not modify the tracked probe
manifests or locks, and is not Git-source, runtime or PostgreSQL acceptance.

## Commands

From `testdata/result-transform-probe/` in the clean maintenance checkout:

```sh
cargo test --locked
```

The default run needs no task-specific environment. For actual PostgreSQL
interruption coverage, set `CRABBER_TEST_POSTGRES_URL` to a nonempty disposable
PostgreSQL 14+ database URL, then run:

```sh
CRABBER_REQUIRE_POSTGRES=1 cargo test --locked --features postgres -- --nocapture
```

This probe's PostgreSQL tests do not skip: when that URL is empty or absent,
they start their own `postgres:14` container and require a reachable Docker
daemon. These tests do not read `CRABBER_REQUIRE_POSTGRES`: a nonempty URL
selects the configured service; an absent URL selects Docker. The container
handle/watchdog removes it at normal exit or Ctrl-C; SIGKILL can leave a
container for manual removal.
The process-gone assertion uses `/proc` and runs on Linux only.

## Runtime assertions

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
  starts a child process tracked by both its native phase and mount. After the
  child reports ready the test calls `RunHandle::interrupt`. While reaping is
  held, the call has no result and its phase is not quiescent. The test releases
  reaping before awaiting `Interrupted` with the fixed text. The permit returns
  only after the child is reaped and its pipe closes. Mount shutdown also waits
  for this cleanup; a cleanup timeout retains uncertainty and the real join.
- `run_interrupt_after_accepted_fallback_persists_only_final_redaction`: a value
  accepted before cancellation persists only as the final redactor's output,
  still with status `Interrupted`.
- `interruption_support::child::reduction_child` is the child's entry point and
  does nothing when run as an ordinary test.

The interruption proofs observe the run handle only after actual cleanup and
durable settlement. The fixture keeps `INTERRUPT_SETTLEMENT_BOUND` (1 second)
separate from the driver's `FINAL_REDACTION_DEADLINE` (500 milliseconds). The
accepted-only redaction case pauses Tokio time after real child readiness, with
a wall-clock watchdog and RAII resume before Store/reaper joins; it does not
assert a 500ms live-child wall-clock duration. Existing paused driver unit tests
pin the shared final-redaction deadline. Repository tests separately exercise
slow redactors and incomplete cleanup. The process-gone check uses `/proc` and
runs on Linux only.

## Clean-checkout verification

The exact dependency pin above identifies the crate code under test. The
repository checkout may be a later maintenance revision whose manifest selects
that pin. Record both the checkout commit and the resolved Crabber dependency
SHA with the actual command results.

Set `PROBE_CHECKOUT_COMMIT` to the reviewed published checkout revision:

```sh
git clone git@github.com:mattsp1290/crabber.git /path/to/unique-clean-clone
git -C /path/to/unique-clean-clone checkout --detach "$PROBE_CHECKOUT_COMMIT"
```

Enter this probe directory and run the locked commands above without a path
override. Inspect the resolved source with `cargo tree --locked -i crabber` and
`cargo metadata --locked --format-version 1`. The local static API check is
separate from these Git-source/runtime results.
