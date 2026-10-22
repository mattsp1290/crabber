# Test execution and performance evidence

## Ten-minute follow-up

The current follow-up replaces the original percentage target and historical tuple-bijection requirement with a 600-second complete-check budget and [risk-based coverage ownership](test-coverage-policy.md). Qualification is pending. The budgeted entrypoint is `python3 -I -S .github/scripts/check.py`; it wraps the full `cargo xtask check`, including real PostgreSQL, compilation, checks and cleanup. Use `cargo xtask check --lane offline` or `check-postgres` for explicitly partial developer runs. The combined schedule is under validation; the 600-second target has not passed qualification.

The clock starts before Cargo (and before CI cache restoration). The launcher defaults development/test debug symbols to zero while preserving explicit caller settings and Cargo’s local incremental-compilation default. It starts cleanup with a 20-second reserve and returns failure for deadline expiration, child failure, cancellation or an incomplete schedule report. `--cache-state unchanged|runtime-rebuild|session-rebuild` labels a qualification case; bootstrap/unclassified runs never claim cached-dependency qualification. Provisioned pinned tools, downloaded dependencies and the PostgreSQL image are prerequisites; ordinary workspace compilation counts. Required cache cases are unchanged source, forced runtime rebuild and forced session rebuild with dependencies warm. Each must pass within 600 seconds locally and on the declared CI resources. Package-scoped cleanup must cover every retained nested target containing the affected crate and its dependents. Existing timing results below describe the earlier implementation, not acceptance of this budget.


Current work starts at `3fc10a553bdd007f018fd85cbe629f44d316f52a` on `feat/ten-minute-tests`. The full gate provisions one owned PostgreSQL server, retains narrow driver/feature checks, compiles an explicit combined native feature graph, discovers and executes those exact test binaries once, runs explicit doctests and unique host checks, then cleans owned resources. Unknown cases in backend-enabled binaries use a conservative service group. The normal combined profile allows four PostgreSQL tests locally and initially two on CI, four SQLite slots, and weighted WASM work; ordinary Memory tests use the general scheduler.

The current low-optimization candidate builds the cached `serde_json`, `sha2` and `jsonschema` dependencies at level 1 in dev and inherited test profiles. Workspace crates remain at level 0, with debug assertions and overflow checks enabled. This targets the serialization, digest and schema-validation work in CPU-heavy contract histories; it must improve complete checks, including affected-crate compilation, before acceptance. Cargo package overrides are documented in [the Cargo profile reference](https://doc.rust-lang.org/cargo/reference/profiles.html#overrides).

Supervised child commands discard the caller's `CARGO_PKG_*` and `CARGO_MANIFEST_DIR`, `CARGO_MANIFEST_PATH`, and `CARGO_MANIFEST_LINKS` package metadata. Nested Cargo supplies each compiled package's own metadata. Inheriting xtask's values caused build scripts such as `ring` to record a different ambient `rerun-if-env-changed` input from a direct Cargo invocation, rebuilding dependencies when commands alternated. Target directories, build jobs, profiles and compiler flags remain explicit build configuration and are preserved. This avoids unnecessary rebuilds without overriding Cargo freshness or reusing artifacts across incompatible graphs.

CI now runs one complete job. Its timestamp precedes cache restoration and is passed into the launcher. Compiler caches are separated by architecture, compiler identity, profile, tool pins, locks and the combined graph. Snapshot saving occurs afterward as separately reported CI maintenance. Bootstrap/cache-miss results do not establish the cached-dependency timing promise. Source provenance is refreshed after restoration. The complete run uploads sanitized timing, selected-manifest and fixture reports.

Final qualification uses `.github/scripts/qualify-check.py --warm-report <previous-complete-check.json> --output <new-directory>`. It requires a completed check for the same clean revision, then runs unchanged-source, runtime-rebuild and session-rebuild cases with the same complete command. Package-scoped cleanup inventories every retained target containing that current workspace crate, preserves registry dependency artifacts, and verifies both affected-package and dependent rebuilding. Native standalone consumers share the invocation's main target; explicitly scoped API probes and WASM targets remain separate.

CI's `workflow_dispatch` input `qualify: true` enables these three cases once; ordinary CI runs one complete check. Before this workflow reaches the default branch, the same opt-in can be scoped to an exact candidate commit with the repository variable `CRABBER_QUALIFY_REVISION`; remove that temporary value afterward. Qualification requires a completed same-revision compiler snapshot and includes restoration time in its first case. Invalidations prepare the changed-source cache conditions and are separately reported; during this protocol, source compilation occurs only inside the measured complete checks.

CI preserves source timestamps alongside compiler artifacts using Git index blob IDs and modes. After a fresh checkout, it restores a timestamp only for an identical tracked input in the same workspace path, after verifying the working files against the index. Changed or unknown inputs are made newer than every restored Cargo invocation, including scoped targets, so Cargo rebuilds them even when a fallback snapshot was produced after checkout. The three provenance-sensitive host packages are still cleaned explicitly. Restoration counts toward the check; capturing the next snapshot and saving the cache are reported as subsequent CI maintenance. This is compiler-cache freshness metadata, not a test-source coverage ledger.

One narrow library graph verifies SQLite without the PostgreSQL driver and observations without OTLP. Its Cargo artifact report proves the actual selected features; broad execution alone cannot prove absence. The combined graph owns normal CLI builds and tests, while real standalone consumers and positive/negative custom-HTTP probes retain their separate API boundaries.

The initial SQL fault and pending-tool reductions passed their targeted tests on all three backends, both serially and at four slots. Full combined execution, final retained-family reductions, the three local/CI qualification cases and independent final review remain pending.

The following sections preserve evidence from the earlier plan. Their old schedules, tuple counts, concurrency choices and percentage protocol are historical; the coverage policy and 600-second contract above govern current acceptance.

The previous implementation base was `58535cfc234c2a3d514487b0f72d646e4ba33110` (use `git rev-parse 58535cfc` for the full revision).
That implementation lived on `feat/test-suite-performance` in its own checkout. The
pre-existing provider and orchestrator edits are excluded. This document records
coverage ownership; it is not a claim that performance qualification has passed.

## Historical: Commands and reports

```sh
cargo xtask check --list > target/test-performance/inventory.json
cargo xtask check --timings target/test-performance/run-01/timings.json \
  > target/test-performance/run-01/raw.log 2>&1
```

Create the raw-log parent directory before shell redirection. `--list` constructs
the same ordered schedule as execution, without preparing probes, building WASM,
or starting tests. IDs are derived from the logical working directory and argv;
internal assertions have descriptive IDs. JSON includes argv, feature selection,
environment override **names**, prerequisites, expected checks, and directory.
Environment values, service URLs, and captured telemetry output are excluded.
Raw command output goes to the caller's stdout/stderr. Store raw logs locally,
separately from sanitized summaries; do not publish credentials from raw logs.

The timing document is atomically replaced before and after each gate. A failed
command or assertion makes the aggregate fail, records the child exit status, and
marks every later gate cancelled. Build-and-test durations include compilation;
they must not be presented as test-only durations or summed with their nested
Cargo summaries. SIGINT/SIGTERM cancel the active Unix process group, reap its leader, record
a cancelled phase and cancel later obligations. A forcibly killed runner can
still leave a running record; that is not completed evidence. The PostgreSQL runner also supervises descendants across process groups and
sweeps only registered resources after those descendants stop.

## Historical: Coverage rules

The baseline owner of each row below is the corresponding invocation in
`xtask/src/main.rs` at the implementation base; the current owner is the same
invocation in the data-driven schedule except the explicit transfers below.
Each selected test target owns all of its discovered tests,
assertions, backend branches, and actual subprocess journeys. A helper returning
early without its marker is not a durability test execution. Workspace feature
unification must be inspected before transferring ownership to the workspace run.

Backend labels describe requested features, not proof of executed live tests.
Offline PostgreSQL early returns give no required-service credit. At baseline,
PostgreSQL URLs are supplied only in the separate required lane. Minimal-feature
compilation, provider feature absence, exporter differences, consumer builds,
validating WIT copies, and example output assertions remain distinct obligations.
Default Cargo test invocations also own their applicable doctests. The nextest adapter explicitly transfers doctest ownership to Cargo.

## Historical: Offline gate ledger

`cmd-*` IDs below are abbreviated only by omitting the `command-` prefix. The
machine inventory is authoritative for argv and ordering. Formatting and static
WIT/glue checks now run before compilation. The table maps baseline obligations;
cache-release rows are additional resource-lifecycle checks. The transfer table
below supersedes ownership for consolidated rows. Scripts retain all internal
commands and expected failures described after the table.

| Gate | Package/target or static check | Backend/feature boundary | Role and external prerequisites |
| --- | --- | --- | --- |
| `telemetry-messages` | telemetry-messages | static/filesystem | all runtime telemetry message sites use the qualified redacted vocabulary |
| `agui-pins` | agui-pins | static/filesystem | all consumer manifests and locks use the qualified AG-UI source and revision |
| `prepare-workspace-context-probe` | prepare-workspace-context-probe | static/filesystem | all preparation/assertion steps succeed |
| `local-probe-workspace-context-probe` | `cargo check --all-targets` | default/workspace-unified | local API consumer against checkout; recorded Git pin retained |
| `release-workspace-context-probe` | Release workspace-context-probe cache lease | filesystem | holds refresh/build lock until Cargo and its descendants finish |
| `prepare-prompt-contribution-probe` | prepare-prompt-contribution-probe | static/filesystem | all preparation/assertion steps succeed |
| `local-probe-prompt-contribution-probe` | `cargo check --all-targets` | default/workspace-unified | local API consumer against checkout; recorded Git pin retained |
| `release-prompt-contribution-probe` | Release prompt-contribution-probe cache lease | filesystem | holds refresh/build lock until Cargo and its descendants finish |
| `prepare-result-transform-probe` | prepare-result-transform-probe | static/filesystem | all preparation/assertion steps succeed |
| `local-probe-result-transform-probe` | `cargo check --all-targets` | default/workspace-unified | local API consumer against checkout; recorded Git pin retained |
| `release-result-transform-probe` | Release result-transform-probe cache lease | filesystem | holds refresh/build lock until Cargo and its descendants finish |
| `wit-copies` | wit-copies | static/filesystem | all guest and macro WIT copies equal the root contract |
| `cmd-0e8659e26ed9a7b1` | `wasm-tools component wit wit -o /dev/null` | independent tool/consumer boundary | WIT contract validation |
| `cmd-da42e73f3d0adeb6` | `cargo build --workspace --target wasm32-wasip2 --release` | default/workspace-unified | WASM fixture build; installed wasm32-wasip2 target |
| `wasm-fixture-aliases` | wasm-fixture-aliases | static/filesystem | positive aliases and SHA-256 fixture manifest generated |
| `cmd-61d36d44900fc33e` | `cargo build --workspace --target wasm32-wasip2 --release` | default/workspace-unified | WASM fixture build; installed wasm32-wasip2 target |
| `cmd-09af9e43612942a4` | `cargo fmt --all -- --check` | default/workspace-unified | local API consumer against checkout; recorded Git pin retained |
| `cmd-2be6715887258370` | `cargo test -p server-host-probe --features wasm --test shared_wasm` | wasm | all selected tests and doctests; child/helper journeys retained |
| `cmd-425c570625856998` | `cargo test -p server-host-probe --features sqlite` | sqlite | all selected tests and doctests; child/helper journeys retained |
| `cmd-309d78a12aadb850` | `cargo run --quiet -p server-host-probe -- memory` | default/workspace-unified | host/example execution; offline inputs |
| `cmd-0d1598ecd5a72eef` | `cargo test -p crabber --features sqlite --test server_host --test durable_interactions --test durable_controls --test session_management` | sqlite | all selected tests and doctests; child/helper journeys retained |
| `cmd-19873bec77bda067` | `testdata/server-host-consumer/check.sh` | independent tool/consumer boundary | locked server-host consumer |
| `cmd-5d8503862eaca8d2` | `cargo check -p crabber --no-default-features --features custom-http` | custom-http | compile/lint boundary; no behavioral replacement credit |
| `cmd-cb271c2c6c24aa54` | `testdata/custom-http-feature-probe/check.sh` | independent tool/consumer boundary | custom HTTP positive and negative feature-boundary probe |
| `cmd-02f397e4e04ae2cb` | `cargo clippy -p crabber-providers --all-targets --features all-providers,custom-http -- -D warnings` | all-providers,custom-http | compile/lint boundary; no behavioral replacement credit |
| `cmd-3380040f41f8db79` | `cargo test -p crabber-providers --no-default-features --features custom-http --test custom_http` | custom-http | all selected tests and doctests; child/helper journeys retained |
| `cmd-ccdcf6a2093f6996` | `cargo test -p crabber-providers --features all-providers,custom-http` | all-providers,custom-http | all selected tests and doctests; child/helper journeys retained |
| `cmd-8a66597aa6b7e596` | `cargo test -p crabber --features wasm,sqlite,postgres,custom-http --test typed_input --test typed_output` | wasm,sqlite,postgres,custom-http | all selected tests and doctests; child/helper journeys retained |
| `cmd-fff1a10359c7475f` | `cargo clippy -p crabber-session --features sqlite --all-targets -- -D warnings` | sqlite | compile/lint boundary; no behavioral replacement credit |
| `cmd-b76ea856f30f200d` | `cargo test -p crabber-session --features sqlite` | sqlite | all selected tests and doctests; child/helper journeys retained |
| `cmd-11cf6b830b966628` | `cargo clippy -p crabber --features sqlite --all-targets -- -D warnings` | sqlite | compile/lint boundary; no behavioral replacement credit |
| `cmd-5af306f94ddff2e3` | `cargo clippy -p crabber-runtime --features sqlite --all-targets -- -D warnings` | sqlite | compile/lint boundary; no behavioral replacement credit |
| `cmd-4294abf030ba0abd` | `cargo test -p crabber --features sqlite --test admission_receipts --test bounded_snapshots --test fenced_abandon --test result_transform_interrupt --test unknown_tool_depth --test sqlite_builder` | sqlite | all selected tests and doctests; child/helper journeys retained |
| `cmd-a370b11d6e19992c` | `cargo test -p crabber --features datadog,sqlite --test trace_context` | datadog,sqlite | all selected tests and doctests; child/helper journeys retained |
| `cmd-415680cd339f9a49` | `cargo test -p crabber --features wasm,sqlite --test wasm_roles` | wasm,sqlite | all selected tests and doctests; child/helper journeys retained |
| `cmd-059d755fa985bdd7` | `cargo test -p crabber-runtime --features sqlite` | sqlite | all selected tests and doctests; child/helper journeys retained |
| `cmd-d863f32692e745d7` | `cargo test -p workspace-context-probe --features sqlite` | sqlite | all selected tests and doctests; child/helper journeys retained |
| `cmd-108edd111ef82dbd` | `cargo test -p minimal-embed sqlite_interrupt_then_fresh_process_continues_session` | default/workspace-unified | all selected tests and doctests; child/helper journeys retained |
| `cmd-5c54708068501ac7` | `cargo clippy --workspace --all-targets -- -D warnings` | default/workspace-unified | compile/lint boundary; no behavioral replacement credit |
| `cmd-a1d543035e4af382` | `cargo test --workspace` | default/workspace-unified | all selected tests and doctests; child/helper journeys retained |
| `cmd-2d6a44860c9e86db` | `cargo run --quiet -p agui-sse -- --check` | default/workspace-unified | host/example execution; offline inputs |
| `cmd-04f6931ed55c3c0b` | `cargo test -p crabber --features opencode-go --test opencode_go_chat` | opencode-go | all selected tests and doctests; child/helper journeys retained |
| `cmd-3fc7a3979f742da8` | `cargo test -p crabber --features datadog --test trace_context` | datadog | all selected tests and doctests; child/helper journeys retained |
| `cmd-8879e998d2360767` | `cargo test -p crabber-obs --features otlp` | otlp | all selected tests and doctests; child/helper journeys retained |
| `cmd-20d1c2666a9863fe` | `cargo clippy -p crabber-obs --features otlp --all-targets -- -D warnings` | otlp | compile/lint boundary; no behavioral replacement credit |
| `cmd-12c5addc557a28a8` | `cargo test -p crabber --features otlp --test trace_context` | otlp | all selected tests and doctests; child/helper journeys retained |
| `cmd-f8e272ca77b98da0` | `cargo test -p crabber --features otlp,sqlite --test telemetry` | otlp,sqlite | all selected tests and doctests; child/helper journeys retained |
| `cmd-ee76f91780f221ed` | `cargo run --quiet -p datadog-export` | default/workspace-unified | host/example execution; offline inputs |
| `cmd-6ca6d9007e873ae7` | `cargo run --quiet -p host-trace -- memory` | default/workspace-unified | host/example execution; offline inputs |
| `cmd-97f5f7c7cfe8f2a9` | `cargo run --quiet -p admission-receipt -- --memory` | default/workspace-unified | host/example execution; offline inputs |
| `cmd-3a8d74dd0fe6fa92` | `cargo run --quiet -p bounded-snapshot -- --memory --check` | default/workspace-unified | host/example execution; offline inputs |
| `cmd-38d7df54f3fa0ba8` | `cargo run --quiet -p operational-telemetry -- --check` | default/workspace-unified | host/example execution; offline inputs |
| `cmd-2812235c1e19ff3a` | `cargo run --quiet -p host-trace --features sqlite -- sqlite` | sqlite | host/example execution; offline inputs |
| `cmd-d3e5d127f3c11c81` | `cargo run --quiet -p admission-receipt --features sqlite -- --sqlite` | sqlite | host/example execution; offline inputs |
| `cmd-997a253eaeec4f9b` | `cargo run --quiet -p bounded-snapshot --features sqlite -- --sqlite --check` | sqlite | host/example execution; offline inputs |
| `cmd-742a5c6c14708059` | `cargo run --quiet -p fenced-abandon --features sqlite -- --sqlite` | sqlite | host/example execution; offline inputs |
| `cmd-82b3527680b9c3ad` | `cargo test -p crabber --features wasm --test wasm_state` | wasm | all selected tests and doctests; child/helper journeys retained |
| `cmd-d19ccc04599f83b2` | `cargo test -p crabber --features wasm --test wasm_roles` | wasm | all selected tests and doctests; child/helper journeys retained |
| `cmd-6a65ac97a97dc1e0` | `testdata/external-consumer/check.sh` | independent tool/consumer boundary | external consumer check |
| `minimal-embed-glue` | minimal-embed-glue | static/filesystem | exactly one glue marker pair and at most 60 glue lines |

The three local-source probes each own a prepare, all-targets Cargo check and
lease-release gate. Generated sources and targets persist below the resolved
`CARGO_TARGET_DIR`, keyed by the canonical checkout path and probe name. Each
probe holds an advisory file lock across source refresh and Cargo execution.
Refresh removes stale copied files, copies the workspace lock and original
sources, and rewrites only the disposable Crabber dependency. Tracked Git pins
remain unchanged and are not qualified by a local source check.

Two unchanged runs demonstrated reuse: the warm run checked only the three probe
packages and completed in under a second. Functional checks detected an edited
tracked consumer source, restored it successfully, removed stale generated
copies, and ran two simultaneous invocations successfully. These are cache
correctness checks, not full-gate performance qualification.

The WASM prerequisite builds both positive and negative fixture workspaces,
copies positive aliases, and writes SHA-256 manifest entries. Its target override
is `fixtures/wasm/target`, including negative fixtures. WIT assertions compare
root, guest, and macro copies before the independent `wasm-tools component wit`
validation. Minimal embed has exactly one glue marker pair and a 60-line limit.

The custom HTTP script owns its positive compilation and dependency-feature
graph check, four built-in constructor absence cases, nine custom API absence
cases in built-in-only mode, eight mixed-feature setter absence cases, and seven
raw-client escape absence cases. Each case must fail for its own forbidden API;
infrastructure/build failure is not equivalent evidence. The script now uses
unique scratch sources, a per-checkout persistent target,
and a whole-invocation `flock` lease. Every negative case has a successful
preflight for its exact feature graph and allowed receiver API. A completed
Cargo JSON rejection must have the expected error code on the probe source;
dependency errors, incomplete builds and malformed output fail the gate.
Python helpers use an isolated standard-library interpreter (`-I -S`), so local
Python startup hooks cannot alter the gate. Python 3 and `flock` are prerequisites.

A deterministic diagnostic demonstrated Cargo reusing a newer successful probe
artifact for an older forbidden-call source in a different scratch workspace
with the same package name. Each invocation now uses a unique package identity
as well as the cache lease. The original concurrency failure and controlled
reproduction remain in local evidence; the corrected concurrent invocations
passed.
Provider-only and mixed-provider unit/integration runs remain separate.

The server-host consumer runs its locked external host. The external consumer
builds `external-guest` and `external-controls` for wasm32-wasip2 in release mode
and runs `external-host`. Local API probes do not replace these consumers.

The captured Datadog example requires `status=Completed`, a retry diagnostic on
stderr, and absence of all three fixture-secret sentinels on both streams. Its
child removes `DD_API_KEY`, `OTEL_EXPORTER_OTLP_ENDPOINT`, and
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, and configures the stderr layer. The
operational-telemetry example verifies compiled root, exact SHA and clean Git
state; qualify only a committed checkout using its own target artifacts.

## Historical: Required PostgreSQL baseline ledger

The baseline owner is `.github/workflows/ci.yml`'s `postgres` job. Every command
below is retained and has no replacement evidence yet. The baseline service is
PostgreSQL 14 with normal durability, and the job requires service execution via
`CRABBER_REQUIRE_POSTGRES`. Test inputs use `CRABBER_TEST_POSTGRES_URL`; examples
use `CRABBER_POSTGRES_URL`. They are test-only connection variable names, never
permission to delete an application database. The new managed runner must own
its own isolated endpoint/databases before changing any of these commands.

| Baseline command | Scenario/assertion family and subprocess role |
| --- | --- |
| `cargo test -p crabber-session --features postgres` | PostgreSQL contracts, concurrency, migration and process durability; every selected assertion and marked child journey |
| `cargo test -p crabber-runtime --features postgres` | Typed middleware durable resume and plan-seal reconstruction; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test result_transform_interrupt -- --nocapture` | Public result-transform interruption and protected durable settlement; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test admission_receipts -- --nocapture` | Public facade response-loss and fresh-process reconciliation; every selected assertion and marked child journey |
| `cargo run --quiet -p admission-receipt --features postgres -- --postgres` | Noninteractive PostgreSQL admission demo; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test bounded_snapshots -- --nocapture` | Public bounded snapshot journey and fresh-process continuation; every selected assertion and marked child journey |
| `cargo run --quiet -p bounded-snapshot --features postgres -- --postgres` | Noninteractive PostgreSQL bounded snapshot demo; every selected assertion and marked child journey |
| `cargo test -p crabber --features datadog,postgres --test trace_context -- --nocapture` | Required real Datadog payload and fresh-process recovery contracts; every selected assertion and marked child journey |
| `cargo run --quiet -p host-trace --features postgres -- postgres` | Noninteractive PostgreSQL host tracing and export capture; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test fenced_abandon -- --nocapture` | Public fenced abandonment atomic rollback and process crash; every selected assertion and marked child journey |
| `cargo run --quiet -p fenced-abandon --features postgres -- --postgres` | Noninteractive PostgreSQL fenced abandonment demo; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test unknown_tool_depth -- --nocapture` | Deeply nested unknown-tool record stays decodable; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test storable_records -- --nocapture` | Storable records remain readable; every selected assertion and marked child journey |
| `cargo test -p workspace-context-probe --features postgres -- --nocapture` | Public workspace context probe and fresh-process resume; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test server_host -- --nocapture` | Public configured host and scoped execution ownership; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test durable_interactions -- --nocapture` | Public durable interactions and frozen continuation recovery; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test session_management -- --nocapture` | Public session management, imported history and quiet deletion; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test typed_output -- --nocapture` | Public typed tool output persistence and next-provider request; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test future_queue -- --nocapture` | Public future queue claim, preparation and process recovery; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test session_defaults -- --nocapture` | Public session defaults and cold selection binding; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test durable_controls -- --nocapture` | Public exact-run cancellation and ordered steer controls; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test admission_replay -- --nocapture` | Public original admission replay without replacement factories; every selected assertion and marked child journey |
| `cargo test -p crabber --features postgres --test session_owners -- --nocapture` | Public pre-session ownership and independent session resources; every selected assertion and marked child journey |
| `cargo test -p server-host-probe --features postgres -- --nocapture` | Offline customized server host and injected Store adoption; every selected assertion and marked child journey |

Migration obligations include blank databases, each legacy predecessor,
unsupported schema rejection, current-schema idempotence and fast path. A
pre-migrated template cannot replace those migrations under test. Runtime and
facade obligations include actual old-owner fencing, native resource Join,
response loss, fresh executable processes, killed workers, pending tools,
queue before/after commit, rollback and durable exact budget/receipt readback.
Fixed IDs, truncations, schema mutation and advisory locks require a database
owned by the logical journey before process-parallel execution is enabled.

## Historical: Long-contract scenario accounting

Before splitting, extract a tuple ledger from each original loop. In particular:

* Runtime `preparation_resources/successor/sql.rs`: core, retention, recovery,
  and pending histories for each supported SQL backend.
* Runtime `preparation_resources/pending_tool.rs`: frontend × unkeyed × fault
  cases, preserving fault stage and causal transitions within each history.
* Session `session_owner/owned_scope/task/pending/tool_tests.rs`: frontend,
  recovery, unkeyed, fault and actual same-task identity obligations.
* Facade `wasm_state`: 212-run state-bound journey and its >=1000 assertion;
  replay/configuration cases may separate only when state independence is shown.

The completed tuple mappings and backend validation are recorded in
`test-performance-scenarios.json`. Scheduler measurements are recorded in
`test-performance-resource-selection.json`. No loop bounds or production Store
behavior changed.

## Historical: Reproducible qualification

Record exact source/tree, tool versions, machine CPU/RAM, concurrent workload,
profile settings, installed tools, registry/Git cache state and PostgreSQL image
identity. Use dedicated baseline/candidate checkouts and output roots. Inventory
the main target, `fixtures/wasm/target`, all three standalone-consumer targets,
and generated probe targets. A fresh main `CARGO_TARGET_DIR` alone is not cold.
Do not borrow binaries from another checkout or delete a user's global caches.

Run the original offline and required PostgreSQL schedules before deleting
coverage. Collect one cold full run and three alternating paired warm full runs
per revision on the same machine. Prewarm equivalent full inventories, including
probes and fixtures. Preserve every failed run with exit status; a later pass
does not erase or causally explain it. Record full wall time and phase times,
peak RSS/CPU, connections, separate installation/image-pull time, CI critical
path and aggregate job-seconds without double-counting nested durations.

Acceptance is >=30% lower warm full-gate median, <=10% cold wall regression and
<=10% aggregate CI job-time regression. PostgreSQL execution's 50% reduction is
a target. Compare concurrency 1/2/4 with unchanged deadlines and no retries.
Run changed failure-prone contract families five consecutive times and retain
first failures. All baseline obligations need passing replacement evidence;
equal discovered counts alone do not prove equivalent semantics.

### Benchmark driver

After installing the pinned tools, use separate clean checkouts and new report
and archive directories:

```sh
python3 -I -S scripts/test-performance/paired.py \
  --baseline "$CRABBER_BASELINE_DIR" \
  --candidate "$CRABBER_CANDIDATE_DIR" \
  --candidate-revision "$CRABBER_CANDIDATE_REVISION" \
  --output "$CRABBER_MEASUREMENT_DIR" \
  --archives "$CRABBER_ARCHIVE_DIR" \
  --incremental 1
```

The driver fixes matching dev/test debug settings to zero and uses four Cargo
build jobs unless overridden with `--build-jobs`. `--incremental 1` retains local
incremental compilation; use a separate run with `--incremental 0` for the CI
profile. Product commands do not disable local incremental compilation.

Each lane has its own target directory. PostgreSQL runs before offline fixture
generation, and every nested effective compiler root is freshened for cold runs.
Inactive artifacts are compressed outside the active compiler roots and restored
with nanosecond timestamps; only explicitly listed ignored outputs are removed.
Use storage large enough for those archives. Reports remain separate from
archives. The driver rejects competing build/test runners and stops on a failed
sample without retrying it away.

The default baseline is the exact original `58535cfc` revision. Optional
`--baseline-revision 9890af9c0f6ec22e94687f13a0d8918f6a0b19c4` selects the WP1
instrumentation-only control with exact per-gate reports. The driver verifies
that its product, behavioral tests, fixtures and workflow trees are identical to
the original base. Declare the selected measurement revision explicitly in the
published results; instrumentation is not a performance optimization.

CI keeps two independent jobs. Sanitized timing/fixture reports are uploaded even
when a gate fails. Checkout, tool installation, cache transfers and service
startup belong to hosted job time; local command timings alone do not establish
that end-to-end CI result. The successful original CI run is
[recorded here](https://github.com/mattsp1290/crabber/actions/runs/39523094271).

Rollback of the separate PostgreSQL CI runner-choice commit restores the managed
`--runner cargo` lane while retaining database isolation. A template regression
must use blank provisioning and the original real migrations, rerun correctness
gates, and remeasure before a performance claim. Restore a deduplicated gate if
its replacement coverage is disproved.

## Historical: Current evidence and limitations

The original clean base passed both complete lanes before coverage changes.
The implementation then passed a clean, unchanged full offline gate and all 24
required PostgreSQL commands under Cargo at one, two and four threads. The full
required nextest schedule also passed, including actual demos, the controller
process, allocation assertions and tracing/export captures. Each runner recorded
363 matched history fixtures and four matched demo fixtures before the named
contract splits; containers were independently verified absent.

Affected families passed every backend at one slot and five complete four-slot
repetitions. The quiet one/two/four-slot comparison selected four PostgreSQL
slots. These correctness and scheduler results do not establish final whole-gate
performance thresholds. The integrated final configuration, cold/paired warm
comparison and hosted candidate CI still require qualification. Failed provenance
and overwritten-executable attempts remain retained locally and are never
counted as passing samples. Existing issues `crabber-pxdk`, `crabber-09ku`,
`crabber-9e8u` and `crabber-7j4w` retain their separate scopes.

## Historical: PostgreSQL fixture infrastructure

```sh
cargo run -p xtask --features postgres-check -- check-postgres --fixtures-only
cargo run -p xtask --features postgres-check -- check-postgres
cargo run -p xtask --features postgres-check -- check-postgres --package crabber-session --filter postgres_contract
```

The private `crabber-test-support` crate depends on SQLx/Tokio/UUID, never the
session, runtime or facade crates. The optional xtask feature owns
`testcontainers-modules` 0.15.0 (locked `testcontainers` 0.27.3), and PostgreSQL 14
with manifest digest
`sha256:c2427de38f998489d36de7ca3553db2134872c400f2b08be4b824e5c50e4d619`.
Both the module's fsync default and PostgreSQL startup settings are overridden;
SQL verifies major version 14, fsync, synchronous_commit and full_page_writes.
The default offline xtask build does not start or require Docker.

Set `CRABBER_TEST_POSTGRES_ADMIN_URL` only for a dedicated external test server
with create/drop database and schema privileges. The runner never takes its
administrative endpoint from `CRABBER_POSTGRES_URL`. Without the administrative
override it starts its own container with a dynamic port. `--fixtures-only`
additionally tests a temporary restricted role, so that qualification command
requires role-administration privileges on the dedicated server.

The runner creates a random run identity, holds a dedicated session advisory
lock, and owns a private schema containing create-intent records. Every generated
database name must match that exact run identity and an ownership row. Current
mode clones the migrated template; Blank mode clones template0. The runner
migrates and closes template connections before cloning. Duplicate-name rejection
revokes intent so an existing database cannot be swept. Each database's CREATE
and DROP share an administrative advisory lock on their actual SQL backend, so
cleanup cannot overtake an in-flight CREATE whose client lost its response.

Tests must retain a fixture for their entire logical journey and explicitly pass
its URL to every child. Normal cleanup waits for all connections to disappear;
it fails after its bounded drain wait instead of forcing a successful result.
Put the history inside an inner async scope, await resource/child cleanup, close
raw pools, and drop all pool handles, Stores and Arc holders before `finish`.
`TestDatabase::run_if_available` supplies that scope boundary. Its absent-service
path is explicit; required mode and legacy URL-only configuration fail.

A real qualification case exposed a raw SQLx pool still reporting size 1 after
`close().await` while the test retained its last handle. Dropping the pool before
finishing the database resolved that fixture defect with the original deadline.
The failing diagnostic runs are retained locally; later passes do not erase them.

Linux execution uses a subreaper and checks process identity/start time while
following descendants across process-group changes. Handled SIGINT/SIGTERM stop
owned processes and reap them before database sweep and container removal.
A remaining helper is a failed gate even when it can subsequently be cleaned up.
External cleanup acquires the run advisory lock and refuses a live owner:

```sh
cargo run -p xtask --features postgres-check -- check-postgres --cleanup-run "$RUN_NONCE"
```

SIGKILL cannot execute Rust Drop or signal handlers. This was tested: the managed
container remains until explicitly removed. The runner prints an opaque container
ID and `crabber.test-runner` ownership label; after verifying the runner and its
process tree have stopped, inspect that exact ID/label and remove only that owned
container. A killed external run can leave registered databases; stop its owned
processes first, then use `--cleanup-run` against the same dedicated admin server.
No machine-wide container removal or database-prefix deletion is supported.

Credential-free real-server checks have exercised current/blank mode, identical
session IDs, truncation/schema/advisory-lock isolation, retained Arc Store drain
refusal, child attachment identities, panic after CREATE, a killed parent with a
separately grouped grandchild, denied CREATEDB, active-owner cleanup refusal, and
two concurrent external runners. A final external-server query found zero owned
databases, schemas or roles. Missing Docker fails before tests. Handled SIGTERM
returned 143 and removed its container; the forced-kill test manually reclaimed
its verified owned container afterward.

The full runner retains all original 24 CI obligations: 20 test commands and four
actual demos. Nextest runs tests; explicit Cargo commands retain doctests and
demos. Every PostgreSQL parent owns its database, and children attach to that
parent. Cross-test database locks were removed after isolating their resources.
Use `--runner cargo` for the tested Cargo fallback.

## Historical: Offline and source inventories

`cargo xtask check` explicitly removes live PostgreSQL configuration, provider
API keys and live-export credentials/endpoints in child commands. It does not
mutate the host process environment. The separate PostgreSQL command owns
required-service coverage. A focused subprocess test verifies removal while
retaining the captured example's explicit stderr recipe.

```sh
cargo xtask inventory-tests > target/test-performance/source-inventory.json
```

This read-only source inventory records explicit test declarations, cfg guards,
helper environment-variable names, calls and exact source byte ranges for audits.
It does not expand macros, resolve features or prove execution; Cargo/nextest
lists and actual required-service results remain necessary. A parser test covers
nested guards and UTF-8 offsets. Deliberate wrong-root and wrong-SHA executions
of operational-telemetry were rejected by their respective provenance guards.

The clean original `58535cfc` offline schedule passed in 3248.415 seconds. The
original PostgreSQL CI schedule also passed all 24 commands with its dedicated
container removed. Both runs are exploratory timing evidence because the host
had concurrent work; they establish passing original obligations, not the paired
performance thresholds.


Original Cargo discovery also passed for all 41 distinct invocation variants at
`58535cfc`, recording each feature graph and test list. This is source-revision
coverage evidence; lists alone do not replace executing required database tests.
After preserving these logs and summaries, the exploratory baseline's compiler
outputs were removed to reclaim disk. Qualified cold/warm comparisons must
rebuild both revisions under matching conditions.

Fixture support is a private, unpublished dev dependency. Its PostgreSQL driver
is feature gated: SQLite-only dependency graphs must not acquire PostgreSQL just
because test support exists. `cargo tree -p crabber-session --features sqlite -e
features` was checked for that boundary.

Adopted child helpers use `TestDatabase::child_environment` on their specific
`Command`, then `attached_url` inside the marked helper. Attachment rejects a
foreign server, template database, unregistered database or mismatched opaque
identity, and verifies `current_database()` through an independent connection.
Only the database identity is printed. No process-wide environment mutation is
used. The workspace-context probe's resume and identity-drift children have
passed this handoff against their parent's owned fixture.

The normal PostgreSQL budget is four slots, with runtime contracts weighted two.
`--test-threads 1`, `2` or `4` selects an explicit comparison cap. Cargo fallback
defaults to two libtest threads. Independent runners have independent budgets and
add load. Concurrent lanes in one checkout require distinct `CARGO_TARGET_DIR`
values and fixed sources.

## Historical: Per-history PostgreSQL adoption

Session, runtime, facade and host-probe parents use explicit owned fixtures.
Current-schema histories clone the migrated template and still execute their
original migration/current-fast-path calls. Migration and predecessor histories
use blank databases and preserve their original schema setup. Every reconnect,
raw pool and child receives the same explicit history URL. Shared process helpers
carry either an explicit PostgreSQL URL or the original SQLite path; they never
infer a current database from process-wide state. Backend termination and activity
queries filter on `datname = current_database()` before selecting worker PIDs.

The removed guards protected the shared test database: session `TEST_LOCK` and
its two accessors, facade admission/fenced locks and trace journey lock. Each
history now owns its schema and data, temporary files use unique paths, and
workers attach to their own parent. Intra-history barriers and transaction locks
remain intact. The fixture qualification deliberately uses identical logical
keys in two databases, then truncates and changes one schema while checking the
other remains correct; it also proves advisory-lock independence.

Seven histories intentionally retain uncertain controllers until their original
Tokio test runtime shuts down. Their synchronous wrappers preserve that exact
runtime configuration and drop the history runtime before normal fixture drain.
They cover two pending-tool None-ack histories, three service/acquisition
histories, facade future-queue and facade session-defaults. The original history
helpers and their assertions are unchanged. Provider disposal failure additionally
retains a sticky owner after runtime shutdown: it runs in an actual controller
process, verifies attachment to its parent's database, and acknowledges completion
with the opaque identity. The parent requires both that acknowledgement and
successful process exit before normal cleanup. A zero-match helper invocation or
foreign acknowledgement is rejected. This process boundary never fabricates a
native Join or changes persisted uncertainty.

Initial connection-drain failures after business assertions identified fixture
lifetime defects. Preserving the original runtime shutdown order corrected them;
the diagnostics remain retained locally. Full Cargo and nextest required lanes
subsequently passed.

The private fixture crate selects its PostgreSQL feature by default when it is a
workspace root. Every parent dev dependency explicitly disables those defaults;
SQLite-only package builds still exclude `sqlx-postgres`. This avoids an observed
workspace feature-resolution failure in which dev-only forwarding enabled the
fixture crate's PostgreSQL cfg without linking its optional dependencies. A small
independent workspace reproduced that failure and the root-default correction.
The failed discovery diagnostics are retained locally. Corrected feature graphs,
narrow SQLite builds and the full offline gate subsequently passed.


## Historical: Consolidated offline ownership

[The machine-readable matrix](test-performance-matrix.json) records the original
revision, argv, resolved target features, counts and SHA-256 hashes of sorted test
names. Cargo discovery passed for the updated workspace feature selection: all
1,710 entries from the eleven transferred invocations are present. The owner
currently discovers 2,860 tests across 128 test/doc targets. These are discovery
counts, including unmarked helpers and optional PostgreSQL wrappers; they are not
claims of executed durability histories. Full execution remains necessary.

| Original execution | Replacement execution | Retained boundary |
| --- | --- | --- |
| Session SQLite suite and its doctests | Workspace session suite and workspace doctests | SQLite-only all-target Clippy |
| Runtime SQLite suite | Workspace runtime suite; resolved features remain exactly `default,sqlite` | SQLite-only all-target Clippy |
| Facade SQLite admission/bounded/fenced/result-transform/unknown-tool/builder targets | Same target cases in workspace | SQLite-only facade all-target Clippy |
| Facade SQLite host/interactions/controls/session-management targets | Same target cases in workspace | SQLite-only facade all-target Clippy |
| Minimal embedding SQLite restart filter | Same stdout target case in workspace | Original executable and fresh-process assertions |
| Server host WASM and SQLite suites | Workspace explicitly enables both probe features | Separate WASM-only and SQLite-only all-target checks |
| Workspace context SQLite suite | Workspace explicitly enables probe SQLite | SQLite-only all-target check; independent pinned consumer remains |
| WASM state, WASM roles, WASM+SQLite roles | Same tests in workspace | WASM-only and WASM+SQLite target checks |

The session broad run adds PostgreSQL wrappers; SQLite implementation branches
and shared Memory/SQLite contract calls remain the same. Runtime's package
features are identical in the original narrow and broad discovery. Facade
backend wrappers are additive; SQLite cases still pass explicit SQLite paths and
use the same Store implementations. The WASM state history has only its WASM
module guard; role backend wrappers are separately guarded and the SQLite
continuation is retained. No test functions were removed by consolidation.
Minimal-feature compilation still checks the configurations excluded from the
broader execution. Compatible SQLite Clippy passes now select all three packages
in one invocation, with the same all-target scope and warnings-as-errors policy.

Exporter-isolated Datadog+SQLite, Datadog-only, OTLP-only and OTLP+SQLite execution
remain separate because the workspace enables multiple exporters. Custom-HTTP
only, built-in-only and mixed constructor/negative probes also remain separate,
as does mixed-feature typed I/O execution. Live PostgreSQL still belongs solely
to the required fixture runner. External consumers, executable examples, WIT,
fixture builds, provenance checks and glue assertions retain their owners.


## Historical: Offline nextest integration

Install the pinned tooling described in the README, then run:

```sh
cargo xtask check
cargo run -p xtask --features postgres-check -- check-postgres
cargo run -p xtask --features postgres-check -- check-postgres --runner cargo
cargo xtask qualify-nextest
```

The installer reads `.config/test-tools.json`, verifies the exact 0.9.138 archive
SHA-256 for Linux aarch64/x86_64, and installs in the requested directory. Both
normal lanes reject a different nextest version before tests. Fixture-only
qualification, inactive-run cleanup and explicit Cargo fallback do not require
nextest. Adapters force zero retries and no fail-fast. A failed invocation fails
the root gate and cancels later obligations. Doctests remain explicit Cargo
executions; demos remain actual executables.

`.config/nextest.toml` caps global slots at four. Runtime and WASM contracts have
weight two; SQLite, WASM and PostgreSQL groups have four slots. The required
PostgreSQL profile assigns every discovered entrypoint to its group, including
generic names, new cases and helpers. A list-based gate verifies the catch-all
assignment. Explicit PostgreSQL one/two/four-slot settings preserve weights. The
30-minute outer hang bound exceeds unchanged Rust deadlines. See nextest's
[test groups](https://nexte.st/docs/configuration/test-groups/) and
[weights](https://nexte.st/docs/configuration/threads-required/).

Discovery and feature/cfg audits prove transfers in the ownership ledger. A real
failing nextest fixture propagated status 100 and cancelled the following gate.
A SQL-backed failing PostgreSQL fixture also propagated 100 through the required
runner, completed owned cleanup and removed its container. SIGTERM while a real
controller child held a database connection returned 143; the child and container
were verified gone. A missing Docker endpoint failed before fixtures or tests.
These expected-failure qualifications never count as passing behavioral tests.

Both lanes remove live provider/exporter configuration from child commands while
preserving explicit stderr recipes. Required PostgreSQL supplies its scoped
fixture/admin environment and demo handoffs. An actual subprocess regression
verifies that log/metric export settings and Datadog labels disappear while the
fixture identity/endpoint and `RUST_LOG` remain intact.

Combined facade/session/runtime SQLite Clippy retains the facade's pre-existing
raw SQLx PostgreSQL dev dependency. A separate all-target session/runtime SQLite
check preserves their PostgreSQL-driver-free boundary. Fixture defaults remain
disabled in every consuming dev dependency. The child helper's unused-async
allowance is limited to configurations without PostgreSQL verification.

WASM preparation holds a cancellable advisory lease in its actual output root
through the offline gate's last consumer. Standalone `build-fixtures` releases it
after both fixture workspaces finish. Failure and cancellation release it after
supervised children drain. Source probes retain their existing lock paths. A real
invocation waiting for an external lease returned 143 before building or writing
aliases. The full offline gate also passed with the lease held normally.

## Historical: Fixture timing events

Use a new file for each run; existing event files are rejected:

```sh
cargo run -p xtask --features postgres-check -- check-postgres \
  --fixture-metrics target/test-performance/run-01/fixtures.jsonl
```

The optional append-only event stream records server/run setup, the actual
template migration, current/blank history provisioning, normal database drain and
drop, demo provisioning/drop, and run/server cleanup. A terminal `gate_passed` or
`gate_failed` distinguishes successful cleanup from successful obligations. A
missing terminal event is incomplete evidence. Each row contains a phase, opaque
identity, mode, timestamp and duration; it never contains a URL. A per-write file
lock prevents child events interleaving. File creation refuses reuse, invalid
labels are rejected, and write errors fail the gate. Root setup-event failures
remain inside the resource cleanup boundary. No host environment is mutated.

These timings distinguish template migration, current clones and blank fixture
creation; a blank database count is not a count of calls to the migration API.
Migration/current-fast-path assertions still run in the original tests. A real
queue crash/restart history produced one matched current create/finish pair
across its attached children. A real predecessor queue migration produced one
matched blank pair. Both completed their cleanup and terminal success event.
Six support tests, including concurrent event writes, refused file reuse and
failed writes, and all 24 optional xtask tests passed without credentials. The
support/runner all-target Clippy check passed.



## Historical: Independent contract families

The scenario ledger in `test-performance-scenarios.json` maps every original
parameter tuple to its new named test family. Native pending-tool coverage keeps
100 tuples per backend: tool claim/recovery each own 24, and frontend
claim/recovery each own 26, including the mixed-family cases. Runtime pending-tool
coverage keeps 32 tuples per backend in four frontend/unkeyed families of eight.
The durable successor lifecycle keeps 44 tuples per backend in core (28),
retention (6), recovery (4) and pending (6) tests. Existing separate memory
successor tests remain unchanged.

The original same-task and runtime journey helper bodies are unchanged. Each
inner history still constructs its actual owner, gate, task and scoped IDs. Cold
restart, replay, held acknowledgements, corruptions and actual cleanup joins stay
inside their original histories. Independent families now receive fresh Stores
and clocks; no identity or receipt passes between them. The blocked cleanup
history retains its complete ready/release/child-join sequence. SQL cases still
perform their original migrations and use current-template fixtures only for
current-schema behavior. Production MemoryStore transaction cloning is untouched.

The WASM growth contract remains one 212-run causal history with its original
`>= 1000` state assertion and per-call instance mode. Its replay/configuration
check was already a separate test. Splitting the growth sequence would discard
the retained-state obligation, so it is deliberately kept intact.

All 20 affected Memory/SQLite cases and all 12 affected PostgreSQL cases passed
at one slot. Each backend then passed five complete repetitions under the
four-slot limit, with no individual retries. All 15 PostgreSQL containers from
those repetitions were independently verified absent afterward.

The quiet PostgreSQL comparison used the same source and fixed xtask binary at
one, two and four slots. Nextest execution across the three sequential family
commands took 596.999, 457.223 and 213.574 seconds respectively. Four slots was
selected; runtime contracts retain weight two. The observed connection peaks
were 5, 7 and 13. The first setting rebuilt artifacts, so its compilation CPU/RSS
must not be compared with the warm settings as test-only resource usage. See
`test-performance-resource-selection.json` for the measured scope and resources.
The whole-gate cold/warm qualification remains separate.

The required bounded-snapshot CLI checks use `--smoke`: eight fixture messages still cross the seven-message page limit, preserve the event high-water mark, settle the tool relation, and pass the continuation into the actual durable child entrypoint. The native `bounded_snapshots` facade tests retain the same source-included journey with 1024 messages and the unchanged allocation proof over 512KiB/32MiB histories on Memory, SQLite and PostgreSQL. The default example continues running its full demonstration.

The native pending-approval case is split into same-task, held-claim, held-recovery, malformed/replay and blocked-main families on every backend. These keep all original variant tables, effect/ownership assertions and deadlines. Each family gets a fresh Store and clock, as the independent pending-tool families already do. The earlier unoptimized PostgreSQL aggregate took 76.664 seconds; all fifteen approval cases then passed serially and at four slots, together with the five full bounded-snapshot entrypoints. Each run drained six PostgreSQL fixture pairs and its owned container.

For an owned PostgreSQL container on a local Linux Docker host, the runner prefers a reachable bridge address and avoids the published port TCP proxy. Within a 500 ms probe, both routes must report the same [`system_identifier`](https://www.postgresql.org/docs/14/functions-info.html) from PostgreSQL before any fixture administration. Non-Unix Docker daemon configurations and unreachable or mismatched bridges keep the existing published endpoint. `CRABBER_TEST_POSTGRES_TRANSPORT` selects `auto` (default), `bridge` (require a reachable local bridge), or `published` for a controlled comparison. The same owned container, PostgreSQL image, durability settings, database isolation and cleanup remain in use; logs record the transport label without an endpoint or URL.
