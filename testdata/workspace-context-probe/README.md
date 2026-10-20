# Standalone workspace-context probe

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

From `testdata/workspace-context-probe/` in the clean maintenance checkout:

```sh
cargo test --locked
```

For actual fresh-process PostgreSQL persistence coverage, set
`CRABBER_TEST_POSTGRES_URL` to a disposable PostgreSQL 14+ database and run:

```sh
CRABBER_REQUIRE_POSTGRES=1 cargo test --locked --features postgres -- --nocapture
```

The default run needs no database or task-specific environment. The PostgreSQL
tests otherwise skip when the URL is absent; REQUIRE makes that absence fail.
A default-feature success therefore does not establish PostgreSQL acceptance.

## Runtime assertions

- `two_sessions_observe_their_own_workspace_across_runs`: both named tools see
  each session's persisted identity on two runs and the prompt sees it too.
- `drifted_identity_is_rejected_and_the_tool_is_not_invoked`: admission fails
  closed on either changed identity field.
- `empty_identity_is_unavailable_not_a_host_default_or_model_argument`: paused
  execution exposes explicit absence despite other host defaults and arguments.
- `one_empty_field_is_unavailable_while_the_other_is_exposed`: absence is per field.
- `permission_denial_never_invokes_the_executor`: denied execution records no tool entry.
- `replacement_fence_never_authorizes_pending_approval`: a real replacement
  fence remains stored while ordinary resume refuses the pending approval; the
  executor is not invoked and no tool settlement is fabricated.
- `cancellation_is_available_to_a_running_tool`: a tool-owned observer sees
  cancellation after interrupt, with the persisted identity and a live entry token.
- `fresh_host_resumes_paused_run_with_the_persisted_workspace`: separate OS
  processes resume with persisted values and reject admission to a replacement root.

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

The [in-workspace probe](../../examples/workspace-context-probe/README.md) guards
current source in CI. Its Rust source is intentionally copied here, differing
only in the imported probe crate name; keep the behavior checks in both copies.
WASM guests do not receive workspace context. See the
[embedding guide](../../docs/embedding.md#workspace-context).
