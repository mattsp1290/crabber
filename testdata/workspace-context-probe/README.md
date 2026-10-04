# Standalone workspace-context probe

This Rust 2024 crate has its own workspace and committed lockfile. It consumes
only the public `crabber` facade, provisionally pinned to
`74e546f8c6427ba062991f4f00c68a440e4155d4`. Existing authorized Git SSH access
fetches the private source repository; no provider or runtime credentials are
needed. The memory command needs no database or task-specific environment.

Run from `testdata/workspace-context-probe/`:

```sh
cargo test
```

For real fresh-process persistence checks, set `CRABBER_TEST_POSTGRES_URL` to a
disposable PostgreSQL 14+ database and `CRABBER_REQUIRE_POSTGRES=1`, then run:

```sh
cargo test --features postgres -- --nocapture
```

The proofs are:

- `two_sessions_observe_their_own_workspace_across_runs`: both named tools see
  each session's persisted identity on two runs and the prompt sees it too.
- `drifted_identity_is_rejected_and_the_tool_is_not_invoked`: admission fails
  closed on either changed identity field.
- `empty_identity_is_unavailable_not_a_host_default_or_model_argument`: paused
  execution exposes explicit absence despite other host defaults and arguments.
- `one_empty_field_is_unavailable_while_the_other_is_exposed`: absence is per field.
- `permission_denial_never_invokes_the_executor`: denied execution records no tool entry.
- `stale_fence_never_invokes_the_executor`: reclaim during approval stops execution.
- `cancellation_is_available_to_a_running_tool`: a tool-owned observer sees
  cancellation after interrupt, with the persisted identity and a live entry token.
- `fresh_host_resumes_paused_run_with_the_persisted_workspace`: separate OS
  processes resume with persisted values and reject admission to a replacement root.

Run this probe from `origin/main` at or after this plan's merge commit, never
from the content commit itself, whose manifest still carries the provisional pin.
Use a merge commit to preserve the eventual content pin's reachability.
For clean-clone verification:

```sh
git clone git@github.com:mattsp1290/crabber.git /path/to/unique-clean-clone
git -C /path/to/unique-clean-clone checkout --detach origin/main
cd /path/to/unique-clean-clone/testdata/workspace-context-probe
cargo test
cargo test --features postgres -- --nocapture
```

Record the clone revision, resolved dependency SHA, commands and results. The
PostgreSQL variant uses the variables named above. Before publication, an exact
candidate branch revision may be checked out instead of `origin/main`.

This crate is excluded from `cargo xtask check`; fetching its private source
requires Git access, so its runs are local publication evidence. The
[in-workspace probe](../../examples/workspace-context-probe/README.md) guards
current source in CI. Their Rust sources are intentionally copied, differing
only in the imported probe crate name. Keep shared proofs in both copies and
use only APIs present at the standalone pin.

WASM guests do not receive workspace context. See the
[embedding guide](../../docs/embedding.md#workspace-context).
