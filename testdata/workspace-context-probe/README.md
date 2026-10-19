# Standalone workspace-context probe

## Current acceptance status

The tracked Git revision and lockfile are historical and are **not current-API
acceptance**. The updated probe source uses APIs missing at that revision,
including the current behavior declaration / JSON executor interfaces. Merely
fetching the existing pin or finding it on `origin/main` does not qualify this
source. The direct probe commands below are acceptance instructions for AFTER
an authorized re-pin to the actual published forty-character candidate SHA;
they are not automated tests of the current historical pin.

From the Crabber repository root, `cargo xtask check-probes` provides separate
local static API coverage. It copies the actual probe source, tests and manifest
to disposable outside-repository workspaces and changes only the disposable
`crabber` dependency to the local checkout path. It seeds resolution from the
workspace `Cargo.lock`; additional registry dependencies may still resolve.
It checks all targets with default features (PostgreSQL cfg is off), without
running the probes or fetching their tracked Crabber Git dependency. With all
required dependencies cached, the check can run with `CARGO_NET_OFFLINE=true`;
a cold CI may access the registry without provider credentials. Tracked manifests,
Git pins and lockfiles remain unchanged by this helper.

That type-check is not runtime, Git-source, clean-consumer or PostgreSQL
acceptance. After publication and authorized re-pinning, run the Git-only probe
commands from a separate clean checkout, without a path override, and record the
actual resolved dependency SHA, commands and results. PostgreSQL runtime proof
still requires its real service prerequisites. No historical-pin runtime success
is claimed for the current source.

This Rust 2024 crate has its own workspace and committed lockfile. It consumes
only the public `crabber` facade, pinned to
`4afc30aa8e3ca96f5e0b6307d490b4e495efe308` (historical, incompatible with the
updated source; see status above). Existing authorized Git SSH access
fetches the private source repository; no provider or runtime credentials are
needed. The memory command needs no database or task-specific environment.

After the final published re-pin, run from `testdata/workspace-context-probe/`:

```sh
cargo test
```

For real fresh-process persistence checks, set `CRABBER_TEST_POSTGRES_URL` to a
disposable PostgreSQL 14+ database and `CRABBER_REQUIRE_POSTGRES=1`, then run:

```sh
cargo test --features postgres -- --nocapture
```

The intended runtime assertions after re-pinning are:

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

Direct Git-source/runtime runs remain separate publication evidence and require
Git access. The local `check-probes` static step is included in `cargo xtask check`;
it does not run these commands or qualify their historical pin. The
[in-workspace probe](../../examples/workspace-context-probe/README.md) guards
current source in CI. Their Rust sources are intentionally copied, differing
only in the imported probe crate name. Keep shared proofs in both copies and
qualify those APIs against the final published standalone pin before acceptance.

WASM guests do not receive workspace context. See the
[embedding guide](../../docs/embedding.md#workspace-context).
