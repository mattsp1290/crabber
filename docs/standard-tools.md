# Standard tools

Workspace coding tools for Crabber, mirroring eino-tools: `file_read`, `file_write`,
`file_edit`, `file_list`, `search`, `shell`, `glob`, `apply_patch`, `url_fetch`,
`tracker_write`, `user_interact`, `multi_edit`, `file_delete`, and `file_move`. One catalog extension mounts a
host-chosen subset with bounded work, capability-rooted file access, and process
group cleanup on cancellation. Unix only; Rust 2024 and the containing workspace's stable toolchain.

The tools are workspace crates of this repository. Depend on `crabber`,
`crabber-tools-catalog`, and `crabber-extensions` from
`ssh://git@github.com/mattsp1290/crabber.git` at one immutable `rev`.
One revision carries the runtime, tools, and native extensions.

## Quick start

The application needs `crabber`, `crabber-tools-catalog`, `tokio`, and `tempfile`.
The host explicitly admits the workspace and chooses every process policy.

```rust
use std::{sync::Arc, time::Duration};
use crabber::{Agent, AgentConfig, FakeProvider, PermissionDecision, Selection,
              StaticPolicy, StreamDelta, extension::Scope};
use crabber_tools_catalog::prelude::*;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let directory = tempfile::tempdir()?;
let root = WorkspaceRoot::open(directory.path())?;
let tools = StandardTools::new(root.clone(), Policies {
    enabled: EnabledSet::only([ToolId::FileRead, ToolId::Search, ToolId::Shell]),
    restrict_to_enabled: false,
    shell: ShellPolicy {
        shell_binary: "/bin/sh".into(),
        startup_mode: StartupMode::NonLogin,
        env: EnvPolicy::minimal_allowlist(),
        run_as: None,
        output_cap_bytes: 65536,
    },
    search: SearchPolicy::resolve_rg_from_path(EnvPolicy::minimal_allowlist())?,
    url_fetch: None,
    tracker: None,
    user_interact: None,
    limits: Limits { max_in_flight: 4, max_blocking_wait: Duration::from_secs(2) },
})?;
let mut config = AgentConfig::new(Selection {
    provider_id: "fake".into(), model_id: "scripted".into(),
});
config.directory = root.path().to_string_lossy().into_owned();
let agent = Agent::builder().behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION"))).memory()
    .config(config)
    .provider(Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("ready".into()), StreamDelta::Completed,
    ]])))
    .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
    .shared_extension(Arc::new(tools), Scope::Global).build()?;
agent.prompt(None, "Ready?").await?.done().await?;
agent.close_extensions().await?;
# Ok(())
# }
```

Run the full credential-free example with `cargo run -p mount-standard --locked -- --check`.
It mounts standard tools beside a host extension and host tool, demonstrates a
missing allowlisted tool and a permission-denied shell call, prints tool events,
and checks persisted results.

## Crates

| Crate | Responsibility |
| --- | --- |
| crabber-tools-core | Envelopes, capabilities, bounds, locks, process policy and identities |
| crabber-tools-fileops | Reads, atomic writes, ordered anchored edits, listings, deletion and inode-preserving moves |
| crabber-tools-search | Bounded streaming ripgrep JSON search; requires rg >=14 |
| crabber-tools-shell | Explicit login/non-login shell policy, bounded output and timeouts |
| crabber-tools-glob | Sorted capability-confined path discovery |
| crabber-tools-applypatch | Preflighted multi-file patches with atomic per-file commits |
| crabber-tools-urlfetch | Policy-constrained HTTPS and workspace-confined file text |
| crabber-tools-trackerwrite | Explicit bn-backed tracker mutations with bounded lifecycle |
| crabber-tools-userinteract | Durable host questions or pending relay |
| crabber-tools-catalog | Deterministic metadata, allowlists, Extension and host prelude |

## Host responsibilities

Hosts choose the workspace root, permissions, environment, and uid. Fileops run
as the host uid; `run_as` applies only to spawned processes. Mount
crabber-extensions `command_guard` for command analysis and `tool_result_redactor`
for secret redaction. Shell execution is not a sandbox. `required_permissions`
is advisory; `StaticPolicy` matches tool names. Prefer `restrict_to_enabled: false`
when mounting other tools, because restrictions apply to the entire run.

Use one shared root per agent, or `StandardToolsFactory` with host-owned `WorkspaceAdmission` for multiple workspaces. Multiple
roots mounted globally collide on names. The persisted directory must match the
admitted absolute root; `AgentConfig`'s default `.` is rejected. Reopen the root
handle after deleting and recreating a directory. Existing hard links and detached processes remain host concerns;
`file_move` creates a transient second link during a move. Reads are unordered against in-flight writers.
Started atomic writes may complete after interruption. A started `file_move` may
also finish; a host crash between link and unlink can leave both names.

See [the parity map](standard-tools-parity.md) and [the tool contract](tool-contract.md) for exact schemas, envelopes,
limits, identity inputs, accepted search path races, and deliberate deviations.
Golden upstream fixtures and their allowlist live in [../fixtures/eino-tools](../fixtures/eino-tools).

## Verification

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked
cargo run -p mount-standard --locked -- --check
```

Tests use scripted providers, temporary workspaces, `sh`, and `rg` without
credentials or network. Linux CI runs the workspace gate. The standalone consumer exercises a custom
extension, a custom host tool, and a reopened SQLite store at one Git revision.

## History

The former [crabber-tools repository](https://github.com/mattsp1290/crabber-tools.git)
retains the source history: v0.1.0 supplied the initial tools, 0.2.0 established
eino-tools parity, 0.3.0 added `multi_edit`, `file_delete` and `file_move`, and 0.4.0
adopted the durable host APIs. Consolidation resets the crate version to the
containing workspace's 0.1.0. Configuration identity uses domain v2 and an
explicit `TOOLS_IDENTITY_REVISION`, independent of the crate version.

`EnabledSet::all()` includes all fourteen tools, including destructive `file_delete` and `file_move`.
Hosts wanting upstream-shaped tools use `EnabledSet::eino_parity()` or subtract
`ToolId::BEYOND_PARITY`. Enabling `UrlFetch` requires
`url_fetch: Some(UrlFetchPolicy::default())`; that policy denies private and
special-purpose destinations by default and has an empty host allowlist.
Enabling `TrackerWrite` requires `tracker: Some(TrackerPolicy { .. })` with an
absolute bn executable, explicit hub/project/actor, allowed workflow states and
replacement environment. The host supplies these values from its configuration;
there is no inherited tracker namespace. Keep the policy `None` when the tool is
disabled. Missing required policies fail construction instead of omitting tools.
Any supplied policy is validated, even for a disabled tool.

Enabling `UserInteract` requires an explicit `UserInteractPolicy::durable(authority)`
or `UserInteractPolicy::pending()`. Durable questions pause until the host records
an authenticated answer through Crabber's interaction API. The pending surface
relays questions and accepts a model-supplied `answer`; it does not establish human
consent. Neither surface consumes mount capacity. `EnabledSet::eino_parity()`
selects the eleven upstream tools and requires all three optional policies.

The example mounts a workspace subset with a durable question and a scripted host
answer loop. It needs no tracker installation, credentials or network. The all-tools
catalog test uses a synthetic bn backend and a confined file URL. Tracker writes
mutate the hub and are not retry-safe, but do not take the workspace writer lock.

For multiple workspaces, register `StandardToolsFactory` once with root-free
`Policies` and a host-owned `WorkspaceAdmission`. `ExactRoots` is an explicit
workspace-ID/directory table. Admit new sessions through `prompt_keyed_in` with an
`ExecutionContext`; `prompt_in` requires an existing session. The runnable
[`contexts` example](../examples/mount-standard/src/contexts.rs) exercises concurrent
roots and targeted close. A shared single-root `StandardTools` remains available.
Each workspace owns independent capacity; sessions in that workspace share it.

```rust
use crabber_tools_catalog::prelude::*;
use std::{path::PathBuf, sync::Arc};
# fn factory(policies: Policies, path: PathBuf) -> Result<StandardToolsFactory, crabber::ExtensionError> {
let admission = ExactRoots::new([("project-a".into(), path)])?;
let factory = StandardToolsFactory::new(policies, Arc::new(admission))?;
# Ok(factory)
# }
```

Every host must declare a stable `behavior_version`, changing it when configured
behavior changes. Version 0.4.0 removes catalog `Options` and `Surface::Prompter`,
uses the renamed `shared_tool`/`shared_extension` builders, and returns typed
`ToolOutput`: failure envelopes persist as failed calls. Shell nonzero exits
remain successful results. See [lifecycle and cleanup](standard-tools-lifecycle.md) for
resource ownership, interruption uncertainty, output bounds and restart limits.

The standalone consumer template lives in
[testdata/standard-tools-consumer](../testdata/standard-tools-consumer).
Its adoption check qualifies the published revision outside the workspace.
