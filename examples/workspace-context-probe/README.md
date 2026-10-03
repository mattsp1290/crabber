# Workspace context probe

```sh
cargo test -p workspace-context-probe
```

An external-consumer probe for native workspace context. It depends on the
`crabber` facade crate only: no private fields, no test-only features and no
session-to-workspace table of its own. Everything it knows about a session's
workspace comes from `ToolContext::workspace()` in its tool and from the
`workspace_id` / `workspace_directory` keys of the `ContextAssemble` payload in
its prompt contributor. It needs no credentials: the provider is the scripted
fake.

The in-memory tests in [tests/memory.rs](tests/memory.rs) prove that:

- two native tools in two sessions with different workspace IDs and directories
  each observe their own values, and a later run in the same session observes
  the same values;
- the `ContextAssemble` handler observes the same values as the tool;
- admitting into an existing session with a different directory or workspace ID
  is rejected with `SessionIdentityMismatch` and the tool is not invoked;
- a session persisted with an empty workspace ID or directory exposes the
  explicit unavailable value (`None`), even when the resuming host has other
  defaults and the model's tool arguments name another workspace.

The fresh-process tests in [tests/postgres.rs](tests/postgres.rs) need a durable
store. Set `CRABBER_TEST_POSTGRES_URL` to a disposable PostgreSQL 14+ database:

```sh
cargo test -p workspace-context-probe --features postgres -- --nocapture
```

They re-execute the test binary as a child process to prove that a paused
pending execution resumed by a freshly started host observes the persisted
workspace, and that a fresh host presenting a replacement root is rejected.
Without the variable they skip; with `CRABBER_REQUIRE_POSTGRES=1` its absence
is a failure.

WASM guests do not receive workspace context. See
[the embedding guide](../../docs/embedding.md#workspace-context).
