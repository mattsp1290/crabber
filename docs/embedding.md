# Embedding Crabber

The default facade has no provider credentials, database, WASM, observability or
AG-UI dependency. Begin with `Agent::builder().memory()` and a FakeProvider;
subscribe to live events, then await the run's durable completion. Enable only
capabilities your host uses.

| Capability | Code/guide | Runnable command / feature |
| --- | --- | --- |
| Memory + fake provider | [minimal embedding](../examples/minimal-embed/src/main.rs) | `cargo run -p minimal-embed` |
| PostgreSQL persistence | [session store](../crates/crabber-session/README.md) | `cargo run -p minimal-embed -- --store postgres`; set `CRABBER_POSTGRES_URL`, dedicated PostgreSQL 14+; facade `postgres` |
| Real providers | [provider usage](../crates/crabber-providers/README.md) | facade `anthropic`, `openai`, `codex`, `opencode-go`; manual service configuration |
| Fenced abandonment | [host protocol](fenced-abandon.md) | `cargo run -p fenced-abandon`; memory default, manual PostgreSQL mode |
| Native extensions | [native guide](../examples/native-extension/README.md) | `cargo run -p native-extension` |
| Workspace context for native extensions | [contract](#workspace-context), [external-consumer probe](../examples/workspace-context-probe/README.md) | `cargo test -p workspace-context-probe` |
| WASM extensions | [WASM guide](../examples/wasm-extension/README.md) | `cargo xtask build-fixtures`, then `cargo run -p wasm-extension`; facade `wasm` |
| Codex browser login | [login guide](../examples/codex-login/README.md) | `cargo run -p codex-login -- status`; manual local credential access |
| LLM observability | [Datadog guide](../examples/datadog-export/README.md) | `env -u DD_API_KEY cargo run -p datadog-export`; facade `datadog` |
| Operational distributions | [operational guide](../examples/operational-telemetry/README.md) | `cargo run -p operational-telemetry -- --check` |
| AG-UI SSE | [projection contract](ag-ui.md), [host guide](../examples/agui-sse/README.md) | `cargo run -p agui-sse -- --check`; separate opt-in `crabber-agui` crate |

[Admission receipts](admission-receipts.md) describe ambiguous-response retries
without starting duplicate executors. [Bounded snapshots](bounded-snapshots.md)
provide bounded all-history reads. [Host tracing](../examples/host-trace/README.md)
covers explicit context and composed observers; [operational observation](operational-observation.md)
describes finite run/model/tool reasons and monotonic durations. Use those
contracts when adopting their corresponding public APIs.

`cargo xtask check` builds local WASM fixtures, checks formatting/Clippy, runs
workspace tests, offline host journeys and the external public-API consumer,
and enforces minimal-embed's 60-line marked glue limit. It requires no credentials
or external accounts. Real PostgreSQL/provider setup, login/logout and live
Datadog ingestion are manual external-service steps. Do not run Codex credential
commands in CI against a user's real store. Never let inherited Datadog keys
turn an offline example check into an external write. Generated WASM stays local.

## Workspace context

Native extensions can read the workspace identity of the session a run belongs
to. The values are **routing data, never authorization**: use them to pick a
root or an owner record, and keep trust decisions, process policy and
credentials in the host.

### Public API

| Surface | API |
| --- | --- |
| Type | `crabber::extension::WorkspaceContext` with `workspace_id() -> Option<&str>` and `directory() -> Option<&str>` |
| Tool | `ToolContext::workspace() -> &WorkspaceContext`, read-only, available in `ToolExecutor::execute_with_context` |
| Dynamic prompt contribution | the `ContextAssemble` transform payload carries `workspace_id` and `workspace_directory` (`WorkspaceContext::WORKSPACE_ID_KEY`, `WorkspaceContext::DIRECTORY_KEY`); each is a JSON string, or `null` when unavailable |
| Constructor | `ToolContext::new` takes a required `WorkspaceContext` after `host`. Only code that builds a `ToolContext` itself is affected; `ToolExecutor::execute` and `execute_with_context` are unchanged |

`None` / `null` is the explicit unavailable value. An extension must handle it
and cannot read a default.

Values are exposed verbatim. A directory may be relative, in which case it
resolves against the working directory of whichever process runs the tool.
`AgentConfig::new` defaults to workspace ID `"default"` and directory `"."`;
those are persisted with the session and exposed as available values. Hosts
that route on workspace identity should set both explicitly.

### Source and guarantees

The persisted `Session` record (`workspace_id`, `directory`) is the only source,
on every path. The request saved in a run checkpoint and the request retained in
a keyed-admission capsule are never used to populate extension context; they
are only compared with the session.

- **Missing.** A field is missing only when the persisted session holds an
  empty string for it. The run does not fail; that field is unavailable. The
  runtime never substitutes `AgentConfig` values, the process working
  directory or model-supplied tool arguments. A `Running` run reclaimed after
  its lease expired has no checkpoint request, and that is not missing
  context: it sees exactly what an uninterrupted run sees.
- **Invalid.** A stored session that cannot be decoded, or compared values
  that differ, fail closed with a typed error before any executor or
  `ContextAssemble` handler runs. A difference is
  `StoreError::SessionIdentityMismatch`.
- **Comparison.** Exact string equality, per field. There is no lexical
  normalization, no symlink or filesystem resolution and no check that the
  directory exists; `/srv/a` and `/srv/a/` differ. An empty string is a value
  like any other, so empty against non-empty is a mismatch in either
  direction. Availability is decided only after every comparison passes.
- **Handlers cannot change the values.** The runtime re-asserts both keys
  after every `ContextAssemble` handler, so a handler that overwrites or
  removes them affects neither later handlers nor the tool context. A handler
  that returns anything other than a JSON object fails the turn with
  `ExtensionError::Rejected`.
- **Same turn, same values.** The tool context and the `ContextAssemble`
  payload of a run come from one read of the session. `ContextAssemble` runs
  once per turn, including turns after resume or recovery; contributions are
  re-invoked, not replayed.
- **Plan fingerprint.** Workspace values are not fingerprint inputs. The
  fingerprint covers registered component identities (and, for a static plan,
  its static prompt text); two sessions in different workspaces with the same
  components have the same fingerprint. Handler output is neither
  fingerprinted nor persisted, so output that varies with the workspace does
  not affect receipts or replay.

### Path matrix

| Path | Comparison | On conflict |
| --- | --- | --- |
| `prompt` creating a session | none; the request's values are persisted | n/a |
| `prompt` into an existing session | host-presented: `AgentConfig` workspace ID and directory against the session, inside the admission transaction | `SessionIdentityMismatch`; no run is created |
| `prompt_keyed` | host-presented, inside the admission transaction, before the receipt lookup | `SessionIdentityMismatch`; no run, no replay |
| `resume`, `resume_with_context` of a paused run | stored-record: the checkpoint request against the session, before plan acquisition, the claim and any pending tool | `SessionIdentityMismatch`; no plan, fence or lease is taken and the run keeps its status |
| `resume` of a reclaimed `Running` run with no checkpoint request | none | n/a |
| `recover`, `recover_with_context` | as `resume`, per run | the run is skipped, listed in the returned `RecoverReport`, and the sweep continues |
| `recover_admission` | host-presented, before the receipt lookup and before any replay | `SessionIdentityMismatch`; no claim, no replayed receipt |

A run rejected on `resume` stays unfinished and is rejected again on every
later attempt; it does not heal itself, and its session stays busy. `recover`
returns a `RecoverReport`: the runs it recovered, plus a `SkippedRun` with the
reason for each expired run it left unfinished: an identity mismatch, a session that is
missing or cannot be decoded, a checkpoint request without identity
(`Validation`), a live competing claim or an unstarted keyed admission. Dispose
of a skipped run through [fenced abandonment](fenced-abandon.md).

### Unsupported context

WASM guests do not receive workspace context. The WIT contract and the
guest-visible `turn-metadata` record are unchanged (its `workspace-id` field
stays the empty string), and no guest rebuild is needed. Exposing it to guests
is a separate change with its own WIT version decision.

### Schema, adoption and rollback

There is no schema change and no migration: sessions already store both fields,
and the schema version stays 5. Adopt by upgrading the crate and, only if you
construct `ToolContext` yourself, passing the new argument. Roll back by
reverting to the previous revision. Neither direction needs a data migration.

Upgrade hazard: earlier revisions let an unkeyed `prompt` into an existing
session present a different workspace ID or directory, and a run admitted that
way saved the different values in its pause checkpoint. After the upgrade such a
paused run is rejected on `resume`, skipped by `recover` and keeps its session
busy. Before upgrading, let paused runs finish or settle them; after upgrading,
find any in the `skipped` list of `recover` and `abandon` them once their lease
expires.

One behavior change comes with adoption: an unkeyed `prompt` into an existing
session now rejects a different workspace ID or directory, as keyed admission
already did. A host that reuses a session must present the identity it was
created with. Sessions created with an empty workspace ID or directory keep
working and expose the unavailable value.
