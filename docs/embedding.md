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
| Native extensions | [native guide](../examples/native-extension/README.md) | `cargo run -p native-extension` |
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
