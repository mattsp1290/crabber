# crabber

Crabber is an embeddable Rust agent runtime. The minimal example uses an in-memory session store and a scripted provider, so it runs without credentials:

```sh
cargo run -p minimal-embed
```

An application starts with `Agent::builder()`, starts a prompt, receives live events, and waits for its durable result:

```rust,no_run
use crabber::{Agent, AgentConfig, EventKind, FakeProvider, Selection, StreamDelta};
use std::sync::Arc;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let provider = FakeProvider::scripted(vec![vec![
    StreamDelta::TextDelta("Hello!".into()),
    StreamDelta::Completed,
]]);
let agent = Agent::builder()
    .memory()
    .provider(Arc::new(provider))
    .config(AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    }))
    .build()?;
let mut run = agent.prompt(None, "Say hello").await?;
let mut events = run.events();
loop {
    let Some(event) = events.recv().await? else { break };
    if event.kind == EventKind::TextDelta {
        print!("{}", event.payload["text"].as_str().unwrap_or_default());
    }
}
run.done().await?;
# Ok(())
# }
```

## Host-authorized `AGENTS.md` middleware

Model middleware can append bounded system-prompt text. The first-party recipe has
no ambient filesystem access: the embedding host must validate the exact persisted
workspace context and return a rooted, bounded reader. A mount looks like this
(schematic; the [runnable example](examples/agents-md-middleware/src/main.rs)
contains the complete deny-by-default in-memory resolver):

```rust,ignore
use crabber::{Agent, Scope, WorkspaceReaderResolver};
use crabber_middleware::AgentsMdExtension;
use std::sync::Arc;

let resolver: Arc<dyn WorkspaceReaderResolver> = host_authorized_resolver();
let agent = Agent::builder()
    .memory()
    .provider(provider)
    .config(config) // persists the exact workspace ID/directory used for routing
    .workspace_reader_resolver(resolver)
    .extension(Arc::new(AgentsMdExtension::default()), Scope::Global)
    .build()?;
```

For an authorized `AGENTS.md`, the recipe appends an explicit
`## Workspace instructions: AGENTS.md` frame with a byte-count marker and closing
`## End workspace instructions: AGENTS.md` heading. See the [middleware capability,
framing, and security contract](docs/middleware.md), then run
`cargo run -p agents-md-middleware` without credentials.

`crabber-middleware` is currently distributed as a workspace/path dependency rather
than as a standalone crates.io package.

Run the full workspace quality gate with `cargo xtask check`. It checks formatting, Clippy, tests, and that the example's marked embedding glue stays within 60 lines.

## PostgreSQL persistence

Enable the `postgres` feature to use `PostgresStore`. PostgreSQL 14+ and a dedicated database are required; call `PostgresStore::migrate(url)` before connecting. The minimal embedding example enables this feature and accepts `--store postgres`, reading `CRABBER_POSTGRES_URL`. With `--interrupt` (also accepted as `--interrupt-after-first-delta`), it prints the run ID and leaves the interrupted run in PostgreSQL. A later process can run `--store postgres --resume <run-id>` with the same URL. The default `--store memory` remains process local.

Keyed admission lets a host reconcile an ambiguous response without creating a
second executor. Allocate and retain a `SessionId` before the first request, then
call `Agent::prompt_keyed(session, text, AdmissionOptions { key, fingerprint,
behavior_fingerprint })`. Only `Admission::Started` returns a handle;
`Admission::Replayed` returns the identical immutable receipt. Use
`Agent::lookup_admission(&session, &key)` for read-only reconciliation. See the
[store and fingerprint contract](crates/crabber-session/README.md#keyed-admission-memorystore-and-postgresql)
for retention, opaque behavior versioning, custom adapters and unknown outcomes.

Run the noninteractive, credential-free fake-provider journey:

```sh
cargo run -p admission-receipt -- --memory
# Set CRABBER_TEST_POSTGRES_URL to a disposable dedicated PostgreSQL 14+ database.
cargo run -p admission-receipt --features postgres -- --postgres
```

Both modes assert one provider execution, one user message and identical receipts
across concurrent and terminal retries after an injected lost admission reply. Output includes the full source SHA,
store/schema, fake-provider identity, correlation IDs and measured counts. Memory
retains receipts only while that store lives; PostgreSQL retains them across restarts.
An admission receipt proves acceptance, not completion. A committed turn whose host
dies before spawning execution can complete its original turn through
`Agent::recover_admission(session, original_text, original_options)` after the
original owner expires. Restore the original configuration and behavior version.
The store requires durable Unstarted evidence and a one-shot fenced begin before
execution effects; Started/legacy ambiguity remains conservative. PostgreSQL
schema 5 requires a coordinated stop/backup/migrate/deploy, with no mixed writers.

The [host algorithm and verification map](docs/admission-receipts.md) explain
unknown outcomes, retention, custom stores and process fault/restart assertions.

## Bounded embedding history

Use `Store::snapshot(SnapshotRequest)` to page a large session with explicit message,
tool-call, part, UTF-8 text and compact-record JSON byte caps. Keep its immutable
high-water cursor through every page, then consume events strictly after that cursor
once the snapshot is complete. `Limited` supports same-position retry;
`Invalidated` requires discarding the pages and restarting.

```sh
cargo run -p bounded-snapshot -- --memory
# Set CRABBER_TEST_POSTGRES_URL to a disposable dedicated PostgreSQL 14+ database.
cargo run -p bounded-snapshot --features postgres -- --postgres
```

Run from a clean, committed checkout. Both fake-data modes verify bounded allocation,
settled tool relations and concurrent continuation; PostgreSQL also resumes in a fresh
process. See the [host algorithm, limits, custom Store and verification map](docs/bounded-snapshots.md)
for ordering, token durability, errors and schema-4 maintenance requirements.

The [host tracing embedding](examples/host-trace/README.md) demonstrates explicit
validated 64/128-bit context, composed callbacks and broadcasts, real Datadog
payload capture, durable PostgreSQL workers and native recovery links. It
includes request-to-assertion evidence, Observer/Store adoption guidance and a
host-owned APM live verification procedure. Automated runs need no credentials;
current live linked-product evidence is UNVERIFIED.

[Operational observation adoption](docs/operational-observation.md) describes safe
finite terminal reasons, run/model/tool and first-token measurements, monotonic
clock injection, and the host-local limits of lease-loss reporting.

Run the complete operational host check with
`cargo run -p operational-telemetry -- --check`; see
[the example](examples/operational-telemetry/README.md) for offline fixtures and adoption.

Hosts can settle stopped or expired work through `Agent::abandon` without running
persisted providers, tools or extensions. See the [Memory/PostgreSQL abandonment and
administrative stop contract](crates/crabber-session/README.md#fenced-abandonment-memorystore)
for ownership checks, the host's process-stop duty and retry semantics.

The [fenced abandonment host protocol and acceptance map](docs/fenced-abandon.md)
cover unknown-response retry, authoritative worker stop, retained session/inbox
behavior and crash proofs. Run `cargo run --quiet -p fenced-abandon` for Memory;
with `CRABBER_POSTGRES_URL` configured, run
`cargo run --quiet -p fenced-abandon --features postgres -- --postgres` for durable
fresh-process readback. Both print matching compilation/runtime Git SHAs, durable
run/event IDs and measured zero provider/tool/mount counters.

## Adoption examples

The [embedding capability matrix](docs/embedding.md) links runnable memory,
PostgreSQL, provider, extension, authentication and telemetry paths. Guides for
[native extensions](examples/native-extension/README.md),
[WASM extensions](examples/wasm-extension/README.md),
[manual Codex login](examples/codex-login/README.md), and
[Datadog export](examples/datadog-export/README.md) explain their actual commands
and credential requirements.

Tool result transforms receive read-only call context and return
`TransformOutput`. Bind on the exact tool name and normalized input, and
register any protective redactor in the final phase:

```rust,no_run
use crabber::extension::{Registrar, ToolInput, TransformOutput};
use serde_json::json;
use std::sync::Arc;

fn register_redactor(registrar: &mut Registrar) {
    registrar.on_final_redaction(0, "shell-redactor", Arc::new(|context, mut result| {
        Box::pin(async move {
            if context.tool_name() == "shell"
                && context.resolved()
                && matches!(context.input(), ToolInput::Normalized(input)
                    if input["command"] == "echo hello")
            {
                result["private"] = json!("[REDACTED]");
            }
            Ok(TransformOutput::new(result))
        })
    }));
}
```

Use `on_result_transform` for ordinary reducers. Final redactors run after
all ordinary handlers; a false `mark_error` never clears an existing error.
After cancellation, only an accepted value protected by all remaining final
redactors can be persisted, still as `Interrupted`; otherwise the result is
fixed text `interrupted`. See the [public API, five paths and migration
contract](docs/embedding.md#tool-result-transform-context) and the runnable
[native example](examples/native-extension/README.md).

Adoption is a clean contract break: update JSON callbacks to preserve the full
envelope, update host `ToolPipeline` signatures and rebuild WASM guests.
Schema 5 stays unchanged, but old frozen plans fail with `PlanChanged`.
Finish or settle unfinished runs before upgrading or rolling back; rollback
pins the previous host revision and compatible guests. Await
`Agent::close_extensions()` on a Tokio runtime after interrupting runs as
needed. Its default 5s bound covers lease and cleanup draining; rollback and
extension shutdown remain unbounded. The guide describes timeout observation,
the detached reaper and human publication gates.

For live HTTP streams, add the opt-in [crabber-agui adapter](crates/crabber-agui/README.md)
and follow the [AG-UI contract](docs/ag-ui.md). Run
`cargo run -p agui-sse -- --check` for the [credential-free SSE example](examples/agui-sse/README.md),
including pinned Rust-client ASCII interoperability and independently decoded
fragmented Unicode. The default facade remains independent of AG-UI/HTTP.
