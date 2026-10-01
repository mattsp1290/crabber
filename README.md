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
across concurrent and terminal retries. Output includes the full source SHA,
store/schema, fake-provider identity, correlation IDs and measured counts. Memory
retains receipts only while that store lives; PostgreSQL retains them across restarts.
An admission receipt proves acceptance, not completion. A committed turn whose host
dies before spawning execution remains admitted; fenced recovery interrupts that
non-paused run without repeating its provider request.

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
for ordering, token durability, errors and schema-3 maintenance requirements.

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
