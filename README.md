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
[store and fingerprint contract](crates/crabber-session/README.md#keyed-admission-memorystore)
for retention, opaque behavior versioning, custom adapters and unknown outcomes.

Run the credential-free concurrent/terminal replay journey with
`cargo run -p admission-receipt`. It asserts one provider execution, one user
message and identical receipts and prints only correlation metadata and source SHA.
This slice supports keyed admission in `MemoryStore`; PostgreSQL fails explicitly
with `AdmissionUnsupported` until its durable receipt implementation lands.
