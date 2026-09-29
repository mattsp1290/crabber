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
    let event = events.recv().await?;
    if event.kind == EventKind::TextDelta {
        print!("{}", event.payload["text"].as_str().unwrap_or_default());
    }
    if event.kind == EventKind::RunSettled { break; }
}
run.done().await?;
# Ok(())
# }
```

Run the full workspace quality gate with `cargo xtask check`. It checks formatting, Clippy, tests, and that the example's marked embedding glue stays within 60 lines.
