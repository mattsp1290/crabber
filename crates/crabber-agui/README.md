# crabber-agui

Opt-in live projection of Crabber source events into `ag-ui-core` 0.2.0 from
[mattsp1290/ag-ui](https://github.com/mattsp1290/ag-ui/tree/5cc34b11fb41c6f068c27410b5756dbebcb367dd),
revision `5cc34b11fb41c6f068c27410b5756dbebcb367dd`. Core/runtime and the default
Crabber facade do not depend on AG-UI or HTTP. The adapter re-exports its pinned
`ag_ui_core` so hosts share one wire type identity.

```rust,no_run
use crabber_agui::{Completion, ProjectionConfig, Projector, encode_sse};
use crabber_core::{RunId, SessionId};
# fn example() -> Result<(), crabber_agui::ProjectionError> {
let config = ProjectionConfig::default();
let mut projector = Projector::new(
    SessionId::from("stored-session"), RunId::from("actual-runtime-run"),
    "client-thread".into(), "client-run".into(), config.clone(),
)?;
// For every record from RunEvents::recv: projector.push(&record).
// After drainage AND authoritative RunHandle::done:
for event in projector.finish(Completion::Failed)? {
    let frame = encode_sse(&event, config.max_event_bytes)?;
    // Write the UTF-8 frame to the host transport.
    assert!(frame.starts_with(b"data: "));
}
# Ok(())
# }
```

A push error permanently faults the stream. Call `fail(ProjectionError::Lagged)`
when the receiver loses events; `finish` then emits one safe error instead of
success. Completion must classify the actual task result, including lease loss.
Repeated finish is empty; pushes after finish are rejected.

See the [full mapping and host contract](../../docs/ag-ui.md), the
[HTTP example](../../examples/agui-sse/README.md), and integration tests in
[tests/projection.rs](tests/projection.rs). `cargo test -p crabber-agui` runs
without credentials. SSE encoding uses compact SDK JSON, no replay ID and no
`[DONE]` marker. Reasoning display defaults off.

Hosts can use `Projector::push_with_delivery` to reserve and send an entire batch
atomically without cloning prior identities. A rejected delivery rolls back
presentation boundaries and permanently faults the stream. `sse_frame_len`
counts a frame before allocation so host data and control queues can share an
explicit byte budget.
