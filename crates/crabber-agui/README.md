# crabber-agui

Opt-in live projection of Crabber source events into `ag-ui-core` 0.2.0 from
[mattsp1290/ag-ui](https://github.com/mattsp1290/ag-ui/tree/705303b26b6b900cdf9bdfa456fbd92a7003ca6b),
revision `705303b26b6b900cdf9bdfa456fbd92a7003ca6b`. Core/runtime and the default
Crabber facade do not depend on AG-UI or HTTP. The adapter re-exports its pinned
`ag_ui_core` and `ag_ui_encoder` so hosts share the pinned wire types and encoder.

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
without credentials. SSE encoding uses the SDK encoder and producer normalization, no replay ID and no
`[DONE]` marker. Reasoning display defaults off.

Hosts can use `Projector::push_with_delivery` to reserve and send an entire batch
atomically without cloning prior identities. A rejected delivery rolls back
presentation boundaries and permanently faults the stream. `frame_budget` performs
an allocation-free conservative precheck, including SSE framing, before the SDK
allocates a normalized event. The limit covers the whole frame. Queue accounting
uses the encoded frame's actual length; `sse_frame_len` is an allocating wrapper.
`encode_sse` uses live producer normalization; `encode_sse_recorded` preserves a
recorded `RUN_STARTED` version or its omission. Both reject invalid SDK events as
permanent `Malformed` faults. Hosts can use `ag_ui_encoder::Encoder::keepalive`
for bounded comment frames and `ag_ui_encoder::CONTENT_TYPE` for HTTP responses.
