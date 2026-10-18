#![doc = include_str!("../README.md")]
mod input;
mod projector;
mod sse;
pub use ag_ui_core;
pub use ag_ui_encoder;
pub use input::{NormalizedRunInput, WireInputError, convert_run_input};
pub use projector::{
    Completion, ProjectionConfig, ProjectionError, Projector, public_tool_content,
};
pub use sse::{encode_sse, encode_sse_recorded, frame_budget, sse_frame_len};
