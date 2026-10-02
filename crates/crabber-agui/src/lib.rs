#![doc = include_str!("../README.md")]
mod projector;
mod sse;
pub use ag_ui_core;
pub use projector::{
    Completion, ProjectionConfig, ProjectionError, Projector, public_tool_content,
};
pub use sse::encode_sse;
