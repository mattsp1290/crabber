use super::*;
use crabber::{
    core::ToolInfo,
    extension::{Registrar, ToolDefinition},
    runtime::ExecutionMode,
};

fn id(value: &Value) -> &str {
    value["id"].as_str().unwrap()
}

#[path = "lifecycle/process.rs"]
mod process;

#[path = "lifecycle/runtime.rs"]
mod runtime;

#[path = "lifecycle/close.rs"]
mod close;
