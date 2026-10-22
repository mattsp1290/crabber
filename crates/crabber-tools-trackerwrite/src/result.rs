use crabber_tools_core::ToolError;
use serde_json::{Value, json};
pub(crate) fn failure(op: &str, id: &str, error: ToolError) -> Value {
    let op = if op.len() <= 64 { op } else { "" };
    let id = if id.len() <= 256 { id } else { "" };
    json!({"outcome":"failed","op":op,"id":id,"error":{"category":error.category,"message":error.message,"op":op}})
}
