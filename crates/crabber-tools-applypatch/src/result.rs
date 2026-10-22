use crate::parser::Operation;
use crabber_tools_core::ToolError;
use serde::Serialize;
use serde_json::{Value, json};
#[derive(Serialize)]
pub(crate) struct FileResult {
    operation: &'static str,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    new_path: Option<String>,
    pub(crate) status: &'static str,
    additions: usize,
    deletions: usize,
}
impl FileResult {
    pub(crate) fn new(op: &Operation) -> Self {
        Self {
            operation: op.kind.name(),
            path: op.path.clone(),
            new_path: op.new_path.clone(),
            status: "preflighted",
            additions: op.additions,
            deletions: op.deletions,
        }
    }
}
pub(crate) fn failure(error: ToolError, files: Vec<FileResult>, partial: bool) -> Value {
    json!({"outcome":"failed","files":files,"partial":partial,"error":error})
}
