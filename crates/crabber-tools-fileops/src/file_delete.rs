//! The file_delete tool definition.
use crate::{FileTool, Kind, Options};
use crabber::{ToolDefinition, core::ToolInfo};
use std::sync::Arc;
/// Deterministic model-facing schema and advisory safety metadata.
pub fn info() -> ToolInfo {
    ToolInfo { name: "file_delete".into(), description: "Delete one workspace-relative regular file. Directories, symlinks and missing files are rejected with structured errors (is_directory, validation, not_found). Not idempotent: deleting a missing file fails.".into(), parameters: serde_json::json!({"type": "object", "additionalProperties": false, "required": ["path"], "properties": {"path": {"type": "string", "minLength": 1, "description": "Workspace-relative file to delete."}}}), retry_safe: false, required_permissions: vec![crabber_tools_core::permission::FS_WRITE.into()] }
}
/// Build a context-required executor sharing the mount's capacity.
pub fn definition(options: Arc<Options>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(FileTool {
            options,
            kind: Kind::Delete,
        }),
    })
}

use crate::operations::{Request, io_error};
use crabber_tools_core::{ToolError, category};
use serde_json::{Value, json};
impl Request {
    pub(crate) fn require_regular_source(&self, options: &Options) -> Result<(), ToolError> {
        if self.path.is_root() {
            return Err(ToolError::new(
                category::IS_DIRECTORY,
                "path is a directory",
            ));
        }
        let metadata = options
            .root
            .dir()
            .symlink_metadata(self.path.as_path())
            .map_err(io_error)?;
        if metadata.is_dir() {
            return Err(ToolError::new(
                category::IS_DIRECTORY,
                "path is a directory",
            ));
        }
        if !metadata.is_file() {
            return Err(ToolError::new(category::VALIDATION, "not a regular file"));
        }
        Ok(())
    }
    pub(crate) fn delete(&self, options: &Options) -> Result<Value, ToolError> {
        self.require_regular_source(options)?;
        options
            .root
            .dir()
            .remove_file(self.path.as_path())
            .map_err(io_error)?;
        Ok(json!({"outcome":"succeeded"}))
    }
}
