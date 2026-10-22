//! The file_write tool definition.
use crate::{FileTool, Kind, Options};
use crabber::{ToolDefinition, core::ToolInfo};
use std::sync::Arc;
/// Deterministic model-facing schema and advisory safety metadata.
pub fn info() -> ToolInfo {
    ToolInfo {name: "file_write".into(), description: "Write (create or overwrite) a workspace-relative file. Content is capped at 256 KiB. Optional create_dirs=true mkdir -p's the parent chain. Returns a structured error envelope on path_escape, not_found (missing parent), too_large, or io.".into(), parameters: serde_json::json!({"properties": {"path": {"minLength": 1, "description": "Workspace-relative target file.", "type": "string"}, "content": {"description": "File content. May be empty (truncates the target to 0 bytes).", "type": "string"}, "create_dirs": {"description": "If true, mkdir -p the parent chain. Default false.", "type": "boolean"}}, "additionalProperties": false, "required": ["path", "content"], "type": "object"}), retry_safe: false, required_permissions: vec![crabber_tools_core::permission::FS_WRITE.into()]}
}
/// Build a context-required executor sharing the mount's capacity.
pub fn definition(options: Arc<Options>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(FileTool {
            options,
            kind: Kind::Write,
        }),
    })
}

use crate::{
    atomic,
    operations::{Request, WriteArgs, io_error},
};
use crabber_tools_core::{ToolError, category};
use serde_json::{Value, json};

impl Request {
    pub(crate) fn write(&self, options: &Options, a: &WriteArgs) -> Result<Value, ToolError> {
        if self.path.is_root() {
            return Err(ToolError::new(
                category::IS_DIRECTORY,
                "path is a directory",
            ));
        }
        if a.create_dirs
            && let Some(parent) = self
                .path
                .as_path()
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
        {
            options
                .root
                .dir()
                .create_dir_all(parent)
                .map_err(io_error)?;
        }
        if let Some(hook) = &options.pre_write {
            hook();
        }
        let created = atomic::write(
            options.root.dir(),
            self.path.as_path(),
            a.content.as_bytes(),
        )
        .map_err(io_error)?;
        Ok(json!({"outcome":"succeeded","bytes_written":a.content.len(),"created":created}))
    }
}
