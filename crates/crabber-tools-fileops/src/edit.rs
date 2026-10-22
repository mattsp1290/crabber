//! The file_edit tool definition.
use crate::{FileTool, Kind, Options};
use crabber::{ToolDefinition, core::ToolInfo};
use std::sync::Arc;
/// Deterministic model-facing schema and advisory safety metadata.
pub fn info() -> ToolInfo {
    ToolInfo {name: "file_edit".into(), description: "Edit a workspace-relative file in place by anchored substring replacement. The anchor MUST appear exactly once in the file. Returns structured error envelopes on path_escape, not_found, is_directory, anchor_not_found, anchor_ambiguous, too_large, or io.".into(), parameters: serde_json::json!({"properties": {"path": {"minLength": 1, "description": "Workspace-relative file to edit. Must exist.", "type": "string"}, "anchor": {"minLength": 1, "description": "Literal substring to find. Must match exactly once. No regex.", "type": "string"}, "replacement": {"description": "Substitute for the anchor. May be empty.", "type": "string"}}, "additionalProperties": false, "required": ["path", "anchor", "replacement"], "type": "object"}), retry_safe: false, required_permissions: vec![crabber_tools_core::permission::FS_WRITE.into()]}
}
/// Build a context-required executor sharing the mount's capacity.
pub fn definition(options: Arc<Options>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(FileTool {
            options,
            kind: Kind::Edit,
        }),
    })
}

use crate::{
    MAX_OUTPUT_BYTES, atomic,
    operations::{EditArgs, Request, io_error},
};
use crabber_tools_core::{ToolError, category, failed};
use serde_json::{Value, json};
use std::io::Read;

impl Request {
    pub(crate) fn edit(&self, options: &Options, a: &EditArgs) -> Result<Value, ToolError> {
        let f = self.open(options)?;
        let mut bytes = Vec::new();
        f.take((MAX_OUTPUT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        if bytes.len() > MAX_OUTPUT_BYTES {
            return Err(ToolError::new(category::TOO_LARGE, "file exceeds 256 KiB"));
        }
        if bytes.contains(&0) {
            return Err(ToolError::new(category::BINARY, "not UTF-8 text"));
        }
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| ToolError::new(category::BINARY, "not UTF-8 text"))?;
        let count = text.matches(&a.anchor).count();
        if count != 1 {
            let mut out = failed(
                if count == 0 {
                    category::ANCHOR_NOT_FOUND
                } else {
                    category::ANCHOR_AMBIGUOUS
                },
                "anchor must occur exactly once",
            );
            out["anchor_occurrences"] = json!(count);
            return Ok(out);
        }
        let result = text.replacen(&a.anchor, &a.replacement, 1);
        if result.len() > MAX_OUTPUT_BYTES {
            return Err(ToolError::new(
                category::TOO_LARGE,
                "edited file exceeds 256 KiB",
            ));
        }
        if let Some(hook) = &options.pre_write {
            hook();
        }
        atomic::write(options.root.dir(), self.path.as_path(), result.as_bytes())
            .map_err(io_error)?;
        Ok(json!({"outcome":"succeeded","bytes_written":result.len(),"anchor_occurrences":count}))
    }
}
