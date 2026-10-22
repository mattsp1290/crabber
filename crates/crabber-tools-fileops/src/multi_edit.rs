//! The `multi_edit` tool definition.
use crate::{FileTool, Kind, Options};
use crabber::{ToolDefinition, core::ToolInfo};
use std::sync::Arc;
/// Deterministic model-facing schema and advisory safety metadata.
#[must_use]
pub fn info() -> ToolInfo {
    ToolInfo { name: "multi_edit".into(), description: "Apply several anchored substring replacements to one workspace-relative file in a single atomic write. Edits apply in order against the running result; each anchor must occur exactly once at its turn. If any edit fails, nothing is written and the failing edit index is reported.".into(), parameters: serde_json::json!({"type": "object", "additionalProperties": false, "required": ["path", "edits"], "properties": {"path": {"type": "string", "minLength": 1, "description": "Workspace-relative file to edit. Must exist."}, "edits": {"type": "array", "minItems": 1, "maxItems": 64, "items": {"type": "object", "additionalProperties": false, "required": ["anchor", "replacement"], "properties": {"anchor": {"type": "string", "minLength": 1, "description": "Literal substring to find. Must match exactly once when this edit runs. No regex."}, "replacement": {"type": "string", "description": "Substitute for the anchor. May be empty."}}}, "description": "Ordered edits. Later edits see earlier replacements."}}}), retry_safe: false, required_permissions: vec![crabber_tools_core::permission::FS_WRITE.into()] }
}
/// Build a context-required executor sharing the mount's capacity.
#[must_use]
pub fn definition(options: Arc<Options>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(FileTool {
            options,
            kind: Kind::MultiEdit,
        }),
    })
}

use crate::{
    MAX_OUTPUT_BYTES, atomic,
    operations::{MultiEditArgs, Request, io_error},
};
use crabber_tools_core::{ToolError, category, failed};
use serde_json::{Value, json};
use std::io::Read;
impl Request {
    pub(crate) fn multi_edit(
        &self,
        options: &Options,
        a: &MultiEditArgs,
    ) -> Result<Value, ToolError> {
        let file = self.open(options)?;
        let mut bytes = Vec::new();
        file.take((MAX_OUTPUT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        if bytes.len() > MAX_OUTPUT_BYTES {
            return Err(ToolError::new(category::TOO_LARGE, "file exceeds 256 KiB"));
        }
        if bytes.contains(&0) {
            return Err(ToolError::new(category::BINARY, "not UTF-8 text"));
        }
        let mut text = String::from_utf8(bytes)
            .map_err(|_| ToolError::new(category::BINARY, "not UTF-8 text"))?;
        for (index, edit) in a.edits.iter().enumerate() {
            let count = text.matches(&edit.anchor).count();
            if count != 1 {
                let mut out = failed(
                    if count == 0 {
                        category::ANCHOR_NOT_FOUND
                    } else {
                        category::ANCHOR_AMBIGUOUS
                    },
                    format!("edit {index} anchor must occur exactly once"),
                );
                out["edit_index"] = json!(index);
                out["anchor_occurrences"] = json!(count);
                return Ok(out);
            }
            text = text.replacen(&edit.anchor, &edit.replacement, 1);
            if text.len() > MAX_OUTPUT_BYTES {
                return Err(ToolError::new(
                    category::TOO_LARGE,
                    "edited file exceeds 256 KiB",
                ));
            }
        }
        if let Some(hook) = &options.pre_write {
            hook();
        }
        atomic::write(options.root.dir(), self.path.as_path(), text.as_bytes())
            .map_err(io_error)?;
        Ok(json!({"outcome":"succeeded","bytes_written":text.len(),"edits_applied":a.edits.len()}))
    }
}
