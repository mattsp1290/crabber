//! The file_read tool definition.
use crate::{FileTool, Kind, Options};
use crabber::{ToolDefinition, core::ToolInfo};
use std::sync::Arc;
/// Deterministic model-facing schema and advisory safety metadata.
pub fn info() -> ToolInfo {
    ToolInfo {name: "file_read".into(), description: "Read a workspace-relative UTF-8 text file. Plain {path} calls return the leading content prefix capped at 256 KiB. Supplying offset and/or limit returns a line-window with raw and numbered content. Returns structured errors including path_escape, not_found, is_directory, binary, validation, and io.".into(), parameters: serde_json::json!({"properties": {"path": {"minLength": 1, "description": "Workspace-relative path of the file to read.", "type": "string"}, "offset": {"minimum": 1, "description": "Optional 1-based starting line for a line-windowed read. When omitted, legacy prefix reads are preserved unless limit is present.", "type": "integer"}, "limit": {"maximum": 5000, "minimum": 1, "description": "Optional number of lines for a line-windowed read. Default 2000 when offset or limit is present; cap is 5000.", "type": "integer"}}, "additionalProperties": false, "required": ["path"], "type": "object"}), retry_safe: true, required_permissions: vec![crabber_tools_core::permission::FS_READ.into()]}
}
/// Build a context-required executor sharing the mount's capacity.
pub fn definition(options: Arc<Options>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(FileTool {
            options,
            kind: Kind::Read,
        }),
    })
}

use crate::operations::{ReadArgs, Request, io_error};
use crate::{DEFAULT_READ_WINDOW_LINES, MAX_OUTPUT_BYTES, text};
use crabber_tools_core::{ToolError, category};
use serde_json::{Value, json};
use std::io::{self, BufReader, Read};
use tokio_util::sync::CancellationToken;

impl Request {
    pub(crate) fn read(
        &self,
        options: &Options,
        a: &ReadArgs,
        cancel: &CancellationToken,
    ) -> Result<Value, ToolError> {
        let f = self.open(options)?;
        if a.offset.is_none() && a.limit.is_none() {
            let mut bytes = Vec::new();
            f.take((MAX_OUTPUT_BYTES + 1) as u64)
                .read_to_end(&mut bytes)
                .map_err(io_error)?;
            let truncated = bytes.len() > MAX_OUTPUT_BYTES;
            bytes.truncate(MAX_OUTPUT_BYTES);
            if bytes.iter().take(8192).any(|b| *b == 0) {
                return Err(ToolError::new(category::BINARY, "not UTF-8 text"));
            }
            let content = text::utf8_prefix(&bytes, truncated)
                .map_err(|_| ToolError::new(category::BINARY, "not UTF-8 text"))?;
            let mut out = json!({"outcome":"succeeded"});
            if !content.is_empty() {
                out["content"] = json!(content);
                out["content_bytes"] = json!(content.len());
            }
            if truncated {
                out["truncated"] = json!(true);
                out["truncation_reason"] = json!("prefix");
            }
            return Ok(out);
        }
        let offset = a.offset.unwrap_or(1);
        let limit = a.limit.unwrap_or(DEFAULT_READ_WINDOW_LINES);
        let mut reader = BufReader::new(f);
        let (mut total, mut end) = (0usize, 0usize);
        let (mut content, mut numbered) = (String::new(), String::new());
        let (mut line_truncated, mut byte_cap) = (false, false);
        let mut reason = "";
        loop {
            if cancel.is_cancelled() {
                return Err(ToolError::new(category::UNKNOWN, "cancelled"));
            }
            let line = text::line(&mut reader).map_err(|e| {
                if e.kind() == io::ErrorKind::InvalidData {
                    ToolError::new(category::BINARY, "not UTF-8 text")
                } else {
                    io_error(e)
                }
            })?;
            let Some((line, cut)) = line else {
                break;
            };
            total += 1;
            if total >= offset && total - offset < limit && !byte_cap {
                if content.len() + line.len() > MAX_OUTPUT_BYTES {
                    byte_cap = true;
                    reason = "bytes";
                } else {
                    if cut {
                        line_truncated = true;
                        if reason.is_empty() {
                            reason = "line";
                        }
                    }
                    content.push_str(&line);
                    numbered.push_str(&format!("{total}: {line}"));
                    if !line.ends_with('\n') {
                        numbered.push('\n');
                    }
                    end = total;
                }
            }
        }
        if offset > total {
            return Err(ToolError::new(
                category::VALIDATION,
                format!("offset {offset} is past EOF; file has {total} total lines"),
            ));
        }
        let remaining = end < total;
        if remaining && reason.is_empty() {
            reason = "lines";
        }
        let mut out = json!({"outcome":"succeeded","line_start":offset,"total_lines":total});
        if end > 0 {
            out["line_end"] = json!(end);
        }
        if !content.is_empty() {
            out["content_bytes"] = json!(content.len());
            out["content"] = json!(content);
            out["numbered_content"] = json!(numbered);
        }
        if remaining {
            out["next_offset"] = json!(if end == 0 { offset } else { end + 1 });
            out["truncated"] = json!(true);
        }
        if byte_cap {
            out["truncated"] = json!(true);
        }
        if line_truncated {
            out["line_truncated"] = json!(true);
        }
        if !reason.is_empty() {
            out["truncation_reason"] = json!(reason);
        }
        Ok(out)
    }
}
