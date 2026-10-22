//! Deterministic shell metadata.
use crabber::core::ToolInfo;
/// Model-facing shell contract.
pub fn info() -> ToolInfo {
    ToolInfo {name:"shell".into(),description:"Run a command with host-configured shell policy in the agent's workspace cwd. Captures stdout, stderr, exit code, and duration. Per-call timeout defaults to 60s and is capped at 600s. Stdout/stderr use host-configured per-stream caps (explicitly supplied by the host); oversize output sets truncated=true.".into(),parameters:serde_json::json!({"properties": {"cmd": {"minLength": 1, "description": "Shell command body. Executed with host-configured shell policy in the agent's workspace cwd.", "type": "string"}, "timeout_seconds": {"maximum": 600, "minimum": 0, "description": "Per-call timeout in seconds. 0 or omitted applies the default (60s). Cap is 600s.", "type": "integer"}}, "additionalProperties": false, "required": ["cmd"], "type": "object"}),retry_safe:false,required_permissions:vec![crabber_tools_core::permission::PROCESS_EXEC.into(),crabber_tools_core::permission::FS_WRITE.into()]}
}
