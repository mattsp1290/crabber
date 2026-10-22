//! Deterministic upstream-compatible metadata.
use crabber::core::ToolInfo;
/// Model-facing schema and advisory write permission.
pub fn info() -> ToolInfo {
    ToolInfo {
        name:"apply_patch".into(),
        description:"Apply a multi-file structured patch under the workspace. Supports add, update, delete, and move. Preflights every target before writing and returns per-file operation summaries; partial=true is reserved for commit-time failures after preflight.".into(),
        parameters:serde_json::json!({"type":"object","additionalProperties":false,"properties":{"patch_text":{"type":"string","minLength":1,"description":"Patch text using the *** Begin Patch / *** End Patch grammar. Cap is 1 MiB."}},"required":["patch_text"]}),
        retry_safe:false,
        required_permissions:vec![crabber_tools_core::permission::FS_WRITE.into()],
    }
}
