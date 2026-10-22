//! Deterministic identities for schemas and serializable host policies.
use crabber::core::ToolInfo;
use serde::Serialize;
use sha2::{Digest, Sha256};
/// Schema identity domain separator.
pub const SCHEMA_IDENTITY_VERSION: &str = "crabber-tools-tool-schema-v1";
/// Configuration identity domain separator.
pub const CONFIG_IDENTITY_VERSION: &str = "crabber-tools-config-v2";
/// Explicit revision of persisted tool behavior, independent of the workspace version.
pub const TOOLS_IDENTITY_REVISION: &str = "1";
/// Hash name, description, and recursively sorted parameter keys.
pub fn schema_hash(info: &ToolInfo) -> String {
    let mut parameters = info.parameters.clone();
    parameters.sort_all_objects();
    config_hash(&(
        SCHEMA_IDENTITY_VERSION,
        &info.name,
        &info.description,
        parameters,
    ))
}
/// SHA256 over JSON encoding. Caller must include the appropriate domain separator.
/// Panics only for a policy whose Serialize implementation rejects serialization.
pub fn config_hash(value: &impl Serialize) -> String {
    let bytes = serde_json::to_vec(value).expect("policy serializes");
    format!("{:x}", Sha256::digest(bytes))
}
/// Advisory permissions; hosts must enforce their own permission policy.
pub mod permission {
    /// Ask the host user a question.
    pub const INTERACTION_ASK: &str = "interaction.ask";
    /// Read workspace files.
    pub const FS_READ: &str = "workspace.fs.read";
    /// Mutate workspace files.
    pub const FS_WRITE: &str = "workspace.fs.write";
    /// Execute subprocesses.
    pub const PROCESS_EXEC: &str = "workspace.process.exec";
}
