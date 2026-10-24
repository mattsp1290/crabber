//! Trusted native extensions for Crabber. Hosts retain trust and process policy.
pub mod ask_user;
pub mod background_jobs;
pub mod command_guard;
mod contextual;
pub mod delegate_task;
mod process;
pub mod python_repl;
pub mod rtk_reducer;
mod safe_future;
pub mod tool_result_redactor;
pub mod web_search;
pub mod workspace_instructions;

use sha2::{Digest, Sha256};

fn config_hash(value: &impl serde::Serialize) -> String {
    let encoded = serde_json::to_vec(value).expect("extension configuration serializes");
    format!("{:x}", Sha256::digest(encoded))
}

fn config_error(code: &'static str) -> crabber::ExtensionError {
    crabber::ExtensionError::Plan(format!("extension configuration invalid: {code}"))
}

fn valid_identity(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}
