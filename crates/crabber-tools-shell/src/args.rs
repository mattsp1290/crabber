use crabber_tools_core::{ToolError, category};
use serde::Deserialize;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Args {
    pub cmd: String,
    #[serde(default)]
    pub timeout_seconds: u64,
}
impl Args {
    pub fn parse(value: serde_json::Value) -> Result<Self, ToolError> {
        let mut a: Self = serde_json::from_value(value)
            .map_err(|_| ToolError::new(category::VALIDATION, "invalid arguments"))?;
        if a.cmd.trim().is_empty() || a.cmd.contains('\0') || a.timeout_seconds > 600 {
            return Err(ToolError::new(
                category::VALIDATION,
                "invalid command or timeout",
            ));
        }
        if a.timeout_seconds == 0 {
            a.timeout_seconds = 60;
        }
        Ok(a)
    }
}
