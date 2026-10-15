//! Explicit, versioned completion envelope for JSON execution middleware.
use crate::ExtensionError;
pub use crabber_core::TOOL_OUTPUT_ENVELOPE_VERSION;
use crabber_core::ToolOutput;
use serde_json::Value;

/// Serialize validated data without conferring call or execution authority.
#[must_use]
pub fn tool_output_envelope(output: ToolOutput) -> Value {
    output.into_envelope()
}

/// Decode only the declared envelope; arbitrary JSON remains the responsibility
/// of the explicit `JsonToolExecutor` adapter.
/// # Errors
/// Rejects unknown versions, malformed content and unknown authority fields.
pub fn parse_tool_output_envelope(value: Value) -> Result<ToolOutput, ExtensionError> {
    ToolOutput::from_envelope(value)
        .map_err(|_| ExtensionError::Tool("invalid typed tool completion envelope".into()))
}
