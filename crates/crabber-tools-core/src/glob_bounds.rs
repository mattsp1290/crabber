//! Shared bounds applied before globset parses recursive alternations.
use crate::{ToolError, category};
/// Maximum pattern length in bytes.
pub const MAX_BYTES: usize = 4096;
/// Maximum nested alternation depth before recursive glob parsing.
pub const MAX_BRACE_DEPTH: usize = 16;
fn invalid() -> ToolError {
    ToolError::new(
        category::VALIDATION,
        "invalid or overly complex glob pattern",
    )
}
/// Reject excessive size or brace depth before glob compilation.
///
/// # Errors
/// Returns `validation` when the pattern exceeds a finite parser bound.
pub fn validate(pattern: &str) -> Result<(), ToolError> {
    if pattern.len() > MAX_BYTES {
        return Err(invalid());
    }
    let (mut escaped, mut class, mut depth) = (false, false, 0usize);
    for byte in pattern.bytes() {
        if escaped {
            escaped = false;
            continue;
        }
        if byte == b'\\' {
            escaped = true;
            continue;
        }
        if class {
            if byte == b']' {
                class = false;
            }
            continue;
        }
        match byte {
            b'[' => class = true,
            b'{' => {
                depth += 1;
                if depth > MAX_BRACE_DEPTH {
                    return Err(invalid());
                }
            }
            b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}
