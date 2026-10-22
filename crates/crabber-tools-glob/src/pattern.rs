//! Bounds are checked before globset's recursive parser; regex compilation is fallible.
use crabber_tools_core::{ToolError, category};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
pub(crate) const MAX_BYTES: usize = 4096;
pub(crate) const MAX_BRACE_DEPTH: usize = 16;
fn invalid() -> ToolError {
    ToolError::new(
        category::VALIDATION,
        "invalid or overly complex glob pattern",
    )
}
pub(crate) fn validate(pattern: &str) -> Result<(), ToolError> {
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
pub(crate) fn compile(pattern: &str) -> Result<GlobSet, ToolError> {
    validate(pattern)?;
    let glob = GlobBuilder::new(pattern)
        .literal_separator(true)
        .build()
        .map_err(|_| invalid())?;
    GlobSetBuilder::new()
        .add(glob)
        .build()
        .map_err(|_| invalid())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parser_bounds_respect_escaped_and_class_braces() {
        assert!(validate(&"{".repeat(MAX_BRACE_DEPTH + 1)).is_err());
        assert!(validate(&"x".repeat(MAX_BYTES + 1)).is_err());
        assert!(compile(&"\\{".repeat(100)).is_ok());
        assert!(compile(&format!("[{}]", "{".repeat(100))).is_ok());
        assert!(compile("{a,b}/**/*.rs").unwrap().is_match("a/src/lib.rs"));
        assert!(compile("[").is_err());
    }
}
