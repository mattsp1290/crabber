//! Bounds are checked before globset's recursive parser; regex compilation is fallible.
pub(crate) use crabber_tools_core::glob_bounds::validate;
#[cfg(test)]
use crabber_tools_core::glob_bounds::{MAX_BRACE_DEPTH, MAX_BYTES};
use crabber_tools_core::{ToolError, category};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
fn invalid() -> ToolError {
    ToolError::new(
        category::VALIDATION,
        "invalid or overly complex glob pattern",
    )
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
