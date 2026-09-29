//! Shared domain types for Crabber.

/// Placeholder until the core domain types are introduced.
pub const CRATE_NAME: &str = "crabber-core";

#[cfg(test)]
mod tests {
    #[test]
    fn crate_name_is_stable() {
        assert_eq!(crate::CRATE_NAME, "crabber-core");
    }
}
