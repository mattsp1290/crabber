//! Turn orchestration for Crabber.

/// Placeholder until the agent loop is introduced.
pub const CRATE_NAME: &str = "crabber-runtime";

#[cfg(test)]
mod tests {
    #[test]
    fn crate_name_is_stable() {
        assert_eq!(crate::CRATE_NAME, "crabber-runtime");
    }
}
