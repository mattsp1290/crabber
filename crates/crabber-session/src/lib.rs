//! Session storage for Crabber.

/// Placeholder until session storage is introduced.
pub const CRATE_NAME: &str = "crabber-session";

#[cfg(test)]
mod tests {
    #[test]
    fn crate_name_is_stable() {
        assert_eq!(crate::CRATE_NAME, "crabber-session");
    }
}
