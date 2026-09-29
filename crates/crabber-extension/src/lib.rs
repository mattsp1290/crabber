//! Extension points for Crabber.

/// Placeholder until extension interfaces are introduced.
pub const CRATE_NAME: &str = "crabber-extension";

#[cfg(test)]
mod tests {
    #[test]
    fn crate_name_is_stable() {
        assert_eq!(crate::CRATE_NAME, "crabber-extension");
    }
}
