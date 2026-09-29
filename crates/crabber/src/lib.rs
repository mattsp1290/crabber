//! Public embedding surface for the Crabber agent runtime.

pub use crabber_core as core;
pub use crabber_extension as extension;
pub use crabber_providers as providers;
pub use crabber_runtime as runtime;
pub use crabber_session as session;

#[cfg(test)]
mod tests {
    #[test]
    fn core_is_available() {
        assert_eq!(crate::core::CRATE_NAME, "crabber-core");
    }
}
