//! Actual committed native output with a selected original issuer locator.
mod verify;
use super::PreparationCompletion;
use crate::session_owner::driver::NativeClaimLocator;
pub(crate) use verify::*;

/// Fresh native capabilities plus the actual acknowledged publisher locator.
/// Replay has no locator or fresh capability. The locator is metadata only.
pub struct PreparedDriverCompletion {
    pub completion: PreparationCompletion,
    pub claim: Option<NativeClaimLocator>,
}
