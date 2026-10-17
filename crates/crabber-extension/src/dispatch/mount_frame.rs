//! Process-local callback lifetime identity, independent of semantic mount order.
use crate::{ExtensionError, ResourceRegistrationError};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct MountFrame(u64);
impl MountFrame {
    /// Unregistered templates do not name any actual published mount lifetime.
    pub(crate) const UNREGISTERED: Self = Self(0);
    pub(crate) fn new() -> Result<Self, ExtensionError> {
        NEXT.try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
            value.checked_add(1)
        })
        .map(Self)
        .map_err(|_| ResourceRegistrationError::Capacity.into())
    }
    #[cfg(test)]
    pub(crate) const fn fixture(value: u64) -> Self {
        Self(value)
    }
}
