//! A quiescent suspension closes background observation, never worker authority.
use std::sync::atomic::{AtomicU8, Ordering};
const ACTIVE: u8 = 0;
const QUIESCENT_COMMIT: u8 = 1;
const CLOSED: u8 = 2;

#[derive(Default)]
pub(super) struct ObservationLifetime(AtomicU8);
impl ObservationLifetime {
    pub(super) fn active(&self) -> bool {
        self.0.load(Ordering::SeqCst) == ACTIVE
    }
    /// Call only after local resources joined, on a path that cannot return to
    /// model/tool effects. Keep the guard through bounded retries/reconciliation.
    pub(super) fn quiescent_commit(&self) -> QuiescentCommit<'_> {
        self.0.fetch_max(QUIESCENT_COMMIT, Ordering::SeqCst);
        QuiescentCommit(self)
    }
}
pub(super) struct QuiescentCommit<'a>(&'a ObservationLifetime);
impl Drop for QuiescentCommit<'_> {
    fn drop(&mut self) {
        self.0.0.store(CLOSED, Ordering::SeqCst);
    }
}
