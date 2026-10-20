//! This adapter withholds one ACK only after the actual native publisher succeeds.
use crabber::session::{PublicSourceMutation, PublicStateError, PublicStateReceipt};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
pub(crate) struct SourceAck {
    pub(crate) calls: AtomicUsize,
    pub(crate) committed: Mutex<Option<(PublicSourceMutation, PublicStateReceipt)>>,
}
impl SourceAck {
    pub(super) fn after_commit(
        &self,
        original: PublicSourceMutation,
        receipt: PublicStateReceipt,
    ) -> Result<PublicStateReceipt, PublicStateError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            *self.committed.lock().unwrap() = Some((original, receipt));
            Err(PublicStateError::UnknownStoreOutcome)
        } else {
            Ok(receipt)
        }
    }
}
