//! The runtime and Store materialize the same committed pause source event.
use super::{ApprovalError as Error, ApprovalSuspension};
use crate::AttemptConclusion;
use crabber_core::Run;
use time::OffsetDateTime;

impl ApprovalSuspension {
    /// Canonical native conclusion for a trusted custom Store transaction.
    /// This descriptor grants no authority and does not prove resource cleanup.
    /// # Errors
    /// Rejects invalid batch/presentation data before any native boundary write.
    pub fn conclusion(&self, run: &Run, now: OffsetDateTime) -> Result<AttemptConclusion, Error> {
        self.validate()?;
        crate::approval::conclusion_at(self, run.created_at, now)
    }
}
