//! Bind private continuation presentation and deadlines to the committed pause.
use crate::AttemptReceipt;
use crate::approval::ApprovalRecord;
use crate::{ApprovalError as Error, ApprovalReceipt};
use crabber_core::{EventRecord, Run};

impl ApprovalRecord {
    pub(crate) fn verify_origin(
        &self,
        run: &Run,
        suspension: &ApprovalReceipt,
        audit: &EventRecord,
        native: &AttemptReceipt,
        native_audit: &EventRecord,
        source: &EventRecord,
    ) -> Result<(), Error> {
        self.validate(&run.session_id, &run.id)?;
        self.verify_recovery_batch(suspension)?;
        crate::approval::verify_receipt(suspension, audit)?;
        crate::attempt::verify_receipt(native, native_audit)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let conclusion = self.batch.conclusion(run, self.created_at)?;
        conclusion
            .verify_replay(native)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let mut expected = conclusion
            .materialize(run)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        expected.cursor = source.cursor;
        if self.handoff
            != crate::continuation_handoff::Handoff::from_event(audit)
                .map_err(|()| Error::UnknownStoreOutcome)?
            || self.created_at != audit.created_at
            || native.attempt.session_id != self.session_id
            || native.attempt.run_id != self.run_id
            || native.revision <= self.batch.expected_revision
            || suspension.revision
                != native
                    .revision
                    .next()
                    .map_err(|_| Error::UnknownStoreOutcome)?
            || native.event_cursor >= suspension.event_cursor
            || source
                .cursor
                .is_none_or(|cursor| cursor >= native.event_cursor)
            || serde_json::to_value(source).map_err(|_| Error::UnknownStoreOutcome)?
                != serde_json::to_value(&expected).map_err(|_| Error::UnknownStoreOutcome)?
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}
