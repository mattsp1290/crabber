//! Bind private continuation presentation and deadlines to the committed pause.
use crate::AttemptReceipt;
use crate::approval::ApprovalRecord;
use crate::{ApprovalError as Error, ApprovalReceipt};
use crabber_core::{EventRecord, Run};

pub(crate) struct OriginRunFacts<'a> {
    pub session_id: &'a crabber_core::SessionId,
    pub run_id: &'a crabber_core::RunId,
    pub created_at: time::OffsetDateTime,
}

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
        self.verify_origin_at(
            &OriginRunFacts {
                session_id: &run.session_id,
                run_id: &run.id,
                created_at: run.created_at,
            },
            suspension,
            audit,
            native,
            native_audit,
            source,
        )
    }
    pub(crate) fn verify_origin_at(
        &self,
        run: &OriginRunFacts<'_>,
        suspension: &ApprovalReceipt,
        audit: &EventRecord,
        native: &AttemptReceipt,
        native_audit: &EventRecord,
        source: &EventRecord,
    ) -> Result<(), Error> {
        self.validate(run.session_id, run.run_id)?;
        self.verify_recovery_batch(suspension)?;
        crate::approval::verify_receipt(suspension, audit)?;
        crate::attempt::verify_receipt(native, native_audit)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let conclusion =
            crate::approval::conclusion_at(&self.batch, run.created_at, self.created_at)?;
        conclusion
            .verify_replay(native)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let mut expected =
            crate::attempt_conclusion::materialize_scope(&conclusion, run.session_id, run.run_id)
                .map_err(|_| Error::UnknownStoreOutcome)?;
        expected.cursor = source.cursor;
        if self.handoff.as_deref()
            != crate::continuation_handoff::Handoff::from_event(audit)
                .map_err(|()| Error::UnknownStoreOutcome)?
                .as_ref()
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
