//! Private host termination evidence, distinct from worker phase completion.
use crate::tool_continuation::{
    ToolContinuationError as Error, ToolContinuationOutcome, ToolContinuationReceipt,
    ToolContinuationRecord, ToolContinuationState, canonical, derived_id,
};
use crate::{RunControlAction, RunControlKind, RunControlReceipt, RunControlRequest};
use crabber_core::{ControlId, InputFingerprint};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolContinuationTermination {
    pub id: ControlId,
    pub original: RunControlRequest,
    pub accepted: RunControlReceipt,
    pub batch_digest: InputFingerprint,
    pub created_at: OffsetDateTime,
}
impl ToolContinuationTermination {
    pub(crate) fn new(
        record: &ToolContinuationRecord,
        original: RunControlRequest,
        accepted: RunControlReceipt,
        now: OffsetDateTime,
    ) -> Result<Self, Error> {
        let proof = Self {
            id: derived_id("host-cancel", &original.id),
            original,
            accepted,
            batch_digest: record
                .batch
                .fingerprint(&record.session_id, &record.run_id)?,
            created_at: now,
        };
        proof.verify(record)?;
        Ok(proof)
    }
    pub(crate) fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        canonical::digest(&("crabber.tool-continuation.host-termination.v1", self))
    }
    pub(crate) fn verify(&self, record: &ToolContinuationRecord) -> Result<(), Error> {
        if self.id != derived_id("host-cancel", &self.original.id)
            || !matches!(self.original.action, RunControlAction::Cancel { .. })
            || self.original.target.session_id != record.session_id
            || self.original.target.run_id != record.run_id
            || self.original.target.plan_fingerprint != record.batch.expected_plan.as_str()
            || self.accepted.operation_id != self.original.id
            || self.accepted.target != self.original.target
            || self.accepted.kind != RunControlKind::Cancel
            || self.accepted.semantic_digest
                != self
                    .original
                    .fingerprint()
                    .map_err(|_| Error::UnknownStoreOutcome)?
            || self.batch_digest
                != record
                    .batch
                    .fingerprint(&record.session_id, &record.run_id)?
            || crate::snapshot::encoded_bytes(self, 32_768).is_none()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
    pub(crate) fn verify_receipt(
        &self,
        record: &ToolContinuationRecord,
        receipt: &ToolContinuationReceipt,
    ) -> Result<(), Error> {
        self.verify(record)?;
        if record.state != ToolContinuationState::Released
            || receipt.operation_id != self.id
            || receipt.semantic_digest != self.fingerprint()?
            || receipt.outcome
                != (ToolContinuationOutcome::Terminated {
                    continuation: record.batch.continuation.clone(),
                    original_cancel_id: self.original.id.clone(),
                })
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
    pub(crate) fn verify_run(
        &self,
        record: &ToolContinuationRecord,
        run: &crabber_core::Run,
        latest: &crate::AttemptSnapshot,
    ) -> Result<(), Error> {
        crate::run_control::verify_target(&self.original.target, run)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if run.status != crabber_core::RunStatus::Interrupted
            || run.checkpoint.as_ref() != Some(&crate::tool_continuation::checkpoint(&record.batch))
            || run.usage != record.batch.usage
        {
            return Err(Error::UnknownStoreOutcome);
        }
        self.verify_attempt(record, latest)
    }
    pub(crate) fn verify_attempt(
        &self,
        record: &ToolContinuationRecord,
        latest: &crate::AttemptSnapshot,
    ) -> Result<(), Error> {
        if latest.session_id != record.session_id
            || latest.run_id != record.run_id
            || latest.run_id != self.original.target.run_id
            || latest.target != record.batch.attempt
            || latest.state != crate::AttemptState::Suspended
            || latest.cleanup != crate::AttemptCleanup::Complete
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}
