//! Current private state must account for independently selected immutable audits.
use crate::tool_continuation::{
    ToolContinuationError as Error, ToolContinuationOutcome as Outcome, ToolContinuationReceipt,
    ToolContinuationRecord, ToolContinuationState as State,
};
use crabber_core::{ControlId, EventKind, EventRecord, InteractionId};
use std::collections::BTreeMap;

#[derive(Default)]
pub(crate) struct CurrentAudits {
    origin: Option<ToolContinuationReceipt>,
    answers: BTreeMap<InteractionId, ToolContinuationReceipt>,
    claim: Option<ToolContinuationReceipt>,
    recovery: Option<ToolContinuationReceipt>,
}
pub(crate) fn audit_key(
    event: &EventRecord,
    record: &ToolContinuationRecord,
) -> Result<Option<ControlId>, Error> {
    if event.session_id != record.session_id
        || event.run_id != record.run_id
        || event.kind != EventKind::ContinuationChanged
        || event
            .payload
            .get("kind")
            .and_then(serde_json::Value::as_str)
            != Some("tool_continuation")
        || event
            .payload
            .pointer("/outcome/continuation/id")
            .and_then(serde_json::Value::as_str)
            != Some(record.batch.continuation.id.0.as_str())
    {
        return Ok(None);
    }
    serde_json::from_value(
        event
            .payload
            .get("operation_id")
            .cloned()
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map(Some)
    .map_err(|_| Error::UnknownStoreOutcome)
}
impl CurrentAudits {
    pub(crate) fn insert(
        &mut self,
        receipt: ToolContinuationReceipt,
        record: &ToolContinuationRecord,
    ) -> Result<(), Error> {
        let target = match &receipt.outcome {
            Outcome::Suspended { continuation }
            | Outcome::Answered { continuation, .. }
            | Outcome::Claimed { continuation, .. }
            | Outcome::ReclaimedUnstarted { continuation, .. } => continuation,
        };
        if receipt.session_id != record.session_id
            || receipt.run_id != record.run_id
            || target != &record.batch.continuation
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let replaced = match &receipt.outcome {
            Outcome::Suspended { .. } => self.origin.replace(receipt).is_some(),
            Outcome::Answered { interaction_id, .. } => {
                let duplicate = self
                    .answers
                    .insert(interaction_id.clone(), receipt)
                    .is_some();
                duplicate || self.answers.len() > 64
            }
            Outcome::Claimed { .. } => self.claim.replace(receipt).is_some(),
            Outcome::ReclaimedUnstarted { .. } => {
                if self
                    .recovery
                    .as_ref()
                    .is_none_or(|old| old.event_cursor < receipt.event_cursor)
                {
                    self.recovery = Some(receipt);
                }
                false
            }
        };
        if replaced {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
    pub(crate) fn verify(
        self,
        record: &ToolContinuationRecord,
        released: bool,
    ) -> Result<(), Error> {
        let origin = self.origin.ok_or(Error::UnknownStoreOutcome)?;
        if origin.operation_id != record.batch.id
            || origin.semantic_digest
                != record
                    .batch
                    .fingerprint(&record.session_id, &record.run_id)?
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let mut answers = self.answers;
        for question in &record.questions {
            match (answers.remove(&question.view.id), &question.answer) {
                (None, None) => (),
                (Some(receipt), Some(answer))
                    if receipt.operation_id == answer.id
                        && receipt.semantic_digest == answer.fingerprint()?
                        && receipt.event_cursor > origin.event_cursor
                        && self
                            .claim
                            .as_ref()
                            .is_none_or(|claim| receipt.event_cursor < claim.event_cursor)
                        && receipt.outcome
                            == (Outcome::Answered {
                                continuation: record.batch.continuation.clone(),
                                interaction_id: question.view.id.clone(),
                                state: question.view.state,
                            }) => {}
                _ => return Err(Error::UnknownStoreOutcome),
            }
        }
        if !answers.is_empty() {
            return Err(Error::UnknownStoreOutcome);
        }
        let expected = match (&self.claim, &record.claim) {
            (None, None) if !released => State::Waiting,
            (Some(receipt), Some(claim))
                if receipt.operation_id == claim.id
                    && receipt.semantic_digest == claim.fingerprint()?
                    && receipt.event_cursor > origin.event_cursor =>
            {
                if record
                    .questions
                    .iter()
                    .any(|question| question.answer.is_none())
                {
                    return Err(Error::UnknownStoreOutcome);
                }
                if released {
                    State::Released
                } else {
                    State::Claimed
                }
            }
            _ => return Err(Error::UnknownStoreOutcome),
        };
        if record.state != expected {
            return Err(Error::UnknownStoreOutcome);
        }
        match (&self.recovery, &record.recovery) {
            (None, None) => (),
            (Some(receipt), Some(recovery)) if receipt.operation_id == recovery.id => {
                recovery.verify_receipt(receipt)?;
                if self
                    .claim
                    .as_ref()
                    .is_none_or(|claim| claim.event_cursor >= receipt.event_cursor)
                {
                    return Err(Error::UnknownStoreOutcome);
                }
            }
            _ => return Err(Error::UnknownStoreOutcome),
        }
        Ok(())
    }
}

pub(crate) fn terminal_candidate(event: &EventRecord, record: &ToolContinuationRecord) -> bool {
    event.session_id == record.session_id
        && event.run_id == record.run_id
        && event.kind == EventKind::ExecutionAttemptChanged
        && !event.live_only
        && (event
            .payload
            .get("version")
            .and_then(serde_json::Value::as_u64)
            == Some(2)
            && event
                .payload
                .get("operation")
                .and_then(serde_json::Value::as_str)
                == Some("conclude_run")
            || event
                .payload
                .get("version")
                .and_then(serde_json::Value::as_u64)
                == Some(3))
        && serde_json::from_value::<crate::AttemptSnapshot>(
            event.payload.get("attempt").cloned().unwrap_or_default(),
        )
        .is_ok_and(|attempt| {
            attempt.session_id == record.session_id
                && attempt.run_id == record.run_id
                && attempt.target == record.batch.next_attempt
                && attempt.cleanup == crate::AttemptCleanup::Complete
                && matches!(
                    attempt.state,
                    crate::AttemptState::Completed
                        | crate::AttemptState::Failed
                        | crate::AttemptState::Interrupted
                )
        })
}
