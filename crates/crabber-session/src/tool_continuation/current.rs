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
    termination: Option<ToolContinuationReceipt>,
}
pub(crate) fn audit_key(
    event: &EventRecord,
    record: &ToolContinuationRecord,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<ControlId>, crate::public_session::v2::proof::ToolReadError> {
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
    let key = event
        .payload
        .get("operation_id")
        .and_then(serde_json::Value::as_str)
        .ok_or(Error::UnknownStoreOutcome)?;
    proof
        .measure(&key)
        .map_err(crate::public_session::v2::proof::ToolReadError::Budget)?;
    Ok(Some(ControlId::from(key)))
}
impl CurrentAudits {
    pub(crate) fn insert(
        &mut self,
        receipt: ToolContinuationReceipt,
        record: &ToolContinuationRecord,
    ) -> Result<(), Error> {
        let target = match &receipt.outcome {
            Outcome::Terminated { continuation, .. }
            | Outcome::Suspended { continuation }
            | Outcome::Answered { continuation, .. }
            | Outcome::Claimed { continuation, .. }
            | Outcome::FrontendClaimed { continuation, .. }
            | Outcome::ReclaimedUnstarted { continuation, .. }
            | Outcome::FrontendReclaimedUnstarted { continuation, .. } => continuation,
        };
        if receipt.session_id != record.session_id
            || receipt.run_id != record.run_id
            || target != &record.batch.continuation
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let replaced = match &receipt.outcome {
            Outcome::Terminated { .. } => self.termination.replace(receipt).is_some(),
            Outcome::Suspended { .. } => self.origin.replace(receipt).is_some(),
            Outcome::Answered { interaction_id, .. } => {
                let duplicate = self
                    .answers
                    .insert(interaction_id.clone(), receipt)
                    .is_some();
                duplicate || self.answers.len() > 64
            }
            Outcome::Claimed { .. } | Outcome::FrontendClaimed { .. } => {
                self.claim.replace(receipt).is_some()
            }
            Outcome::ReclaimedUnstarted { .. } | Outcome::FrontendReclaimedUnstarted { .. } => {
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
                        && self
                            .termination
                            .as_ref()
                            .is_none_or(|end| receipt.event_cursor < end.event_cursor)
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
        if let Some(termination) = &self.termination {
            let proof = record
                .termination
                .as_ref()
                .ok_or(Error::UnknownStoreOutcome)?;
            proof.verify_receipt(record, termination)?;
            if released
                || self.claim.is_some()
                || self.recovery.is_some()
                || termination.event_cursor <= origin.event_cursor
                || termination.event_cursor <= proof.accepted.event_cursor
            {
                return Err(Error::UnknownStoreOutcome);
            }
            return Ok(());
        }
        if record.termination.is_some() {
            return Err(Error::UnknownStoreOutcome);
        }
        let expected = match (&self.claim, &record.claim) {
            (None, None) if !released => State::Waiting,
            (Some(receipt), Some(claim))
                if receipt.operation_id == claim.id
                    && receipt.semantic_digest == record.claim_digest()?
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
        verify_claim_transition(self.recovery.as_ref(), self.claim.as_ref(), record)
    }
}
fn verify_claim_transition(
    recovery: Option<&ToolContinuationReceipt>,
    claim: Option<&ToolContinuationReceipt>,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    match (recovery, &record.recovery, &record.frontend_recovery) {
        (None, None, None) => (),
        (Some(receipt), Some(recovery), None) if receipt.operation_id == recovery.id => {
            recovery.verify_receipt(receipt)?;
        }
        (Some(receipt), None, Some(recovery)) if receipt.operation_id == recovery.id => {
            recovery.verify_receipt(receipt)?;
        }
        _ => return Err(Error::UnknownStoreOutcome),
    }
    if recovery
        .is_some_and(|receipt| claim.is_none_or(|claim| claim.event_cursor >= receipt.event_cursor))
    {
        return Err(Error::UnknownStoreOutcome);
    }
    if let Some(receipt) = recovery.or(claim)
        && record.latest_claim_source()?
            != (
                receipt.operation_id.clone(),
                receipt.semantic_digest.clone(),
            )
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

pub(crate) fn terminal_candidate(event: &EventRecord, record: &ToolContinuationRecord) -> bool {
    event.session_id == record.session_id
        && event.run_id == record.run_id
        && event.kind == EventKind::ExecutionAttemptChanged
        && !event.live_only
        && matches!(
            (
                event
                    .payload
                    .get("version")
                    .and_then(serde_json::Value::as_u64),
                event
                    .payload
                    .get("operation")
                    .and_then(serde_json::Value::as_str)
            ),
            (Some(2), Some("conclude_run"))
                | (Some(4), Some("conclude_terminal"))
                | (Some(3 | 5), _)
        )
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
