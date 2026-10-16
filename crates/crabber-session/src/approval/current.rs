//! Immutable accepted-answer and continuation-transition evidence.
use crate::approval::ApprovalRecord;
use crate::{
    ApprovalContinuationState, ApprovalError as Error, ApprovalOutcome, ApprovalReceipt,
    AttemptCleanup, AttemptReceipt, AttemptSnapshot, AttemptState, AttemptTarget,
    ContinuationTarget,
};
use crabber_core::{ContinuationId, EventRecord, Run, RunStatus};
use std::collections::BTreeSet;

pub(crate) struct Source {
    pub receipt: ApprovalReceipt,
    pub event: EventRecord,
}
pub(crate) struct NativeSource {
    pub receipt: AttemptReceipt,
    pub event: EventRecord,
    pub latest: AttemptSnapshot,
}
pub(crate) enum Release {
    Handoff(crate::continuation_handoff::Handoff),
    Native(NativeSource),
    Successor {
        pub_source: Source,
        attempt: AttemptTarget,
    },
}
pub(crate) struct CurrentProof {
    pub answers: Vec<Source>,
    pub latest: Source,
    pub release: Option<Release>,
}
impl Source {
    pub(crate) fn verify(&self) -> Result<(), Error> {
        crate::approval::verify_receipt(&self.receipt, &self.event)
    }
}
pub(crate) fn continuation(outcome: &ApprovalOutcome) -> &ContinuationTarget {
    match outcome {
        ApprovalOutcome::Suspended { continuation }
        | ApprovalOutcome::Answered { continuation, .. }
        | ApprovalOutcome::Claimed { continuation, .. }
        | ApprovalOutcome::ReclaimedUnstarted { continuation, .. }
        | ApprovalOutcome::Terminated { continuation, .. } => continuation,
    }
}
impl ApprovalRecord {
    pub(crate) fn verify_current(
        &self,
        run: &Run,
        proof: &CurrentProof,
        barrier: Option<&ContinuationId>,
        releasing: bool,
    ) -> Result<(), Error> {
        self.verify_current_status(run.status, proof, barrier, releasing)
    }
    pub(crate) fn verify_current_status(
        &self,
        run_status: RunStatus,
        proof: &CurrentProof,
        barrier: Option<&ContinuationId>,
        releasing: bool,
    ) -> Result<(), Error> {
        let fail = || Error::UnknownStoreOutcome;
        if proof.answers.len() > 64 {
            return Err(fail());
        }
        let mut seen = BTreeSet::new();
        for source in &proof.answers {
            source.verify()?;
            let ApprovalOutcome::Answered {
                continuation,
                interaction_id,
                ..
            } = &source.receipt.outcome
            else {
                return Err(fail());
            };
            if *continuation != self.batch.continuation || !seen.insert(interaction_id) {
                return Err(fail());
            }
            let answer = self
                .questions
                .iter()
                .find(|q| q.view.id == *interaction_id)
                .and_then(|q| q.answer.as_ref())
                .ok_or_else(fail)?;
            crate::approval::verify_recovery_answer(answer, &source.receipt)?;
        }
        if self.questions.iter().filter(|q| q.answer.is_some()).count() != seen.len() {
            return Err(fail());
        }
        proof.latest.verify()?;
        let latest = &proof.latest.receipt;
        if latest.session_id != self.session_id
            || latest.run_id != self.run_id
            || *continuation(&latest.outcome) != self.batch.continuation
        {
            return Err(fail());
        }
        match self.state {
            ApprovalContinuationState::Waiting => {
                if latest.operation_id != self.batch.id
                    || !matches!(latest.outcome, ApprovalOutcome::Suspended { .. })
                    || run_status != RunStatus::Paused
                    || barrier != Some(&self.batch.continuation.id)
                {
                    return Err(fail());
                }
            }
            ApprovalContinuationState::Claimed => {
                self.verify_claim_source(latest)?;
                if (!releasing && run_status != RunStatus::Running)
                    || barrier != Some(&self.batch.continuation.id)
                {
                    return Err(fail());
                }
            }
            ApprovalContinuationState::Released => {
                self.verify_release(run_status, proof, barrier)?;
            }
        }
        Ok(())
    }
    fn verify_claim_source(&self, latest: &ApprovalReceipt) -> Result<(), Error> {
        let (ApprovalOutcome::Claimed { attempt, .. }
        | ApprovalOutcome::ReclaimedUnstarted { attempt, .. }) = &latest.outcome
        else {
            return Err(Error::UnknownStoreOutcome);
        };
        if attempt.session_id != self.session_id
            || attempt.run_id != self.run_id
            || attempt.target != self.batch.next_attempt
            || attempt.state != AttemptState::Unstarted
            || attempt.cleanup != AttemptCleanup::Pending
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
    fn verify_release(
        &self,
        run_status: RunStatus,
        proof: &CurrentProof,
        barrier: Option<&ContinuationId>,
    ) -> Result<(), Error> {
        let fail = || Error::UnknownStoreOutcome;
        if barrier == Some(&self.batch.continuation.id) {
            return Err(fail());
        }
        if let Some(ended) = &self.termination {
            ended.request.verify_receipt(&proof.latest.receipt)?;
            if ended.ended_at != proof.latest.event.created_at
                || run_status != ended.request.run_status()
            {
                return Err(fail());
            }
            return Ok(());
        }
        self.verify_claim_source(&proof.latest.receipt)?;
        match proof.release.as_ref().ok_or_else(fail)? {
            Release::Handoff(handoff) => {
                if handoff.predecessor.domain != crate::continuation_handoff::Domain::Approval
                    || handoff.predecessor.continuation != self.batch.continuation
                    || handoff.predecessor.operation_id != proof.latest.receipt.operation_id
                    || handoff.predecessor.digest != proof.latest.receipt.semantic_digest
                    || handoff.predecessor.cursor != proof.latest.receipt.event_cursor
                    || handoff.parent != self.batch.next_attempt
                    || handoff.plan != self.batch.expected_plan
                {
                    return Err(fail());
                }
            }
            Release::Successor {
                pub_source,
                attempt,
            } => {
                pub_source.verify()?;
                let ApprovalOutcome::Suspended { continuation } = &pub_source.receipt.outcome
                else {
                    return Err(fail());
                };
                if pub_source.receipt.session_id != self.session_id
                    || pub_source.receipt.run_id != self.run_id
                    || self.batch.continuation.generation.next().ok()
                        != Some(continuation.generation)
                    || *attempt != self.batch.next_attempt
                    || pub_source.receipt.event_cursor <= proof.latest.receipt.event_cursor
                {
                    return Err(fail());
                }
            }
            Release::Native(source) => {
                crate::attempt::verify_receipt(&source.receipt, &source.event)
                    .map_err(|_| fail())?;
                let snapshot = &source.receipt.attempt;
                let status = match snapshot.state {
                    AttemptState::Completed => RunStatus::Completed,
                    AttemptState::Failed => RunStatus::Failed,
                    AttemptState::Interrupted => RunStatus::Interrupted,
                    _ => return Err(fail()),
                };
                let version = source
                    .event
                    .payload
                    .get("version")
                    .and_then(serde_json::Value::as_u64);
                let genuine = matches!(version, Some(2 | 4))
                    || (version == Some(3)
                        && source
                            .event
                            .payload
                            .get("request")
                            .and_then(|v| v.get("Retire"))
                            .is_some());
                if !genuine
                    || source.event.turn_id.is_some()
                    || snapshot != &source.latest
                    || snapshot.session_id != self.session_id
                    || snapshot.run_id != self.run_id
                    || snapshot.target != self.batch.next_attempt
                    || snapshot.cleanup != AttemptCleanup::Complete
                    || run_status != status
                    || source.receipt.event_cursor <= proof.latest.receipt.event_cursor
                {
                    return Err(fail());
                }
            }
        }
        Ok(())
    }
}
