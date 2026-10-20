//! Exact original compound approval evidence, separate from capability holders.
use super::super::Error;
use super::ContinuationFacts as Facts;
use crate::native_parent::{CompletionOperation as Operation, RunIssuance};
use crate::session_owner::{audit::Audit, preparation::ExistingWorkTarget};
use crabber_core::EventRecord;

pub(crate) struct Evidence<'a> {
    pub receipt: &'a crate::ApprovalReceipt,
    pub event: &'a EventRecord,
    pub parent: &'a crate::AttemptReceipt,
    pub parent_event: &'a EventRecord,
    pub batch: &'a crate::ApprovalSuspension,
    pub suspension: &'a crate::ApprovalReceipt,
    pub suspension_event: &'a EventRecord,
    pub pause_parent: &'a crate::AttemptReceipt,
    pub pause_event: &'a EventRecord,
    pub pause_frame: &'a EventRecord,
    pub original: Option<(&'a crate::ApprovalReceipt, &'a EventRecord)>,
    pub status: Option<&'a RunIssuance>,
}
impl Evidence<'_> {
    pub(crate) fn verify(
        &self,
        previous: &RunIssuance,
        next: &RunIssuance,
        audits: &[Audit],
    ) -> Result<Facts, Error> {
        let [reserve, begin, join, child_reserve, child_begin] = audits else {
            return Err(Error::UnknownStoreOutcome);
        };
        if !crate::native_parent::completion_history::verify_control(
            previous,
            next,
            reserve,
            begin,
            join,
            &crate::native_parent::completion_history::Control::Approval(self.receipt, self.event),
        )
        .map_err(unknown)?
        {
            return Err(Error::SemanticConflict);
        }
        crate::session_owner::audit::verify_current(
            &child_begin.record,
            child_reserve,
            child_begin,
        )?;
        crate::session_owner::preparation::resource_source::verify(
            &child_begin.record,
            reserve,
            begin,
        )?;
        if join.record.retained_resources.as_ref() != Some(&child_begin.record.snapshot.target) {
            return Err(Error::SemanticConflict);
        }
        let origin = reserve
            .record
            .preparation
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?;
        let (target, attempt) = self.targets()?;
        crate::approval::original::verify_native_origin(
            self.batch,
            self.suspension,
            self.suspension_event,
            self.pause_parent,
            self.pause_event,
            self.pause_frame,
        )
        .map_err(unknown)?;
        if self.batch.next_attempt != *attempt {
            return Err(Error::SemanticConflict);
        }
        let (intent, command, domain) = self.intent(origin, next)?;
        let crate::session_owner::audit::RevisionState::Present(effective) =
            join.publication.ok_or(Error::UnknownStoreOutcome)?.after
        else {
            return Err(Error::UnknownStoreOutcome);
        };
        let mutation = crate::AttemptMutation {
            id: crate::approval::derived_id(domain, &self.receipt.operation_id),
            expected_revision: effective,
            command,
        };
        crate::attempt::verify_receipt(self.parent, self.parent_event).map_err(unknown)?;
        let parent_command = self
            .parent_event
            .payload
            .get("command")
            .ok_or(Error::UnknownStoreOutcome)?;
        if intent != origin.intent
            || self.parent.operation_id != mutation.id
            || self.parent.attempt != *self.attempt_snapshot()?
            || serde_json::to_value(&mutation.command).map_err(unknown)? != *parent_command
            || mutation
                .fingerprint(&self.receipt.session_id, &self.receipt.run_id)
                .map_err(unknown)?
                != self.parent.semantic_digest
            || self.receipt.revision != self.parent.revision.next().map_err(unknown)?
            || self.parent.event_cursor >= self.receipt.event_cursor
            || self.suspension.event_cursor >= self.parent.event_cursor
        {
            return Err(Error::SemanticConflict);
        }
        let head = self.head(next)?;
        Ok(Facts {
            revision: self.receipt.revision,
            head: head.sequence,
            head_digest: crate::native_parent::issuance_digest(head).map_err(unknown)?,
            continuation: target.clone(),
            attempt: attempt.clone(),
        })
    }
    fn targets(&self) -> Result<(&crate::ContinuationTarget, &crate::AttemptTarget), Error> {
        match &self.receipt.outcome {
            crate::ApprovalOutcome::Claimed {
                continuation,
                attempt,
            }
            | crate::ApprovalOutcome::ReclaimedUnstarted {
                continuation,
                attempt,
                ..
            } => Ok((continuation, &attempt.target)),
            _ => Err(Error::SemanticConflict),
        }
    }
    fn attempt_snapshot(&self) -> Result<&crate::AttemptSnapshot, Error> {
        match &self.receipt.outcome {
            crate::ApprovalOutcome::Claimed { attempt, .. }
            | crate::ApprovalOutcome::ReclaimedUnstarted { attempt, .. } => Ok(attempt),
            _ => Err(Error::SemanticConflict),
        }
    }
    fn intent(
        &self,
        origin: &crate::session_owner::preparation::Origin,
        next: &RunIssuance,
    ) -> Result<
        (
            crabber_core::InputFingerprint,
            crate::AttemptCommand,
            &'static str,
        ),
        Error,
    > {
        let source = next.completion_source.as_ref().ok_or(Error::Unsupported)?;
        let (continuation, attempt) = self.targets()?;
        let result = match (&self.receipt.outcome, source.native_operation) {
            (crate::ApprovalOutcome::Claimed { .. }, Operation::ApprovalClaim) => {
                if self.original.is_some() {
                    return Err(Error::SemanticConflict);
                }
                (
                    crate::approval::control_fingerprint(&(
                        "crabber.approval.claim.v1",
                        &self.receipt.session_id,
                        &self.receipt.run_id,
                        origin.expected_revision,
                        continuation,
                        &self.batch.expected_plan,
                    ))
                    .map_err(unknown)?,
                    crate::AttemptCommand::Reserve {
                        target: attempt.clone(),
                    },
                    "claim-attempt",
                )
            }
            (
                crate::ApprovalOutcome::ReclaimedUnstarted {
                    original_claim_id, ..
                },
                Operation::ApprovalRecovery,
            ) => {
                let (original, event) = self.original.ok_or(Error::UnknownStoreOutcome)?;
                crate::approval::verify_receipt(original, event).map_err(unknown)?;
                if original.operation_id != *original_claim_id
                    || original.session_id != self.receipt.session_id
                    || original.run_id != self.receipt.run_id
                    || !matches!(&original.outcome, crate::ApprovalOutcome::Claimed { continuation: t, attempt: a }
                        if t == continuation && a.target == *attempt)
                    || original.event_cursor >= self.parent.event_cursor
                {
                    return Err(Error::SemanticConflict);
                }
                (
                    crate::approval::control_fingerprint(&(
                        "crabber.approval.reclaim-unstarted.v1",
                        original_claim_id,
                        &original.semantic_digest,
                        attempt,
                        origin.expected_revision,
                    ))
                    .map_err(unknown)?,
                    crate::AttemptCommand::ReclaimUnstarted {
                        target: attempt.clone(),
                    },
                    "reclaim-attempt",
                )
            }
            _ => return Err(Error::Unsupported),
        };
        if result.0 != source.native_semantic_digest {
            return Err(Error::SemanticConflict);
        }
        Ok(result)
    }
    fn head<'a>(&'a self, next: &'a RunIssuance) -> Result<&'a RunIssuance, Error> {
        let claim = next
            .completion_source
            .as_ref()
            .ok_or(Error::Unsupported)?
            .native_operation
            == Operation::ApprovalClaim;
        if !claim {
            if self.status.is_some() {
                return Err(Error::SemanticConflict);
            }
            return Ok(next);
        }
        let status = self.status.ok_or(Error::UnknownStoreOutcome)?;
        status.validate().map_err(unknown)?;
        if status.version != 1
            || status.sequence
                != next
                    .sequence
                    .checked_add(1)
                    .ok_or(Error::UnknownStoreOutcome)?
            || status.state.session_id != next.state.session_id
            || status.state.run_id != next.state.run_id
            || status.state.token != next.state.token
            || status.state.status != crabber_core::RunStatus::Running
            || status.completion_source.is_some()
            || status.ownership_handoff.is_some()
        {
            return Err(Error::SemanticConflict);
        }
        Ok(status)
    }
}
pub(crate) fn work(receipt: &crate::ApprovalReceipt) -> Result<ExistingWorkTarget, Error> {
    match &receipt.outcome {
        crate::ApprovalOutcome::Claimed { continuation, .. } => {
            Ok(ExistingWorkTarget::ApprovalClaim(continuation.clone()))
        }
        crate::ApprovalOutcome::ReclaimedUnstarted {
            continuation,
            attempt,
            ..
        } => Ok(ExistingWorkTarget::ApprovalRecovery {
            continuation: continuation.clone(),
            attempt: attempt.target.clone(),
        }),
        _ => Err(Error::Unsupported),
    }
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}

/// The protected compound operation selects its exact native receipt domain.
pub(crate) fn parent_operation(
    next: &RunIssuance,
    id: &crabber_core::ControlId,
) -> Result<(bool, crabber_core::ControlId), Error> {
    let operation = next
        .completion_source
        .as_ref()
        .ok_or(Error::Unsupported)?
        .native_operation;
    let (claim, domain) = match operation {
        Operation::ApprovalClaim => (true, "claim-attempt"),
        Operation::ApprovalRecovery => (false, "reclaim-attempt"),
        _ => return Err(Error::Unsupported),
    };
    Ok((claim, crate::approval::derived_id(domain, id)))
}
