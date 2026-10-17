//! Original compound requests are rehashed against their own protected roots.
//! A recovery never authenticates its original claim from a mutable digest alone.
use super::super::Error;
use super::ContinuationFacts as Facts;
use crate::native_parent::{CompletionOperation as Operation, RunIssuance};
use crate::session_owner::{audit::Audit, preparation::Origin};
use crate::{ToolContinuationOutcome as Outcome, ToolContinuationReceipt as Receipt};
use crabber_core::{EventRecord, InputFingerprint};

pub(crate) struct Claim<'a> {
    pub previous: &'a RunIssuance,
    pub next: &'a RunIssuance,
    pub audits: &'a [Audit],
    pub receipt: &'a Receipt,
    pub event: &'a EventRecord,
}

pub(crate) struct Evidence<'a> {
    pub receipt: &'a Receipt,
    pub event: &'a EventRecord,
    pub batch: &'a crate::ToolContinuationSuspension,
    pub suspension: &'a Receipt,
    pub suspension_event: &'a EventRecord,
    pub claim: &'a crate::ToolContinuationClaim,
    pub frontend: Option<&'a crate::FrontendContinuationClaim>,
    pub original: Option<Claim<'a>>,
}

impl Evidence<'_> {
    pub(crate) fn verify(
        &self,
        previous: &RunIssuance,
        next: &RunIssuance,
        audits: &[Audit],
    ) -> Result<Facts, Error> {
        let origin = frame(previous, next, audits, self.receipt, self.event)?;
        crate::tool_continuation::verify_receipt(self.suspension, self.suspension_event)
            .map_err(unknown)?;
        crate::continuation_origin::tool(
            self.batch,
            &self.receipt.session_id,
            &self.receipt.run_id,
            &self.batch.continuation,
            self.suspension,
        )
        .map_err(unknown)?;
        let (_, parent, _) = targets(self.receipt)?;
        if parent.target != self.batch.next_attempt
            || self.suspension.event_cursor >= self.receipt.event_cursor
        {
            return Err(Error::SemanticConflict);
        }
        let operation = next
            .completion_source
            .as_ref()
            .ok_or(Error::Unsupported)?
            .native_operation;
        let digest = match operation {
            Operation::ToolClaim | Operation::FrontendClaim => {
                if self.original.is_some() {
                    return Err(Error::SemanticConflict);
                }
                self.claim_digest(origin, self.receipt, operation)?
            }
            Operation::ToolRecovery | Operation::FrontendRecovery => {
                self.recovery_digest(origin, operation)?
            }
            _ => return Err(Error::Unsupported),
        };
        if digest != origin.intent || digest != self.receipt.semantic_digest {
            return Err(Error::SemanticConflict);
        }
        Ok(Facts {
            revision: self.receipt.revision,
            head: next.sequence,
            head_digest: crate::native_parent::issuance_digest(next).map_err(unknown)?,
            continuation: self.batch.continuation.clone(),
            attempt: self.batch.next_attempt.clone(),
        })
    }

    fn claim_digest(
        &self,
        origin: &Origin,
        receipt: &Receipt,
        operation: Operation,
    ) -> Result<InputFingerprint, Error> {
        let (continuation, parent, original_id) = targets(receipt)?;
        if original_id.is_some()
            || receipt.operation_id != self.claim.id
            || receipt.session_id != self.claim.session_id
            || receipt.run_id != self.claim.run_id
            || *continuation != self.claim.continuation
            || *continuation != self.batch.continuation
            || parent.target != self.batch.next_attempt
            || self.claim.expected_plan != self.batch.expected_plan
            || self.claim.expected_revision != origin.expected_revision
        {
            return Err(Error::SemanticConflict);
        }
        // Validate the real stored worker fields, while hashing only the public
        // semantic contract. No dummy owner/lease descriptor is constructed.
        self.claim.fingerprint().map_err(unknown)?;
        let digest = crate::tool_continuation::canonical::claim_fingerprint(
            &receipt.operation_id,
            &receipt.session_id,
            &receipt.run_id,
            continuation,
            origin.expected_revision,
            &self.batch.expected_plan,
        )
        .map_err(unknown)?;
        match (operation, &receipt.outcome, self.frontend) {
            (Operation::ToolClaim, Outcome::Claimed { .. }, None)
                if self.batch.frontend.is_empty() =>
            {
                Ok(digest)
            }
            (Operation::FrontendClaim, Outcome::FrontendClaimed { .. }, Some(frontend)) => {
                frontend.validate_for_batch(self.batch).map_err(unknown)?;
                if frontend.claim.id != self.claim.id
                    || frontend.claim.fingerprint().map_err(unknown)? != digest
                {
                    return Err(Error::SemanticConflict);
                }
                crate::frontend_continuation::claim_fingerprint(
                    &digest,
                    &frontend.resolver,
                    &frontend.results,
                )
                .map_err(unknown)
            }
            _ => Err(Error::SemanticConflict),
        }
    }

    fn recovery_digest(
        &self,
        origin: &Origin,
        operation: Operation,
    ) -> Result<InputFingerprint, Error> {
        let original = self.original.as_ref().ok_or(Error::UnknownStoreOutcome)?;
        let original_origin = frame(
            original.previous,
            original.next,
            original.audits,
            original.receipt,
            original.event,
        )?;
        let original_operation = match operation {
            Operation::ToolRecovery => Operation::ToolClaim,
            Operation::FrontendRecovery => Operation::FrontendClaim,
            _ => return Err(Error::Unsupported),
        };
        let digest = self.claim_digest(original_origin, original.receipt, original_operation)?;
        let (continuation, parent, original_id) = targets(self.receipt)?;
        if original_id != Some(&original.receipt.operation_id)
            || *continuation != self.batch.continuation
            || parent.target != self.batch.next_attempt
            || original.receipt.session_id != self.receipt.session_id
            || original.receipt.run_id != self.receipt.run_id
            || original.receipt.event_cursor >= self.receipt.event_cursor
            || self.suspension.event_cursor >= original.receipt.event_cursor
            || original_origin.intent != digest
            || original.receipt.semantic_digest != digest
            || original
                .next
                .completion_source
                .as_ref()
                .is_none_or(|source| source.native_operation != original_operation)
        {
            return Err(Error::SemanticConflict);
        }
        match (&self.receipt.outcome, operation) {
            (Outcome::ReclaimedUnstarted { .. }, Operation::ToolRecovery) => {
                crate::tool_continuation::canonical::recovery_fingerprint(
                    &self.receipt.operation_id,
                    origin.expected_revision,
                    &original.receipt.operation_id,
                    &digest,
                    &parent.target,
                )
                .map_err(unknown)
            }
            (Outcome::FrontendReclaimedUnstarted { .. }, Operation::FrontendRecovery) => {
                crate::frontend_continuation::recovery_semantics(
                    &self.receipt.operation_id,
                    origin.expected_revision,
                    &digest,
                    &parent.target,
                )
                .map_err(unknown)
            }
            _ => Err(Error::SemanticConflict),
        }
    }
}

fn frame<'a>(
    previous: &RunIssuance,
    next: &RunIssuance,
    audits: &'a [Audit],
    receipt: &Receipt,
    event: &EventRecord,
) -> Result<&'a Origin, Error> {
    let [reserve, begin, join, child_reserve, child_begin] = audits else {
        return Err(Error::UnknownStoreOutcome);
    };
    if !crate::native_parent::completion_history::verify_control(
        previous,
        next,
        reserve,
        begin,
        join,
        &crate::native_parent::completion_history::Control::Tool(receipt, event),
    )
    .map_err(unknown)?
    {
        return Err(Error::SemanticConflict);
    }
    crate::session_owner::audit::verify_current(&child_begin.record, child_reserve, child_begin)?;
    crate::session_owner::preparation::resource_source::verify(
        &child_begin.record,
        reserve,
        begin,
    )?;
    if join.record.retained_resources.as_ref() != Some(&child_begin.record.snapshot.target) {
        return Err(Error::SemanticConflict);
    }
    reserve
        .record
        .preparation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)
}

pub(crate) fn targets(
    receipt: &Receipt,
) -> Result<
    (
        &crate::ContinuationTarget,
        &crate::AttemptSnapshot,
        Option<&crabber_core::ControlId>,
    ),
    Error,
> {
    match &receipt.outcome {
        Outcome::Claimed {
            continuation,
            attempt,
        }
        | Outcome::FrontendClaimed {
            continuation,
            attempt,
        } => Ok((continuation, attempt, None)),
        Outcome::ReclaimedUnstarted {
            continuation,
            attempt,
            original_claim_id,
        }
        | Outcome::FrontendReclaimedUnstarted {
            continuation,
            attempt,
            original_claim_id,
        } => Ok((continuation, attempt, Some(original_claim_id))),
        _ => Err(Error::Unsupported),
    }
}

fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
