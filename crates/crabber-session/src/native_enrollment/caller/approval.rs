//! Actual compound Approval grant and its independently published native Attempt.
use super::{Attempt, Scope};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::{
    ApprovalContinuationClaim, ApprovalContinuationRecovery, ApprovalGrant, ApprovalReceipt,
    AttemptMutation, AttemptReceipt,
};
use crabber_core::{ControlId, EventRecord, InputFingerprint, Run};
#[derive(Clone, Copy)]
pub(crate) enum ApprovalInput<'a> {
    Claim(&'a ApprovalContinuationClaim),
    Recovery(&'a ApprovalContinuationRecovery),
}
impl<'a> ApprovalInput<'a> {
    pub(crate) fn id(self) -> &'a ControlId {
        match self {
            Self::Claim(r) => &r.id,
            Self::Recovery(r) => &r.id,
        }
    }
    pub(crate) fn original(self) -> &'a ApprovalContinuationClaim {
        match self {
            Self::Claim(r) => r,
            Self::Recovery(r) => &r.original,
        }
    }
    pub(crate) fn measure(self, proof: &mut Proof) -> Result<(), Failure> {
        match self {
            Self::Claim(r) => proof.measure(r),
            Self::Recovery(r) => proof.measure(r),
        }
    }
    pub(crate) fn fingerprint(self) -> Result<InputFingerprint, crate::ApprovalError> {
        match self {
            Self::Claim(r) => r.fingerprint(),
            Self::Recovery(r) => r.fingerprint(),
        }
    }
    fn bookkeeping(self) -> (&'a str, u64) {
        match self {
            Self::Claim(r) => (&r.owner, r.lease_ms),
            Self::Recovery(r) => (&r.owner, r.lease_ms),
        }
    }
}
/// Only actual successful native compound output can populate this borrowed set.
#[derive(Clone, Copy)]
pub(crate) struct ApprovalCompletion<'a> {
    pub input: ApprovalInput<'a>,
    pub previous: &'a Run,
    pub activated: &'a Run,
    pub record: &'a crate::approval::ApprovalRecord,
    pub native_request: &'a AttemptMutation,
    pub native_receipt: &'a AttemptReceipt,
}
/// Actual parts returned by this one native issuer, consumed exactly once.
pub(crate) struct ApprovalResult {
    pub receipt: ApprovalReceipt,
    pub fence: crabber_core::RunFence,
    pub authority: crate::AttemptAuthority,
    pub calls: Vec<crate::GrantedApproval>,
}
impl ApprovalCompletion<'_> {
    pub(crate) fn issue(
        &self,
        parts: ApprovalResult,
        proof: &mut Proof,
    ) -> Result<crate::ApprovalClaimOutcome, Failure> {
        proof.measure(&(
            &parts.fence,
            &self.record.batch.source_turn_id,
            &self.record.batch.usage,
            self.record.batch.turns_used,
        ))?;
        proof.charge(std::mem::size_of::<ApprovalGrant>())?;
        Ok(crate::ApprovalClaimOutcome::Applied {
            receipt: parts.receipt,
            grant: Box::new(ApprovalGrant {
                fence: parts.fence,
                authority: parts.authority,
                calls: parts.calls,
                source_turn_id: self.record.batch.source_turn_id.clone(),
                usage: self.record.batch.usage.clone(),
                turns_used: self
                    .record
                    .batch
                    .turns_used
                    .ok_or_else(|| unknown("actual Approval turns"))?,
            }),
        })
    }
}
pub(crate) struct Approval<'a> {
    pub input: ApprovalInput<'a>,
    pub previous: &'a Run,
    pub activated: &'a Run,
    pub current: &'a Run,
    pub record: &'a crate::approval::ApprovalRecord,
    pub parent: &'a crate::attempt::AttemptRecord,
    pub native_request: &'a AttemptMutation,
    pub native_receipt: &'a AttemptReceipt,
    pub native_event: &'a EventRecord,
    pub receipt: &'a ApprovalReceipt,
    pub event: &'a EventRecord,
    pub grant: &'a ApprovalGrant,
}
impl<'a> Approval<'a> {
    pub(super) fn verify(&self, proof: &mut Proof) -> Result<Scope<'a>, Failure> {
        self.input.measure(proof)?;
        proof.measure(&(
            self.previous,
            self.activated,
            self.current,
            self.record,
            self.receipt,
            self.event,
        ))?;
        self.record
            .validate(&self.previous.session_id, &self.previous.id)
            .map_err(unknown)?;
        crate::approval::verify_receipt(self.receipt, self.event).map_err(unknown)?;
        self.transition(proof)?;
        self.outcome()?;
        Attempt {
            run: self.current,
            request: self.native_request,
            record: self.parent,
            receipt: self.native_receipt,
            event: self.native_event,
            issued: Some(&self.grant.authority),
        }
        .verify(proof)?;
        measure_grant(self.grant, proof)?;
        self.record
            .verify_grants(&self.grant.calls)
            .map_err(unknown)?;
        let claim = self.input.original();
        if self.receipt.operation_id != *self.input.id()
            || self.receipt.semantic_digest != self.input.fingerprint().map_err(unknown)?
            || self.receipt.session_id != claim.session_id
            || self.receipt.run_id != claim.run_id
            || self.previous.session_id != claim.session_id
            || self.previous.id != claim.run_id
            || self.record.batch.continuation != claim.continuation
            || self.record.batch.expected_plan != claim.expected_plan
            || self.record.state != crate::ApprovalContinuationState::Claimed
            || self.record.questions.iter().any(|q| !q.view.consumed)
            || self.parent.snapshot.target != self.record.batch.next_attempt
            || self.native_receipt.revision >= self.receipt.revision
            || self.grant.fence.run_id != self.current.id
            || self.grant.fence.claim_token != self.current.claim_token
            || self.grant.source_turn_id != self.record.batch.source_turn_id
            || self.grant.usage != self.record.batch.usage
            || Some(self.grant.turns_used) != self.record.batch.turns_used
        {
            return Err(unknown("actual Raw Approval grant"));
        }
        Ok(Scope {
            session: &self.current.session_id,
            run: &self.current.id,
        })
    }
    fn outcome(&self) -> Result<(), Failure> {
        let (prefix, target) = match self.input {
            ApprovalInput::Claim(_) => {
                if !matches!(
                    self.native_request.command,
                    crate::AttemptCommand::Reserve { .. }
                ) {
                    return Err(unknown("actual Approval Reserve"));
                }
                let crate::ApprovalOutcome::Claimed {
                    continuation,
                    attempt,
                } = &self.receipt.outcome
                else {
                    return Err(unknown("actual Approval Claim receipt"));
                };
                if *continuation != self.record.batch.continuation
                    || *attempt != self.parent.snapshot
                {
                    return Err(unknown("actual Approval Claim target"));
                }
                ("claim-attempt", &self.record.batch.next_attempt)
            }
            ApprovalInput::Recovery(request) => {
                request.verify_receipt(self.receipt).map_err(unknown)?;
                if !matches!(
                    self.native_request.command,
                    crate::AttemptCommand::ReclaimUnstarted { .. }
                ) {
                    return Err(unknown("actual Approval reclaim"));
                }
                let crate::ApprovalOutcome::ReclaimedUnstarted { attempt, .. } =
                    &self.receipt.outcome
                else {
                    return Err(unknown("actual Approval Recovery receipt"));
                };
                if *attempt != self.parent.snapshot {
                    return Err(unknown("actual Approval Recovery target"));
                }
                ("reclaim-attempt", &request.attempt)
            }
        };
        if self.native_request.id != crate::approval::derived_id(prefix, self.input.id())
            || self.native_request.command.target() != target
        {
            return Err(unknown("actual nested Approval request"));
        }
        Ok(())
    }
    fn transition(&self, proof: &mut Proof) -> Result<(), Failure> {
        // Both actual borrowed Runs were charged before this comparison clone.
        let (owner, lease) = self.input.bookkeeping();
        let mut expected = self.previous.clone();
        owner.clone_into(&mut expected.owner);
        expected.claim_token.clone_from(&self.activated.claim_token);
        expected.updated_at = self.activated.updated_at;
        expected.lease_until = expected
            .updated_at
            .checked_add(time::Duration::milliseconds(
                i64::try_from(lease).map_err(unknown)?,
            ))
            .ok_or_else(|| unknown("Approval lease overflow"))?;
        if expected != *self.activated
            || self.previous.claim_token == self.activated.claim_token
            || lease == 0
        {
            return Err(unknown("actual Approval owner activation"));
        }
        proof.measure(self.activated)?;
        expected = self.activated.clone();
        expected.status = crabber_core::RunStatus::Running;
        if matches!(self.input, ApprovalInput::Claim(_)) {
            expected.updated_at = self.native_event.created_at;
        }
        if expected != *self.current {
            return Err(unknown("actual Approval current Run"));
        }
        Ok(())
    }
}
pub(crate) fn measure_grant(grant: &ApprovalGrant, proof: &mut Proof) -> Result<(), Failure> {
    if grant.calls.len() > 64 {
        return Err(unknown("Approval target bound"));
    }
    proof.measure(&(
        &grant.fence,
        &grant.source_turn_id,
        &grant.usage,
        grant.turns_used,
    ))?;
    for call in &grant.calls {
        let tag = decision_tag(call.decision);
        proof.measure(&(&call.target, &call.call_binding, tag))?;
    }
    Ok(())
}
#[cfg(test)]
impl ApprovalInput<'_> {
    fn test_digest(self) -> InputFingerprint {
        match self {
            Self::Claim(r) => {
                crate::native_enrollment::caller::contract::snapshot_digest(&("claim", r))
            }
            Self::Recovery(r) => {
                crate::native_enrollment::caller::contract::snapshot_digest(&("recovery", r))
            }
        }
    }
}
#[cfg(test)]
impl Approval<'_> {
    pub(crate) fn test_digest(&self) -> InputFingerprint {
        let calls: Vec<_> = self
            .grant
            .calls
            .iter()
            .map(|c| (&c.target, &c.call_binding, decision_tag(c.decision)))
            .collect();
        crate::native_enrollment::caller::contract::snapshot_digest(&(
            "crabber.native.raw-approval.test.v1",
            self.input.test_digest(),
            (self.previous, self.activated, self.current, self.record),
            (
                &self.parent.snapshot,
                &self.parent.owner,
                &self.parent.token,
                &self.parent.run_token,
                self.parent.lease_until,
            ),
            (self.native_request, self.native_receipt, self.native_event),
            (self.receipt, self.event),
            (
                &self.grant.fence,
                calls,
                &self.grant.source_turn_id,
                &self.grant.usage,
                self.grant.turns_used,
            ),
        ))
    }
}
fn decision_tag(decision: crate::GrantedDecision) -> &'static str {
    match decision {
        crate::GrantedDecision::Allow => "allow",
        crate::GrantedDecision::Deny => "deny",
        crate::GrantedDecision::NotRequired => "not_required",
    }
}
