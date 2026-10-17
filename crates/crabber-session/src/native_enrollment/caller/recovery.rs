//! Actual admission/expired recovery outputs; no synthesized event or authority.
use super::initial::RecoveryPublication;
use super::verify::Scope;
use crate::native_parent::{CompletionOperation, CompletionSource};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::session_owner::preparation::{ExpiredRunOutcome, ExpiredRunPreparation};
use crate::{ClaimUnstartedAdmissionRequest, ClaimedAdmission};
use crabber_core::{Run, RunFence};

pub(crate) fn measure_request(
    request: &ClaimUnstartedAdmissionRequest,
    proof: &mut Proof,
) -> Result<(), Failure> {
    proof.measure(&(
        &request.session_id,
        &request.key,
        &request.expected_fence,
        &request.expected_owner,
        &request.fingerprint,
        &request.behavior_fingerprint,
        &request.semantic_digest,
        &request.capsule_digest,
        &request.owner,
        request.lease,
    ))
}
pub(crate) fn measure_expired_request(
    request: &crate::session_owner::preparation::InspectExpiredRunPreparation,
    proof: &mut Proof,
) -> Result<(), Failure> {
    proof.measure(&(
        &request.session_id,
        &request.expected_fence,
        &request.owner,
        request.lease,
    ))
}
pub(crate) fn measure_expired(
    source: &ExpiredRunPreparation,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let request = source.request();
    proof.measure(&(
        &request.session_id,
        &request.expected_fence,
        &request.owner,
        request.lease,
        source.expected_revision(),
        source.target(),
    ))
}
pub(crate) struct AdmissionRecovery<'a> {
    pub request: &'a ClaimUnstartedAdmissionRequest,
    pub previous: &'a Run,
    pub result: &'a ClaimedAdmission,
    pub completion: Option<&'a CompletionSource>,
    pub publication: &'a RecoveryPublication,
}
impl<'a> AdmissionRecovery<'a> {
    pub(super) fn verify(&self, proof: &mut Proof) -> Result<Scope<'a>, Failure> {
        measure_request(self.request, proof)?;
        proof.measure(&(
            self.previous,
            &self.result.run,
            &self.result.fence,
            &self.result.record,
            self.completion,
        ))?;
        self.result
            .record
            .verify_claim(self.request, self.previous)
            .map_err(unknown)?;
        replacement(
            self.previous,
            &self.result.run,
            &self.result.fence,
            &self.request.owner,
            self.request.lease,
            proof,
        )?;
        publication(self.publication, &self.result.run, self.completion, proof)?;
        if self
            .completion
            .is_some_and(|source| source.native_operation != CompletionOperation::AdmissionRecovery)
        {
            return Err(unknown("actual admission recovery completion"));
        }
        Ok(Scope {
            session: &self.previous.session_id,
            run: &self.previous.id,
        })
    }
}
pub(crate) struct ExpiredRecovery<'a> {
    pub source: &'a ExpiredRunPreparation,
    pub previous: &'a Run,
    pub updated: &'a Run,
    pub completion: &'a CompletionSource,
    pub outcome: &'a ExpiredRunOutcome,
    pub publication: &'a RecoveryPublication,
}
impl<'a> ExpiredRecovery<'a> {
    pub(super) fn verify(&self, proof: &mut Proof) -> Result<Scope<'a>, Failure> {
        measure_expired(self.source, proof)?;
        proof.measure(&(self.previous, self.updated, self.completion))?;
        let ExpiredRunOutcome::Claimed { receipt, fence } = self.outcome else {
            return Err(unknown("replayed expired result in fresh writer"));
        };
        proof.measure(&(
            &receipt.session_id,
            &receipt.run_id,
            &receipt.operation_id,
            receipt.issuance,
            receipt.revision,
            fence,
        ))?;
        let request = self.source.request();
        if request.session_id != self.previous.session_id
            || request.expected_fence.run_id != self.previous.id
            || request.expected_fence.claim_token != self.previous.claim_token
            || receipt.session_id != self.updated.session_id
            || receipt.run_id != self.updated.id
            || receipt.operation_id != self.completion.root_join_id
            || Some(receipt.issuance) != self.source.expected_issuance().checked_add(1)
            || receipt.issuance != self.publication.issuer.sequence
            || self.completion.native_operation != CompletionOperation::ExpiredRunResume
            || self.completion.native_semantic_digest
                != self.source.fingerprint().map_err(unknown)?
        {
            return Err(unknown("actual expired recovery result"));
        }
        replacement(
            self.previous,
            self.updated,
            fence,
            &request.owner,
            request.lease,
            proof,
        )?;
        publication(self.publication, self.updated, Some(self.completion), proof)?;
        Ok(Scope {
            session: &self.previous.session_id,
            run: &self.previous.id,
        })
    }
}
fn publication(
    actual: &RecoveryPublication,
    run: &Run,
    completion: Option<&CompletionSource>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    proof.measure(&(&actual.issuer, &actual.source))?;
    actual.issuer.verify(run).map_err(unknown)?;
    if actual.issuer.completion_source.as_ref() != completion {
        return Err(unknown("actual recovery issuer completion"));
    }
    if let Some(source) = &actual.source {
        source.verify(&actual.issuer).map_err(unknown)?;
    } else if completion.is_some() {
        return Err(unknown("prepared recovery without original journal source"));
    }
    Ok(())
}
fn replacement(
    previous: &Run,
    updated: &Run,
    fence: &RunFence,
    owner: &str,
    lease: std::time::Duration,
    proof: &mut Proof,
) -> Result<(), Failure> {
    // The clone is bounded by this SAME original caller context before allocation.
    proof.measure(previous)?;
    let mut expected = previous.clone();
    owner.clone_into(&mut expected.owner);
    expected.claim_token.clone_from(&updated.claim_token);
    expected.lease_until = updated
        .updated_at
        .checked_add(time::Duration::try_from(lease).map_err(unknown)?)
        .ok_or_else(|| unknown("recovery lease overflow"))?;
    expected.updated_at = updated.updated_at;
    if expected != *updated
        || previous.claim_token == updated.claim_token
        || fence.run_id != updated.id
        || fence.claim_token != updated.claim_token
        || lease.is_zero()
    {
        return Err(unknown("actual native ownership replacement"));
    }
    Ok(())
}

#[path = "recovery/legacy.rs"]
mod legacy;
pub(crate) use legacy::LegacyExpired;
