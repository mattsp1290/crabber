//! Actual compound parent/phase issuers. No standalone native receipt is invented.
#[path = "tool/input.rs"]
mod input;
use super::initial::RecoveryPublication;
use super::verify::Scope;
use super::{measure_authority, measure_phase_authority, measure_phase_record, measure_record};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::{
    attempt::AttemptRecord, phase::PhaseRecord, tool_continuation::ToolContinuationRecord,
};
use crabber_core::{EventRecord, Run};
pub(crate) use input::ToolInput;
pub(crate) struct Tool<'a> {
    pub input: ToolInput<'a>,
    pub previous: &'a Run,
    pub current: &'a Run,
    pub record: &'a ToolContinuationRecord,
    pub previous_parent: &'a AttemptRecord,
    pub parent: &'a AttemptRecord,
    pub native_request: &'a crate::AttemptMutation,
    pub phases: &'a [PhaseRecord],
    pub publication: &'a RecoveryPublication,
    pub completion: Option<&'a crate::native_parent::CompletionSource>,
    pub receipt: &'a crate::ToolContinuationReceipt,
    pub event: &'a EventRecord,
    pub grant: &'a crate::ToolContinuationGrant,
}
impl<'a> Tool<'a> {
    pub(super) fn verify(&self, proof: &mut Proof) -> Result<Scope<'a>, Failure> {
        self.input.measure(proof)?;
        proof.measure(&(
            self.previous,
            self.current,
            self.record,
            self.native_request,
            self.receipt,
            self.event,
            self.completion,
        ))?;
        self.record.validate().map_err(unknown)?;
        self.input.verify_receipt(self.receipt).map_err(unknown)?;
        crate::tool_continuation::verify_receipt(self.receipt, self.event).map_err(unknown)?;
        self.transition(proof)?;
        self.parent(proof)?;
        self.targets(proof)?;
        self.payload(proof)?;
        self.grant
            .verify_native_ack(self.receipt)
            .map_err(unknown)?;
        let claim = self.input.original();
        if self.record.session_id != claim.session_id
            || self.record.run_id != claim.run_id
            || self.record.batch.continuation != claim.continuation
            || self.record.batch.expected_plan != claim.expected_plan
            || self.record.state != crate::ToolContinuationState::Claimed
            || self.record.questions.iter().any(|q| !q.view.consumed)
            || self.grant.fence.run_id != self.current.id
            || self.grant.fence.claim_token != self.current.claim_token
        {
            return Err(unknown("actual compound Raw grant scope"));
        }
        Ok(Scope {
            session: &self.current.session_id,
            run: &self.current.id,
        })
    }
    fn transition(&self, proof: &mut Proof) -> Result<(), Failure> {
        let (owner, lease) = self.input.bookkeeping();
        proof.measure(self.previous)?;
        proof.measure(&(owner, &self.current.claim_token))?;
        let mut expected = self.previous.clone();
        owner.clone_into(&mut expected.owner);
        expected.claim_token.clone_from(&self.current.claim_token);
        expected.status = crabber_core::RunStatus::Running;
        expected.updated_at = self.current.updated_at;
        expected.lease_until = expected
            .updated_at
            .checked_add(time::Duration::milliseconds(
                i64::try_from(lease).map_err(unknown)?,
            ))
            .ok_or_else(|| unknown("compound lease overflow"))?;
        if expected != *self.current
            || lease == 0
            || self.previous.claim_token == self.current.claim_token
        {
            return Err(unknown("actual compound owner replacement"));
        }
        proof.measure(&(&self.publication.issuer, &self.publication.source))?;
        self.publication
            .issuer
            .verify(self.current)
            .map_err(unknown)?;
        if self.publication.issuer.completion_source.as_ref() != self.completion
            || self.completion.is_some_and(|s| {
                s.native_operation != self.input.operation()
                    || s.native_semantic_digest != self.receipt.semantic_digest
            })
        {
            return Err(unknown("actual compound completion publisher"));
        }
        if let Some(source) = &self.publication.source {
            source.verify(&self.publication.issuer).map_err(unknown)?;
        } else if self.completion.is_some() {
            return Err(unknown("prepared compound original source"));
        }
        Ok(())
    }
    fn parent(&self, proof: &mut Proof) -> Result<(), Failure> {
        measure_record(self.previous_parent, proof)?;
        measure_record(self.parent, proof)?;
        measure_authority(Some(&self.grant.authority), proof)?;
        self.previous_parent.validate().map_err(unknown)?;
        self.parent.validate().map_err(unknown)?;
        let prior = &self.previous_parent.snapshot;
        let current = &self.parent.snapshot;
        let recovery = self.input.target().is_some();
        let expected_id = crate::tool_continuation::derived_id(
            if recovery {
                "tool-continuation-reclaim-attempt"
            } else {
                "tool-continuation-claim-attempt"
            },
            self.input.id(),
        );
        if self.native_request.id != expected_id
            || self.native_request.command.target() != &current.target
            || current.target != self.record.batch.next_attempt
            || current.session_id != self.current.session_id
            || current.run_id != self.current.id
            || current.state != crate::AttemptState::Unstarted
            || current.cleanup != crate::AttemptCleanup::Pending
            || self.parent.owner != self.current.owner
            || self.parent.run_token != self.current.claim_token
            || self.parent.lease_until != self.current.lease_until
            || !self.grant.authority.matches_store(
                current,
                &self.parent.token,
                &self.parent.run_token,
            )
        {
            return Err(unknown("actual compound parent authority"));
        }
        let valid = match (self.input.target(), &self.native_request.command) {
            (Some(target), crate::AttemptCommand::ReclaimUnstarted { .. }) => {
                prior == current
                    && &current.target == target
                    && self.previous_parent.token != self.parent.token
                    && self.previous_parent.lease_until <= self.current.updated_at
            }
            (None, crate::AttemptCommand::Reserve { .. }) => {
                prior.target == self.record.batch.attempt
                    && prior.target.generation.next().ok() == Some(current.target.generation)
                    && prior.state == crate::AttemptState::Suspended
                    && prior.cleanup == crate::AttemptCleanup::Complete
            }
            _ => false,
        };
        if !valid {
            return Err(unknown("actual compound native parent transition"));
        }
        Ok(())
    }
    fn targets(&self, proof: &mut Proof) -> Result<(), Failure> {
        let count = self
            .grant
            .phases
            .len()
            .checked_add(self.grant.frontend.len())
            .ok_or_else(|| unknown("compound phase count"))?;
        if count > 64
            || count != self.phases.len()
            || self.grant.phases.len() != self.record.batch.phases.len()
            || self.grant.frontend.len() != self.record.batch.frontend.len()
        {
            return Err(unknown("complete compound phase set"));
        }
        for (i, native) in self.phases.iter().enumerate() {
            measure_phase_record(native, proof)?;
            native.validate().map_err(unknown)?;
            let (target, binding, authority) = if i < self.grant.phases.len() {
                let g = &self.grant.phases[i];
                (
                    &g.target,
                    &self.record.batch.phases[i].call_binding,
                    &g.authority,
                )
            } else {
                let index = i - self.grant.phases.len();
                let g = &self.grant.frontend[index];
                (
                    &g.target,
                    &self.record.batch.frontend[index].call_binding,
                    &g.authority,
                )
            };
            measure_phase_authority(Some(authority), proof)?;
            if self.phases[..i].iter().any(|p| {
                p.snapshot.target.id == native.snapshot.target.id
                    || p.snapshot.target.call_id == native.snapshot.target.call_id
            }) || native.snapshot.target != *target
                || native.snapshot.target.attempt != self.parent.snapshot.target
                || native.snapshot.call_binding != *binding
                || native.snapshot.session_id != self.current.session_id
                || native.snapshot.run_id != self.current.id
                || native.snapshot.state != crate::PhaseState::Claimed
                || native.snapshot.quiescence != crate::PhaseQuiescence::Pending
                || native.output.is_some()
                || !authority.matches_store(&native.snapshot, &native.token)
                || !authority.matches_attempt_store(
                    &self.parent.snapshot,
                    &self.parent.token,
                    &self.parent.run_token,
                )
            {
                return Err(unknown("actual complete compound phase authority"));
            }
        }
        Ok(())
    }
    fn payload(&self, proof: &mut Proof) -> Result<(), Failure> {
        use crate::session_owner::preparation::tool_driver::payload;
        if self.record.questions.len() > 64 {
            return Err(unknown("compound answer count"));
        }
        proof.charge(
            self.record
                .questions
                .len()
                .checked_mul(std::mem::size_of::<crate::continuation_input::Input<'_>>())
                .ok_or_else(|| unknown("compound answer references"))?,
        )?;
        let answers = self
            .record
            .questions
            .iter()
            .map(|q| {
                q.answer
                    .as_ref()
                    .map(crate::continuation_input::Input::ToolAnswer)
                    .ok_or_else(|| unknown("actual consumed answer"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let actual = payload::actual(self.grant, proof).map_err(unknown)?;
        let expected =
            payload::expected(&self.record.batch, &answers, self.input.frontend(), proof)
                .map_err(unknown)?;
        if actual != expected {
            return Err(unknown("actual full compound public grant data"));
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn test_digest(&self) -> crabber_core::InputFingerprint {
        let phases = self
            .phases
            .iter()
            .map(|p| (&p.snapshot, &p.token, &p.output))
            .collect::<Vec<_>>();
        super::contract::snapshot_digest(&(
            "crabber.native.raw-tool.test.v1",
            self.input.test_digest(),
            (self.previous, self.current, self.record),
            (
                &self.previous_parent.snapshot,
                &self.previous_parent.owner,
                &self.previous_parent.token,
                &self.previous_parent.run_token,
                self.previous_parent.lease_until,
            ),
            (
                &self.parent.snapshot,
                &self.parent.owner,
                &self.parent.token,
                &self.parent.run_token,
                self.parent.lease_until,
            ),
            self.native_request,
            phases,
            (&self.publication.issuer, &self.publication.source),
            self.completion,
            (self.receipt, self.event),
            crate::session_owner::preparation::tool_driver::payload::view(self.grant).unwrap(),
        ))
    }
}

/// Actual grant outputs and their complete private native records, captured at issuance.
pub(crate) struct IssuedPhases<T> {
    pub grants: Vec<T>,
    pub records: Vec<PhaseRecord>,
}
impl<T> IssuedPhases<T> {
    pub(crate) fn new(count: usize, proof: &mut Proof) -> Result<Self, Failure> {
        if count > 64 {
            return Err(unknown("actual compound phase bound"));
        }
        proof.charge(
            count
                .checked_mul(std::mem::size_of::<T>() + std::mem::size_of::<PhaseRecord>())
                .ok_or_else(|| unknown("actual phase vectors"))?,
        )?;
        Ok(Self {
            grants: Vec::with_capacity(count),
            records: Vec::with_capacity(count),
        })
    }
    pub(crate) fn empty() -> Self {
        Self {
            grants: Vec::new(),
            records: Vec::new(),
        }
    }
}
/// Move captured records into one exact ordered set; no record is copied here.
pub(crate) fn records(
    tool: Vec<PhaseRecord>,
    frontend: Vec<PhaseRecord>,
    proof: &mut Proof,
) -> Result<Vec<PhaseRecord>, Failure> {
    let count = tool
        .len()
        .checked_add(frontend.len())
        .filter(|n| *n <= 64)
        .ok_or_else(|| unknown("complete compound phase bound"))?;
    proof.charge(
        count
            .checked_mul(std::mem::size_of::<PhaseRecord>())
            .ok_or_else(|| unknown("compound record vector"))?,
    )?;
    let mut records = Vec::with_capacity(count);
    records.extend(tool);
    records.extend(frontend);
    Ok(records)
}
#[derive(Clone, Copy)]
pub(crate) struct ToolCompletion<'a> {
    pub input: ToolInput<'a>,
    pub previous: &'a Run,
    pub current: &'a Run,
    pub record: &'a ToolContinuationRecord,
    pub previous_parent: &'a AttemptRecord,
    pub parent: &'a AttemptRecord,
    pub native_request: &'a crate::AttemptMutation,
    pub phases: &'a [PhaseRecord],
    pub publication: &'a RecoveryPublication,
    pub completion: Option<&'a crate::native_parent::CompletionSource>,
}

pub(crate) struct ToolGrantParts {
    pub fence: crabber_core::RunFence,
    pub authority: crate::AttemptAuthority,
    pub phases: Vec<crate::ToolPhaseResume>,
    pub frontend: Vec<crate::FrontendPhaseResume>,
    pub receipt: crate::ToolContinuationReceipt,
}
impl ToolCompletion<'_> {
    pub(crate) fn issue(
        self,
        parts: ToolGrantParts,
        proof: &mut Proof,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        self.record.measure_grant_data(proof)?;
        self.record.claim_outcome(
            parts.fence,
            parts.authority,
            parts.phases,
            parts.frontend,
            crate::tool_continuation::ack::Completion::new(
                parts.receipt,
                (&self.parent.token, &self.parent.run_token),
                proof,
            ),
        )
    }
}

/// Real native grant components before the original outer audit is appended.
pub(crate) struct ToolGrantOutput {
    pub fence: crabber_core::RunFence,
    pub authority: crate::AttemptAuthority,
    pub phases: Vec<crate::ToolPhaseResume>,
    pub frontend: Vec<crate::FrontendPhaseResume>,
    pub digest: crabber_core::InputFingerprint,
}
pub(crate) struct ParentGrant {
    pub previous: AttemptRecord,
    pub parent: AttemptRecord,
    pub request: crate::AttemptMutation,
    pub authority: crate::AttemptAuthority,
}

impl ToolGrantOutput {
    pub(crate) fn new(
        current: &Run,
        authority: crate::AttemptAuthority,
        phases: Vec<crate::ToolPhaseResume>,
        frontend: Vec<crate::FrontendPhaseResume>,
        digest: crabber_core::InputFingerprint,
        proof: &mut Proof,
    ) -> Result<Self, crate::ToolContinuationError> {
        proof
            .measure(&(&current.id, &current.claim_token))
            .map_err(|_| crate::ToolContinuationError::UnknownStoreOutcome)?;
        Ok(Self {
            fence: crabber_core::RunFence {
                run_id: current.id.clone(),
                claim_token: current.claim_token.clone(),
            },
            authority,
            phases,
            frontend,
            digest,
        })
    }
}
