//! Actual phase Claim/Begin and atomic Start; no synthetic nested source.
use super::super::{measure_phase_authority, measure_phase_record};
use super::{Scope, measure_record};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::{PhaseAuthority, PhaseCommand, PhaseMutation, PhaseReceipt, PhaseState};
use crabber_core::{EventRecord, Run, ToolCallRecord, ToolCallStatus};

pub(crate) struct Phase<'a> {
    pub run: &'a Run,
    pub parent: &'a crate::attempt::AttemptRecord,
    pub call: &'a ToolCallRecord,
    pub request: &'a PhaseMutation,
    pub effect: crate::phase::PhaseEffect<'a>,
    pub record: &'a crate::phase::PhaseRecord,
    pub receipt: &'a PhaseReceipt,
    pub event: &'a EventRecord,
    pub running_event: Option<&'a EventRecord>,
    pub issued: Option<&'a PhaseAuthority>,
}
impl<'a> Phase<'a> {
    pub(super) fn verify(&self, proof: &mut Proof) -> Result<Scope<'a>, Failure> {
        proof.measure(&(
            self.run,
            self.call,
            self.request,
            self.receipt,
            self.event,
            self.running_event,
        ))?;
        measure_record(self.parent, proof)?;
        measure_phase_record(self.record, proof)?;
        measure_phase_authority(self.issued, proof)?;
        match self.effect {
            crate::phase::PhaseEffect::Metadata if self.running_event.is_none() => (),
            crate::phase::PhaseEffect::Start(start) => {
                proof.measure(&(&start.begin, &start.turn_id, start.created_at))?;
                let actual = self
                    .running_event
                    .ok_or_else(|| unknown("missing actual phase Start event"))?;
                let mut expected = start.materialize(self.run, self.call).map_err(unknown)?;
                expected.cursor = actual.cursor;
                if expected != *actual
                    || actual.cursor.is_none()
                    || start.begin.id != self.request.id
                    || start.begin.expected_revision != self.request.expected_revision
                    || start.begin.command.target() != self.request.command.target()
                    || !matches!(self.request.command, PhaseCommand::Begin { .. })
                {
                    return Err(unknown("actual phase Start event"));
                }
            }
            _ => return Err(unknown("raw phase effect")),
        }
        self.record.validate().map_err(unknown)?;
        self.parent.validate().map_err(unknown)?;
        crate::phase::verify_receipt(self.receipt, self.event).map_err(unknown)?;
        let claim = matches!(self.request.command, PhaseCommand::Claim { .. });
        if !matches!(
            self.request.command,
            PhaseCommand::Claim { .. } | PhaseCommand::Begin { .. }
        ) || self.run.id != self.record.snapshot.run_id
            || self.run.session_id != self.record.snapshot.session_id
            || self.record.snapshot != self.receipt.phase
            || self.record.snapshot.target != *self.request.command.target()
            || self.parent.snapshot.target != self.record.snapshot.target.attempt
            || self.parent.snapshot.session_id != self.run.session_id
            || self.parent.snapshot.run_id != self.run.id
            || self.parent.run_token != self.run.claim_token
            || self.call.run_id != self.run.id
            || self.call.id != self.record.snapshot.target.call_id
            || self.call.status
                != if claim {
                    ToolCallStatus::Pending
                } else {
                    ToolCallStatus::Running
                }
            || crate::phase::call_binding(self.call).map_err(unknown)?
                != self.record.snapshot.call_binding
            || self.receipt.operation_id != self.request.id
            || self.receipt.semantic_digest
                != self
                    .effect
                    .fingerprint(self.request, &self.run.session_id, &self.run.id)
                    .map_err(unknown)?
            || self.record.snapshot.state
                != if claim {
                    PhaseState::Claimed
                } else {
                    PhaseState::Started
                }
            || claim != self.issued.is_some()
            || self.issued.is_some_and(|a| {
                !a.matches_store(&self.record.snapshot, &self.record.token)
                    || !a.matches_attempt_store(
                        &self.parent.snapshot,
                        &self.parent.token,
                        &self.parent.run_token,
                    )
            })
        {
            return Err(unknown("actual raw Phase result"));
        }
        Ok(Scope {
            session: &self.run.session_id,
            run: &self.run.id,
        })
    }
}
