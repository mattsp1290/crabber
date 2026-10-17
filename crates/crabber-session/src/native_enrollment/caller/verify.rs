//! Shared borrowed Raw caller verification; opaque grants use their real validation SPI.
use super::measure_authority;
use crate::attempt::AttemptRecord;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::{AttemptAuthority, AttemptCommand, AttemptMutation, AttemptReceipt, AttemptState};
use crabber_core::{EventRecord, Run, RunId, SessionId};

#[derive(Clone, Copy)]
pub(crate) enum Caller {
    Raw,
}

pub(crate) struct Scope<'a> {
    pub session: &'a SessionId,
    pub run: &'a RunId,
}

// Only implemented actual families belong here. Other raw writers are not yet
// inventoried by this representative; no Pending caller can be constructed.
pub(crate) enum Fresh<'a> {
    Attempt(Attempt<'a>),
    LegacyTool(super::LegacyTool<'a>),
    AdmissionBegin(super::AdmissionBegin<'a>),
    AdmissionRecovery(super::recovery::AdmissionRecovery<'a>),
    LegacyExpired(super::recovery::LegacyExpired<'a>),
    ExpiredRecovery(super::recovery::ExpiredRecovery<'a>),
    Phase(Phase<'a>),
    Initial(super::initial::Initial<'a>),
}

pub(crate) struct Attempt<'a> {
    pub run: &'a Run,
    pub request: &'a AttemptMutation,
    pub record: &'a AttemptRecord,
    pub receipt: &'a AttemptReceipt,
    pub event: &'a EventRecord,
    pub issued: Option<&'a AttemptAuthority>,
}

pub(crate) fn relevant(command: &AttemptCommand) -> bool {
    matches!(
        command,
        AttemptCommand::Reserve { .. }
            | AttemptCommand::ReserveBound { .. }
            | AttemptCommand::ReclaimUnstarted { .. }
            | AttemptCommand::Begin { .. }
    )
}

pub(crate) fn measure_record(record: &AttemptRecord, proof: &mut Proof) -> Result<(), Failure> {
    proof.measure(&(
        &record.snapshot,
        &record.owner,
        &record.token,
        &record.run_token,
        record.lease_until,
    ))
}

impl<'a> Fresh<'a> {
    pub(crate) fn measure_and_verify(&self, proof: &mut Proof) -> Result<Scope<'a>, Failure> {
        match self {
            Self::Attempt(fresh) => fresh.verify(proof),
            Self::LegacyTool(fresh) => fresh.verify(proof),
            Self::AdmissionBegin(fresh) => fresh.verify(proof),
            Self::AdmissionRecovery(fresh) => fresh.verify(proof),
            Self::LegacyExpired(fresh) => fresh.verify(proof),
            Self::ExpiredRecovery(fresh) => fresh.verify(proof),
            Self::Phase(fresh) => fresh.verify(proof),
            Self::Initial(fresh) => fresh.verify(proof),
        }
    }
}
impl<'a> Attempt<'a> {
    fn verify(&self, proof: &mut Proof) -> Result<Scope<'a>, Failure> {
        proof.measure(&(self.run, self.request, self.receipt, self.event))?;
        measure_record(self.record, proof)?;
        measure_authority(self.issued, proof)?;
        self.record.validate().map_err(unknown)?;
        crate::attempt::verify_receipt(self.receipt, self.event).map_err(unknown)?;
        let begin = matches!(self.request.command, AttemptCommand::Begin { .. });
        if !relevant(&self.request.command)
            || self.run.id != self.record.snapshot.run_id
            || self.run.session_id != self.record.snapshot.session_id
            || self.record.snapshot != self.receipt.attempt
            || self.record.snapshot.target != *self.request.command.target()
            || self.receipt.operation_id != self.request.id
            || self.receipt.semantic_digest
                != self
                    .request
                    .fingerprint(&self.run.session_id, &self.run.id)
                    .map_err(unknown)?
            || self.record.run_token != self.run.claim_token
            || self.record.snapshot.state
                != if begin {
                    AttemptState::Started
                } else {
                    AttemptState::Unstarted
                }
            || begin == self.issued.is_some()
            || self.issued.is_some_and(|a| {
                !a.matches_store(
                    &self.record.snapshot,
                    &self.record.token,
                    &self.record.run_token,
                )
            })
        {
            return Err(unknown("actual raw Attempt result"));
        }
        Ok(Scope {
            session: &self.run.session_id,
            run: &self.run.id,
        })
    }
}

#[path = "phase.rs"]
mod phase;
pub(crate) use phase::Phase;
