//! Exact original initial parent reservation. Never infers no Begin from state.
use super::{Error, ExpiredAdmissionOrigin, ExpiredRunOrigin};
use crate::{AttemptCleanup, AttemptCommand, AttemptReceipt, AttemptState};
use crabber_core::{AttemptGeneration, EventRecord, InputFingerprint, Run};

pub(crate) struct Original {
    pub receipt: AttemptReceipt,
    pub event: EventRecord,
}
impl Original {
    pub(crate) fn verify(&self) -> Result<(), Error> {
        if crate::snapshot::encoded_bytes(&self.receipt, 8192).is_none()
            || crate::snapshot::encoded_bytes(&self.event, 65536).is_none()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        crate::attempt::verify_receipt(&self.receipt, &self.event).map_err(unknown)?;
        let command: AttemptCommand = serde_json::from_value(
            self.event
                .payload
                .get("command")
                .ok_or(Error::UnknownStoreOutcome)?
                .clone(),
        )
        .map_err(unknown)?;
        if !matches!(command, AttemptCommand::ReserveBound { .. })
            || self.receipt.attempt.state != AttemptState::Unstarted
            || self.receipt.attempt.cleanup != AttemptCleanup::Pending
            || self.receipt.attempt.target.generation != AttemptGeneration::INITIAL
            || self.event.cursor != Some(self.receipt.event_cursor)
            || self.event.turn_id.is_some()
        {
            return Err(Error::Unsupported);
        }
        Ok(())
    }
    /// The adapter must independently refuse any retained native Begin witness
    /// and any execution history beyond this exact original `ReserveBound`.
    pub(crate) fn verify_record(
        &self,
        run: &Run,
        record: &crate::attempt::AttemptRecord,
        now: time::OffsetDateTime,
    ) -> Result<(), Error> {
        self.verify()?;
        record.validate().map_err(unknown)?;
        if record.snapshot != self.receipt.attempt
            || record.snapshot.run_id != run.id
            || record.snapshot.session_id != run.session_id
            || record.run_token != run.claim_token
            || record.owner != run.owner
            || record.lease_until > now
        {
            return Err(Error::StaleOwner);
        }
        Ok(())
    }
    pub(crate) fn digest(&self, initial: &InputFingerprint) -> Result<InputFingerprint, Error> {
        self.verify()?;
        crate::session_owner::digest(&(
            "crabber.preparation.expired-initial-unstarted.v1",
            initial,
            &self.receipt,
        ))
    }
    pub(crate) fn origin(&self, initial: ExpiredRunOrigin) -> Result<ExpiredRunOrigin, Error> {
        self.verify()?;
        let admission = match initial {
            ExpiredRunOrigin::KeyedInitial(key) => ExpiredAdmissionOrigin::Keyed(key),
            ExpiredRunOrigin::UnkeyedAdmission(id) => ExpiredAdmissionOrigin::Unkeyed(id),
            _ => return Err(Error::Unsupported),
        };
        Ok(ExpiredRunOrigin::InitialUnstarted {
            admission,
            reservation: self.receipt.operation_id.clone(),
            revision: self.receipt.revision,
        })
    }
}
pub(crate) fn admission(origin: &ExpiredAdmissionOrigin) -> ExpiredRunOrigin {
    match origin {
        ExpiredAdmissionOrigin::Keyed(key) => ExpiredRunOrigin::KeyedInitial(key.clone()),
        ExpiredAdmissionOrigin::Unkeyed(id) => ExpiredRunOrigin::UnkeyedAdmission(id.clone()),
    }
}
pub(crate) fn validate_capture(source: &super::ExpiredRunPreparation) -> Result<(), Error> {
    if let ExpiredRunOrigin::InitialUnstarted { revision, .. } = source.target().origin
        && revision != source.expected_revision()
    {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
