use super::{
    Error, ObserveSessionOwnerCancel, RequestSessionOwnerCancel, SessionOwnerCancelObservation,
    SessionOwnerCancellation,
};
use crate::session_owner::{
    SessionOwnerAuthority, SessionOwnerState, audit::RevisionState, record::Record,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Observed {
    pub request: ObserveSessionOwnerCancel,
    pub observed_at: OffsetDateTime,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Cancellation {
    pub request: RequestSessionOwnerCancel,
    pub requested_at: OffsetDateTime,
    pub observed: Option<Observed>,
}
impl Cancellation {
    pub fn summary(&self) -> SessionOwnerCancellation {
        SessionOwnerCancellation {
            request_id: self.request.id.clone(),
            requested_at: self.requested_at,
            observed: self
                .observed
                .as_ref()
                .map(|observed| SessionOwnerCancelObservation {
                    operation_id: observed.request.id.clone(),
                    observed_at: observed.observed_at,
                }),
        }
    }
    pub fn validate(&self, record: &Record) -> Result<(), Error> {
        self.request.fingerprint()?;
        if record.version != 2
            || self.request.session_id != record.snapshot.binding.session_id
            || self.request.target != record.snapshot.target
        {
            return Err(Error::UnknownStoreOutcome);
        }
        if let Some(observed) = &self.observed {
            observed.request.fingerprint()?;
            if observed.request.session_id != self.request.session_id
                || observed.request.target != self.request.target
                || observed.request.request_id != self.request.id
            {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        Ok(())
    }
}
impl Record {
    pub fn request_cancel(
        &mut self,
        request: &RequestSessionOwnerCancel,
        revision: RevisionState,
        now: OffsetDateTime,
    ) -> Result<(), Error> {
        self.validate()?;
        request.fingerprint()?;
        self.cancel_scope(&request.session_id, &request.target)?;
        let matches = match (revision, request.expected_revision) {
            (RevisionState::Absent(_), None) => true,
            (RevisionState::Present(actual), Some(expected)) => actual == expected,
            _ => false,
        };
        if !matches {
            return Err(Error::RevisionConflict);
        }
        if self.cancellation.is_some() {
            return Err(Error::CancellationPending);
        }
        self.cancellation = Some(Box::new(Cancellation {
            request: request.clone(),
            requested_at: now,
            observed: None,
        }));
        self.snapshot.cancellation = self.cancellation.as_ref().map(|value| value.summary());
        self.publish_change(now, true)
    }
    pub fn observe_cancel(
        &mut self,
        request: &ObserveSessionOwnerCancel,
        authority: &SessionOwnerAuthority,
        now: OffsetDateTime,
    ) -> Result<(), Error> {
        self.validate()?;
        request.fingerprint()?;
        self.cancel_scope(&request.session_id, &request.target)?;
        if !authority.authenticates(&request.session_id, &request.target, &self.token) {
            return Err(Error::StaleOwner);
        }
        let cancellation = self.cancellation.as_mut().ok_or(Error::InvalidTransition)?;
        if cancellation.request.id != request.request_id {
            return Err(Error::SemanticConflict);
        }
        if cancellation.observed.is_some() {
            return Err(Error::CancellationPending);
        }
        // This is signal acknowledgement only: do not inspect/extend a lease.
        cancellation.observed = Some(Observed {
            request: request.clone(),
            observed_at: now,
        });
        self.snapshot.cancellation = Some(cancellation.summary());
        self.publish_change(now, true)
    }
    pub fn pending_cancel(
        &self,
        authority: &SessionOwnerAuthority,
    ) -> Result<Option<SessionOwnerCancellation>, Error> {
        self.validate()?;
        if !authority.authenticates(
            &self.snapshot.binding.session_id,
            &self.snapshot.target,
            &self.token,
        ) {
            return Err(Error::StaleOwner);
        }
        Ok(self.snapshot.cancellation.clone())
    }
    fn cancel_scope(
        &self,
        session: &crabber_core::SessionId,
        target: &crate::session_owner::SessionOwnerTarget,
    ) -> Result<(), Error> {
        if *session != self.snapshot.binding.session_id {
            return Err(Error::NotFound);
        }
        if *target != self.snapshot.target {
            return Err(Error::GenerationConflict);
        }
        if self.snapshot.state == SessionOwnerState::Joined {
            return Err(Error::InvalidTransition);
        }
        Ok(())
    }
}
