use super::{
    Error, Ledger, OffsetDateTime, Publication, RevisionState, SessionOwnerAuthority,
    SessionOwnerOperation, SessionOwnerOutcome,
};
use crate::session_owner::{
    ObserveSessionOwnerCancel, RequestSessionOwnerCancel, SessionOwnerCancellation,
};
impl Ledger {
    pub fn request_cancel(
        &mut self,
        request: &RequestSessionOwnerCancel,
        revision: RevisionState,
        now: OffsetDateTime,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        if let Some(saved) = self.receipt(&request.session_id, &request.id)? {
            if saved.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            return Ok(SessionOwnerOutcome::Replayed(saved));
        }
        self.validate_scope(&request.session_id)?;
        let mut record = self
            .checked(&request.session_id, &request.target.id)?
            .clone();
        record.request_cancel(request, revision, now)?;
        let receipt = record.receipt(
            request.id.clone(),
            digest,
            SessionOwnerOperation::RequestCancel,
        );
        self.publish(record, receipt.clone(), Publication::new(revision, true)?)?;
        Ok(SessionOwnerOutcome::Applied {
            receipt,
            authority: None,
        })
    }
    pub fn observe_cancel(
        &mut self,
        request: &ObserveSessionOwnerCancel,
        authority: &SessionOwnerAuthority,
        revision: RevisionState,
        now: OffsetDateTime,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        if let Some(saved) = self.receipt(&request.session_id, &request.id)? {
            if saved.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            return Ok(SessionOwnerOutcome::Replayed(saved));
        }
        self.validate_scope(&request.session_id)?;
        let mut record = self
            .checked(&request.session_id, &request.target.id)?
            .clone();
        record.observe_cancel(request, authority, now)?;
        let receipt = record.receipt(
            request.id.clone(),
            digest,
            SessionOwnerOperation::ObserveCancel,
        );
        self.publish(record, receipt.clone(), Publication::new(revision, true)?)?;
        Ok(SessionOwnerOutcome::Applied {
            receipt,
            authority: None,
        })
    }
    pub fn pending_cancel(
        &self,
        authority: &SessionOwnerAuthority,
    ) -> Result<Option<SessionOwnerCancellation>, Error> {
        self.checked(authority.session_id(), &authority.target().id)?
            .pending_cancel(authority)
    }
}
