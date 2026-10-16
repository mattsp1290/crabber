use super::{Error, MemoryStore, SessionOwnerAuthority, SessionOwnerOutcome, revision};
use crate::session_owner::{
    ObserveSessionOwnerCancel, RequestSessionOwnerCancel, SessionOwnerCancellation,
};
impl MemoryStore {
    pub(in crate::memory) fn request_owner_cancel(
        &self,
        request: &RequestSessionOwnerCancel,
    ) -> Result<SessionOwnerOutcome, Error> {
        request.fingerprint()?;
        let fault = super::fault(self);
        let outcome = self.transact(|state| {
            super::super::deletion::ensure_live(state, &request.session_id)
                .map_err(crate::deletion::owner_error)?;
            let before = revision(state, &request.session_id);
            let outcome = state
                .session_owners
                .request_cancel(request, before, self.clock.now())?;
            if matches!(outcome, SessionOwnerOutcome::Applied { .. }) {
                if state.sessions.contains_key(&request.session_id) {
                    super::bump_public_revision(state, &request.session_id)
                        .map_err(|_| Error::UnknownStoreOutcome)?;
                }
                super::before(fault)?;
            }
            Ok(outcome)
        })?;
        super::after(outcome, fault)
    }
    pub(in crate::memory) fn observe_owner_cancel(
        &self,
        request: &ObserveSessionOwnerCancel,
        authority: &SessionOwnerAuthority,
    ) -> Result<SessionOwnerOutcome, Error> {
        request.fingerprint()?;
        let fault = super::fault(self);
        let outcome = self.transact(|state| {
            super::super::deletion::ensure_live(state, &request.session_id)
                .map_err(crate::deletion::owner_error)?;
            let before = revision(state, &request.session_id);
            let outcome = state.session_owners.observe_cancel(
                request,
                authority,
                before,
                self.clock.now(),
            )?;
            if matches!(outcome, SessionOwnerOutcome::Applied { .. }) {
                if state.sessions.contains_key(&request.session_id) {
                    super::bump_public_revision(state, &request.session_id)
                        .map_err(|_| Error::UnknownStoreOutcome)?;
                }
                super::before(fault)?;
            }
            Ok(outcome)
        })?;
        super::after(outcome, fault)
    }
    pub(in crate::memory) fn pending_owner_cancel(
        &self,
        authority: &SessionOwnerAuthority,
    ) -> Result<Option<SessionOwnerCancellation>, Error> {
        let state = self.state.lock().unwrap();
        super::super::deletion::ensure_live(&state, authority.session_id())
            .map_err(crate::deletion::owner_error)?;
        state.session_owners.pending_cancel(authority)
    }
}
