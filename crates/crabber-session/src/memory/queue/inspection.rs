use super::{
    ControlId, MemoryStore, QueueAuthority, QueueState, RetainedQueue, SessionRevision, StoreError,
    head, preflight, protocol, revision_of,
};
impl MemoryStore {
    pub(in crate::memory) fn queue_owned_inspection(
        &self,
        context: &crabber_core::SessionContext,
        authority: &QueueAuthority,
        expected_revision: SessionRevision,
    ) -> Result<RetainedQueue, StoreError> {
        if context.session_id() != &authority.session_id {
            return Err(StoreError::SessionIdentityMismatch);
        }
        crate::queue::scope(context.session_id(), &ControlId("inspect".into()))?;
        crate::queue::identifier(&authority.queue_id.0)?;
        let state = self.state.lock().expect("memory store poisoned");
        crate::memory::deletion::ensure_live(&state, context.session_id())?;
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        preflight(&state, context.session_id(), &mut proof)?;
        let session = state
            .sessions
            .get(context.session_id())
            .ok_or(StoreError::NotFound)?;
        session
            .ensure_identity(context.workspace_id(), context.directory())
            .map_err(|_| StoreError::SessionIdentityMismatch)?;
        if state
            .management
            .get(context.session_id())
            .is_some_and(|m| m.archived)
        {
            return Err(StoreError::Conflict);
        }
        if revision_of(&state, context.session_id()) != expected_revision {
            return Err(StoreError::AdmissionRevisionConflict);
        }
        let scope = crate::queue::source::Scope {
            session: authority.session_id.clone(),
            queue: authority.queue_id.clone(),
        };
        let saved = protocol::record(&state, &scope, &mut proof).map_err(protocol::domain)?;
        if !state
            .queue_publications
            .protocol(&scope, &mut proof)
            .map_err(protocol::domain)?
        {
            return Err(StoreError::HostUnsupported);
        }
        let claim = saved.claim.as_ref().ok_or(StoreError::Conflict)?;
        if saved.summary.state != QueueState::Claimed
            || claim.authority != *authority
            || claim.expires <= self.clock.now()
            || head(&state, &authority.session_id)?
                .is_none_or(|head| head.queue_id != authority.queue_id)
        {
            return Err(StoreError::Conflict);
        }
        if crate::memory::has_pending_continuation(&state, &authority.session_id)?
            || state
                .runs
                .values()
                .any(|run| run.session_id == authority.session_id && !run.status.is_terminal())
        {
            return Err(StoreError::Busy);
        }
        saved.retained(&mut proof).map_err(protocol::domain)
    }
}
