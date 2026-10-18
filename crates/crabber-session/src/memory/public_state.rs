use super::{MemoryExecution, MemoryStore, OperationReceipt, State, bump_public_revision};
use crate::{
    PublicStateError as Error, PublicStateMutation, PublicStateReceipt, PublicStateSnapshot,
    public_state,
};
use crabber_core::{ControlId, SessionId, SessionRevision};
native_pending_initial_items! {
    #[path="public_state/source.rs"]
    mod source;
    #[cfg(test)]
    #[path="public_state/source_tests.rs"]
    mod source_tests;
}

impl MemoryStore {
    pub(super) fn read_public_state(
        &self,
        session: &SessionId,
    ) -> Result<PublicStateSnapshot, Error> {
        let state = self.state.lock().unwrap();
        if !state.sessions.contains_key(session) {
            return Err(Error::NotFound);
        }
        Ok(PublicStateSnapshot {
            session_id: session.clone(),
            revision: revision(&state, session),
            contents: state
                .public_contents
                .get(session)
                .cloned()
                .unwrap_or_default(),
        })
    }
    pub(super) fn public_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<PublicStateReceipt>, Error> {
        public_state::validate_id(id)?;
        let state = self.state.lock().unwrap();
        let receipt = match state.operation_receipts.get(&(session.clone(), id.clone())) {
            Some(OperationReceipt::PublicState(receipt)) => {
                if receipt.session_id != *session || receipt.operation_id != *id {
                    return Err(Error::UnknownStoreOutcome);
                }
                verify(&state, receipt)?;
                Some(receipt.clone())
            }
            Some(
                OperationReceipt::Fork(_)
                | OperationReceipt::RunControl(_)
                | OperationReceipt::FrontendStaging(_)
                | OperationReceipt::Queue(_)
                | OperationReceipt::Management(_)
                | OperationReceipt::Attempt(_)
                | OperationReceipt::Phase(_)
                | OperationReceipt::Approval(_)
                | OperationReceipt::ToolContinuation(_),
            ) => {
                return Err(Error::SemanticConflict);
            }
            None => None,
        };
        Ok(receipt)
    }
}
fn verify(state: &State, receipt: &PublicStateReceipt) -> Result<(), Error> {
    let event = state
        .events
        .get(receipt.event_cursor)
        .ok_or(Error::UnknownStoreOutcome)?;
    public_state::verify_receipt_event(receipt, event)?;
    if let Some(cursor) = public_state::before_event(event)? {
        public_state::verify_before_event(
            event,
            state.events.get(cursor).ok_or(Error::UnknownStoreOutcome)?,
        )?;
    }
    Ok(())
}
fn revision(state: &State, session: &SessionId) -> SessionRevision {
    state
        .public_revisions
        .get(session)
        .copied()
        .unwrap_or(SessionRevision::INITIAL)
}
impl MemoryExecution {
    pub(super) fn change_public_state(
        &self,
        mutation: PublicStateMutation,
    ) -> Result<PublicStateReceipt, Error> {
        public_state::validate_id(&mutation.id)?;
        let result = self.store.transact(|state| {
            let run = state
                .runs
                .get(&self.fence.run_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            if run.id != self.fence.run_id {
                return Err(Error::UnknownStoreOutcome);
            }
            super::credential_access::check(
                state,
                &run,
                &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
            )
            .map_err(public_state::unknown)?;
            let digest = mutation.fingerprint(&run.session_id, &run.id)?;
            let key = (run.session_id.clone(), mutation.id.clone());
            if let Some(saved) = state.operation_receipts.get(&key) {
                let OperationReceipt::PublicState(receipt) = saved else {
                    return Err(Error::SemanticConflict);
                };
                if receipt.session_id != run.session_id || receipt.operation_id != mutation.id {
                    return Err(Error::UnknownStoreOutcome);
                }
                verify(state, receipt)?;
                if receipt.run_id != run.id || receipt.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(receipt.clone());
            }
            public_state::verify_owner(&run, &self.fence, self.store.clock.now())?;
            super::admission_journal::verify(state, &run.id)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            if state
                .admission_executions
                .get(&run.id)
                .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
            {
                return Err(Error::StaleOwner);
            }
            let current = revision(state, &run.session_id);
            if current != mutation.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let next = current.next().map_err(|_| Error::RevisionExhausted)?;
            let mut event = public_state::event(&run, &mutation, next, self.store.clock.now());
            let cursor = state
                .events
                .next_cursor()
                .map_err(|_| Error::UnknownStoreOutcome)?;
            event.cursor = Some(cursor);
            state
                .public_contents
                .insert(run.session_id.clone(), mutation.contents);
            bump_public_revision(state, &run.session_id).map_err(public_state::unknown)?;
            state
                .events
                .append(event)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            let receipt = PublicStateReceipt {
                session_id: run.session_id,
                run_id: run.id,
                operation_id: mutation.id,
                semantic_digest: digest,
                revision: next,
                event_cursor: cursor,
            };
            #[cfg(test)]
            if self
                .store
                .public_state_fault
                .load(std::sync::atomic::Ordering::SeqCst)
                == 1
            {
                return Err(Error::UnknownStoreOutcome);
            }
            state
                .operation_receipts
                .insert(key, OperationReceipt::PublicState(receipt.clone()));
            Ok(receipt)
        });
        #[cfg(test)]
        if self
            .store
            .public_state_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
            && result.is_ok()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn exhausted_revision_leaves_no_partial_write() {
        let store = super::MemoryStore::new();
        let admitted =
            crate::public_state_contract::admit(&store, time::OffsetDateTime::now_utc()).await;
        store.state.lock().unwrap().public_revisions.insert(
            admitted.session.id.clone(),
            crabber_core::SessionRevision::new(i64::MAX as u64).unwrap(),
        );
        crate::public_state_contract::exhausted(&store, admitted).await;
    }
    #[tokio::test]
    async fn memory_public_state_fault_contract() {
        let store = super::MemoryStore::new();
        crate::public_state_contract::run_fault_contract(&store, |point| {
            store
                .public_state_fault
                .store(point as u8, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
    }
}
