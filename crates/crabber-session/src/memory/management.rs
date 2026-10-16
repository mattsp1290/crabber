use super::{MemoryStore, OperationReceipt, State};
use crate::{
    ListSessions, ManageSession, ManagementError as Error, ManagementReceipt, SessionListPage,
    SessionSummary, management,
};
use crabber_core::{ControlId, SessionId, SessionRevision};
fn current(
    state: &State,
    id: &SessionId,
) -> Result<(crabber_core::Session, management::Metadata, SessionRevision), Error> {
    super::deletion::ensure_live(state, id).map_err(crate::deletion::management_error)?;
    let session = state.sessions.get(id).ok_or(Error::NotFound)?;
    let metadata = state.management.get(id).cloned().unwrap_or_default();
    management::validate_stored(session, &metadata).map_err(management::unknown)?;
    Ok((
        state.sessions.get(id).cloned().ok_or(Error::NotFound)?,
        state.management.get(id).cloned().unwrap_or_default(),
        state
            .public_revisions
            .get(id)
            .copied()
            .unwrap_or(SessionRevision::INITIAL),
    ))
}
fn receipt(
    state: &State,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<ManagementReceipt>, Error> {
    super::deletion::ensure_live(state, session).map_err(crate::deletion::management_error)?;
    match state.operation_receipts.get(&(session.clone(), id.clone())) {
        None => Ok(None),
        Some(OperationReceipt::Management(saved)) => {
            management::verify_audit(
                state
                    .management_audits
                    .get(&(session.clone(), id.clone()))
                    .ok_or(Error::UnknownStoreOutcome)?,
                saved,
            )?;
            Ok(Some(saved.clone()))
        }
        Some(_) => Err(Error::SemanticConflict),
    }
}
impl MemoryStore {
    pub(super) fn manage(&self, request: ManageSession) -> Result<ManagementReceipt, Error> {
        let digest = request.fingerprint()?;
        let result = self.transact(|state| {
            if let Some(saved) = receipt(state, &request.session_id, &request.id)? {
                if saved.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok((saved, false));
            }
            if let crate::SessionChange::Create {
                workspace_id,
                directory,
                ..
            } = &request.change
            {
                state
                    .session_owners
                    .context(&request.session_id, workspace_id, directory)
                    .map_err(|error| {
                        if error == crate::session_owner::SessionOwnerError::IdentityMismatch {
                            Error::SemanticConflict
                        } else {
                            Error::UnknownStoreOutcome
                        }
                    })?;
            }
            let existing = if state.sessions.contains_key(&request.session_id) {
                Some(current(state, &request.session_id)?)
            } else {
                None
            };
            let now = self.clock.now();
            let (session, metadata, revision) = management::apply(&request, existing, now)?;
            let saved = ManagementReceipt {
                session_id: session.id.clone(),
                operation_id: request.id.clone(),
                semantic_digest: digest,
                revision,
                summary: management::summary(&session, metadata.clone(), revision),
            };
            let key = (session.id.clone(), request.id.clone());
            state.sessions.insert(session.id.clone(), session);
            state
                .management
                .insert(request.session_id.clone(), metadata);
            state
                .public_revisions
                .insert(request.session_id.clone(), revision);
            state.management_audits.insert(
                key.clone(),
                management::Audit {
                    request,
                    receipt: saved.clone(),
                    created_at: now,
                },
            );
            #[cfg(test)]
            if self
                .management_fault
                .load(std::sync::atomic::Ordering::SeqCst)
                == 1
            {
                return Err(Error::UnknownStoreOutcome);
            }
            state
                .operation_receipts
                .insert(key, OperationReceipt::Management(saved.clone()));
            Ok((saved, true))
        });
        let (saved, changed) = result?;
        let _ = changed; // Production builds do not inject acknowledgement loss.
        #[cfg(test)]
        if self
            .management_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
            && changed
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(saved)
    }
    pub(super) fn management_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<ManagementReceipt>, Error> {
        management::validate_scope(session, id)?;
        receipt(&self.state.lock().unwrap(), session, id)
    }
    pub(super) fn read_summary(&self, id: &SessionId) -> Result<SessionSummary, Error> {
        let (session, metadata, revision) = current(&self.state.lock().unwrap(), id)?;
        Ok(management::summary(&session, metadata, revision))
    }
    pub(super) fn summaries(&self, request: &ListSessions) -> Result<SessionListPage, Error> {
        management::validate_list(request)?;
        let state = self.state.lock().unwrap();
        let mut sessions = Vec::new();
        for (id, session) in &state.sessions {
            if session.workspace_id != request.workspace_id
                || request
                    .cursor
                    .as_ref()
                    .is_some_and(|cursor| id <= &cursor.after)
            {
                continue;
            }
            let metadata = state.management.get(id).cloned().unwrap_or_default();
            if metadata.archived && !request.include_archived {
                continue;
            }
            management::validate_stored(session, &metadata).map_err(management::unknown)?;
            sessions.push(management::summary(
                session,
                metadata,
                state
                    .public_revisions
                    .get(id)
                    .copied()
                    .unwrap_or(SessionRevision::INITIAL),
            ));
            if sessions.len() > usize::from(request.limit) {
                break;
            }
        }
        Ok(management::list_page(request, sessions))
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn management_contract() {
        let store = super::MemoryStore::new();
        crate::management_contract::run_contract(&store).await;
    }
    #[tokio::test]
    async fn management_fault_contract() {
        let store = super::MemoryStore::new();
        crate::management_contract::faults(&store, |point| {
            store
                .management_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
    }
}
