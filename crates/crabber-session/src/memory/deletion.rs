//! Permanent identity checks under the ordinary Memory session gate.
use super::{MemoryStore, State};
use crate::{DeletionError, DeletionReceipt, SessionDisposition, StoreError};
use crabber_core::{ControlId, SessionId, SessionRevision};

pub(super) fn ensure_live(state: &State, session: &SessionId) -> Result<(), StoreError> {
    if let Some(tombstone) = state.tombstones.get(session) {
        tombstone
            .verify(session)
            .map_err(|_| StoreError::Validation("session deletion evidence unavailable".into()))?;
        return Err(StoreError::SessionDeleted);
    }
    Ok(())
}

impl MemoryStore {
    pub(super) fn deletion_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<DeletionReceipt>, DeletionError> {
        crate::management::validate_scope(session, id)
            .map_err(|_| DeletionError::InvalidRequest)?;
        let state = self.state.lock().expect("memory store poisoned");
        let Some(tombstone) = state.tombstones.get(session) else {
            return Ok(None);
        };
        tombstone.verify(session)?;
        if state.sessions.contains_key(session) {
            return Err(DeletionError::UnknownStoreOutcome);
        }
        if tombstone.receipt.operation_id != *id {
            return Err(DeletionError::Deleted);
        }
        Ok(Some(tombstone.receipt.clone()))
    }

    pub(super) fn disposition(
        &self,
        session: &SessionId,
    ) -> Result<SessionDisposition, DeletionError> {
        crate::deletion::validate_session(session)?;
        let state = self.state.lock().expect("memory store poisoned");
        if let Some(tombstone) = state.tombstones.get(session) {
            tombstone.verify(session)?;
            if state.sessions.contains_key(session) {
                return Err(DeletionError::UnknownStoreOutcome);
            }
            return Ok(SessionDisposition::Deleted {
                final_revision: tombstone.receipt.final_revision,
                deleted_at: tombstone.receipt.deleted_at,
            });
        }
        Ok(if state.sessions.contains_key(session) {
            SessionDisposition::Present {
                revision: state
                    .public_revisions
                    .get(session)
                    .copied()
                    .unwrap_or(SessionRevision::INITIAL),
            }
        } else {
            // Absence says nothing about pre-session preparation ownership.
            SessionDisposition::Missing
        })
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn permanent_identity_guards() {
        let store = super::MemoryStore::new();
        let tombstone = crate::deletion::contract::deleted();
        store
            .state
            .lock()
            .unwrap()
            .tombstones
            .insert(tombstone.receipt.session_id.clone(), tombstone.clone());
        crate::deletion::contract::metadata(&store, &tombstone).await;
        crate::deletion::contract::creation_guards(&store, &tombstone).await;
        store
            .state
            .lock()
            .unwrap()
            .tombstones
            .get_mut(&tombstone.receipt.session_id)
            .unwrap()
            .expected_revision = tombstone.receipt.final_revision;
        assert_eq!(
            store
                .deletion_receipt(
                    &tombstone.receipt.session_id,
                    &tombstone.receipt.operation_id
                )
                .unwrap_err(),
            crate::DeletionError::UnknownStoreOutcome
        );
    }
}
