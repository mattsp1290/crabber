//! Atomic service source registration and capability-only existing access.
use super::{Error, MemoryStore, before, bump_public_revision, fault, revision};
use crate::session_owner::service::{
    RegisterSessionServiceDependency, SessionServiceDependencyAccess,
    SessionServiceDependencyOutcome,
};
impl MemoryStore {
    pub(in crate::memory) fn register_service(
        &self,
        request: &RegisterSessionServiceDependency,
    ) -> Result<SessionServiceDependencyOutcome, Error> {
        let session = &request
            .grant
            .original_reservation()
            .owner
            .binding
            .session_id;
        let outcome = self.transact(|state| {
            super::super::deletion::ensure_live(state, session)
                .map_err(crate::deletion::owner_error)?;
            if let Some(receipt) = state.session_owners.service_receipt(request)? {
                return Ok(SessionServiceDependencyOutcome::Replayed(Box::new(receipt)));
            }
            let now = self.clock.now();
            super::validate_preparation_source(state, request.grant.original_reservation(), now)?;
            let current = revision(state, session);
            let outcome = state
                .session_owners
                .register_service(request, current, now)?;
            if state.sessions.contains_key(session) {
                for _ in 0..2 {
                    bump_public_revision(state, session).map_err(|_| Error::UnknownStoreOutcome)?;
                }
            }
            before(fault(self))?;
            Ok(outcome)
        })?;
        if matches!(outcome, SessionServiceDependencyOutcome::Applied { .. }) && fault(self) == 2 {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(outcome)
    }

    pub(in crate::memory) fn check_service_access(
        &self,
        request: &SessionServiceDependencyAccess,
    ) -> Result<(), Error> {
        let state = self.state.lock().unwrap();
        let session = &request
            .grant
            .original_reservation()
            .owner
            .binding
            .session_id;
        super::super::deletion::ensure_live(&state, session)
            .map_err(crate::deletion::owner_error)?;
        let now = self.clock.now();
        super::validate_preparation_source(&state, request.grant.original_reservation(), now)?;
        state
            .session_owners
            .check_service_access(request, revision(&state, session), now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use crate::session_owner_contract::services::{activated_setup, generation};
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn service_compound_faults_preserve_original_metadata_without_authority() {
        let (store, grant) = activated_setup().await;
        let request = RegisterSessionServiceDependency {
            grant,
            generation: generation(),
        };
        let source = crate::session_owner::service::SessionServiceLink::for_grant(
            &request.grant,
            request.generation.clone(),
        )
        .unwrap();
        let reserve = source.identities().unwrap().reserve;
        store.session_owner_fault.store(1, Ordering::SeqCst);
        assert!(matches!(
            store
                .register_session_service_dependency(request.clone())
                .await,
            Err(Error::UnknownStoreOutcome)
        ));
        assert!(
            store
                .lookup_session_owner(&source.root.session_id, &reserve)
                .await
                .unwrap()
                .is_none()
        );
        store.session_owner_fault.store(2, Ordering::SeqCst);
        assert!(matches!(
            store
                .register_session_service_dependency(request.clone())
                .await,
            Err(Error::UnknownStoreOutcome)
        ));
        // The fault remains armed: a metadata retry cannot signal a new write
        // or supply the lost authority needed to expose the generation.
        let SessionServiceDependencyOutcome::Replayed(receipt) = store
            .register_session_service_dependency(request.clone())
            .await
            .unwrap()
        else {
            panic!("metadata only after lost ACK")
        };
        receipt.verify().unwrap();
        store
            .state
            .lock()
            .unwrap()
            .session_owners
            .erase_historical_audit_for_test(&source.root.session_id, &receipt.began.operation_id);
        assert!(matches!(
            store.register_session_service_dependency(request).await,
            Err(Error::UnknownStoreOutcome)
        ));
    }
}
