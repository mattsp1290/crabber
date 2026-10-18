//! Borrow and authenticate before cloning public Raw credentials.
use super::{MemoryStore, State, native_enrollment};
use crate::StoreError;
use crate::native_enrollment::access::{allow, failure};
use crate::public_session::v2::proof::Proof;
use crabber_core::{Run, RunFence, RunId};

native_pending_initial_items! {
    #[cfg(test)]
    #[path = "credential_access/prefix_tests.rs"]
    mod prefix_tests;
}

pub(super) fn check(state: &State, run: &Run, proof: &mut Proof) -> Result<(), StoreError> {
    proof.measure(run).map_err(failure)?;
    check_origin(state, &run.session_id, &run.id, proof)
}

/// Legacy writer guards; these APIs have no enclosing operation Proof.
pub(super) fn check_bounded(state: &State, run: &Run) -> Result<(), StoreError> {
    check(state, run, &mut Proof::bounded(32 << 20))
}
pub(super) fn check_origin_bounded(
    state: &State,
    session: &crabber_core::SessionId,
    run: &RunId,
) -> Result<(), StoreError> {
    check_origin(state, session, run, &mut Proof::bounded(32 << 20))
}

pub(super) fn check_origin(
    state: &State,
    session: &crabber_core::SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<(), StoreError> {
    allow(native_enrollment::original(state, session, run, proof).map_err(failure)?)
}

impl MemoryStore {
    pub(super) fn raw_execution_access(&self, fence: &RunFence) -> Result<(), StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        let run = state.runs.get(&fence.run_id).ok_or(StoreError::NotFound)?;
        if run.id != fence.run_id {
            return Err(StoreError::Validation(
                "stored run identity is invalid".into(),
            ));
        }
        if run.claim_token != fence.claim_token
            || run.status.is_terminal()
            || run.lease_until <= self.clock.now()
        {
            return Err(StoreError::Conflict);
        }
        let mut proof = Proof::bounded(32 << 20);
        proof.measure(fence).map_err(failure)?;
        super::native_parent::current_issuance_with_proof(&state, run, &mut proof)
            .map_err(|_| StoreError::Validation("native run fence evidence unavailable".into()))?;
        check(&state, run, &mut proof)
    }

    pub(super) fn raw_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        let Some(run) = state.runs.get(id) else {
            return Ok(None);
        };
        if run.id != *id {
            return Err(StoreError::Validation(
                "stored run identity is invalid".into(),
            ));
        }
        check(&state, run, &mut Proof::bounded(32 << 20))?;
        Ok(Some(run.clone()))
    }

    pub(super) fn raw_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        let mut proof = Proof::bounded(32 << 20);
        for (id, run) in state
            .runs
            .iter()
            .filter(|(_, run)| !run.status.is_terminal())
        {
            if id != &run.id {
                return Err(StoreError::Validation(
                    "stored run identity is invalid".into(),
                ));
            }
            check(&state, run, &mut proof)?;
        }
        Ok(state
            .runs
            .values()
            .filter(|run| !run.status.is_terminal())
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    native_pending_initial_items! {
        use super::{MemoryStore, Proof, native_enrollment};
    }
    use crate::Store;
    use crate::native_enrollment::access::contract;

    #[async_trait::async_trait]
    impl crate::driver_raw_access_contract::Damage for crate::MemoryStore {
        type Saved = crate::native_enrollment::Unclassified;
        async fn damage(
            &self,
            session: &crabber_core::SessionId,
            run: &crabber_core::RunId,
        ) -> Self::Saved {
            self.state
                .lock()
                .unwrap()
                .execution_enrollment_unclassified
                .remove(&(session.clone(), run.clone()))
                .unwrap()
        }
        async fn restore(
            &self,
            session: &crabber_core::SessionId,
            run: &crabber_core::RunId,
            saved: Self::Saved,
        ) {
            self.state
                .lock()
                .unwrap()
                .execution_enrollment_unclassified
                .insert((session.clone(), run.clone()), saved);
        }
    }
    #[tokio::test]
    async fn raw_driver_registration_and_activation_authenticate_birth() {
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = crate::MemoryStore::with_clock(clock.clone());
        crate::driver_raw_access_contract::run(&store, &clock).await;
    }
    #[tokio::test]
    async fn public_raw_credentials_require_original_enrollment_before_return() {
        let store = crate::MemoryStore::new();
        let admitted = contract::admit(&store).await;
        let _other_scope = contract::admit(&store).await;
        contract::succeeds(&store, &admitted).await;
        let read = store.get_run(&admitted.run.id);
        let list = store.list_unfinished_runs();
        let acquire = store.execution(admitted.fence.clone());
        let scope = (admitted.session.id.clone(), admitted.run.id.clone());
        let original = store
            .state
            .lock()
            .unwrap()
            .execution_enrollment_unclassified
            .remove(&scope)
            .unwrap();
        // These futures were constructed before corruption and first polled after.
        // No individual credential or partial multi-scope list may escape.
        let refused = (
            read.await.is_err(),
            list.await.is_err(),
            acquire.await.is_err(),
        );
        store
            .state
            .lock()
            .unwrap()
            .execution_enrollment_unclassified
            .insert(scope, original);
        contract::succeeds(&store, &admitted).await;
        assert_eq!(refused, (true, true, true));
    }
    use crabber_core::RunFence;

    #[tokio::test]
    async fn wrong_row_cannot_borrow_other_runs_raw_origin() {
        let store = crate::MemoryStore::new();
        let first = contract::admit(&store).await;
        let other = contract::admit(&store).await;
        let forged_route = RunFence {
            run_id: first.run.id.clone(),
            claim_token: other.run.claim_token.clone(),
        };
        let original = store
            .state
            .lock()
            .unwrap()
            .runs
            .insert(first.run.id.clone(), other.run.clone())
            .unwrap();
        let refused = store.execution(forged_route).await.is_err();
        store
            .state
            .lock()
            .unwrap()
            .runs
            .insert(first.run.id.clone(), original);
        contract::succeeds(&store, &first).await;
        assert!(refused, "a row cannot borrow a distinct genuine Run origin");
    }
    #[tokio::test]
    async fn raw_execution_keeps_existing_current_issuer_fence_guard() {
        use crabber_core::{Clock, ManualClock};
        let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = crate::MemoryStore::with_clock(clock.clone());
        let admitted = crate::public_state_contract::admit(&store, clock.now()).await;
        clock.set(admitted.run.lease_until);
        let replacement = store
            .claim_expired_run(&admitted.run.id, "replacement")
            .await
            .unwrap();
        assert_ne!(replacement.claim_token, admitted.fence.claim_token);
        let current = store.get_run(&admitted.run.id).await.unwrap().unwrap();
        let mut restored = admitted.run.clone();
        restored.lease_until = current.lease_until;
        store
            .state
            .lock()
            .unwrap()
            .runs
            .insert(admitted.run.id.clone(), restored);
        // Preserve the old no-op ownership path's independent issuer/head guard.
        assert!(
            store
                .ownership_fenced(&admitted.fence, |_, _| Ok(()))
                .is_err()
        );
        let refused = store.execution(admitted.fence.clone()).await.is_err();
        store
            .state
            .lock()
            .unwrap()
            .runs
            .insert(admitted.run.id.clone(), current);
        assert!(store.execution(replacement).await.is_ok());
        assert!(
            refused,
            "restoring a previous projection cannot restore its authority"
        );
    }
    native_pending_initial_items! {
    #[tokio::test]
    async fn genuine_pending_birth_refuses_copied_cached_raw_mutations() {
        let store = std::sync::Arc::new(MemoryStore::new());
        let session = contract::pending(store.clone()).await;
        let run = {
            let state = store.state.lock().unwrap();
            state
                .runs
                .values()
                .filter(|run| run.session_id == session)
                .find(|run| {
                    native_enrollment::original(
                        &state,
                        &session,
                        &run.id,
                        &mut Proof::bounded(32 << 20),
                    )
                    .unwrap()
                        == crate::native_enrollment::Status::Pending
                })
                .unwrap()
                .clone()
        };
        let cached = crate::memory::MemoryExecution {
            store: store.as_ref().clone(),
            fence: RunFence {
                run_id: run.id.clone(),
                claim_token: run.claim_token.clone(),
            },
        };
        contract::cached_mutations(store.as_ref(), &run, &cached).await;
        let state = store.state.lock().unwrap();
        assert_eq!(state.runs[&run.id].lease_until, run.lease_until);
    }
    }
}
