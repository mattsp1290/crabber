//! Public credential transport is always Raw, including inside an owned task.
//! This gate never adopts a Run or issues an enrolled member.
use super::Status;
use crate::StoreError;
use crate::public_session::v2::proof::Failure;

pub(crate) fn allow(status: Status) -> Result<(), StoreError> {
    match status {
        Status::Legacy | Status::Unclassified => Ok(()),
        Status::Pending => Err(StoreError::HostUnsupported),
    }
}

pub(crate) fn failure(_: Failure) -> StoreError {
    StoreError::Validation("raw execution credential evidence unavailable".into())
}

#[cfg(test)]
pub(crate) mod contract {
    use crate::{AdmitOutcome, KeyedAdmitOutcome, Store};
    use crabber_core::{Run, RunId, SessionId};

    pub(crate) async fn admit(store: &dyn Store) -> Box<AdmitOutcome> {
        let request = crate::admission_execution_contract::keyed(
            &SessionId::new(),
            time::OffsetDateTime::now_utc(),
        );
        let KeyedAdmitOutcome::Started { admitted, .. } =
            store.admit_keyed_run(request).await.unwrap()
        else {
            panic!("fresh native admission")
        };
        admitted
    }

    pub(crate) async fn succeeds(store: &dyn Store, admitted: &AdmitOutcome) {
        let actual = store.get_run(&admitted.run.id).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(&admitted.run).unwrap()
        );
        assert!(store.get_run(&RunId::new()).await.unwrap().is_none());
        let runs = store.list_unfinished_runs().await.unwrap();
        assert!(runs.iter().any(|r: &Run| r.id == admitted.run.id && r.claim_token == admitted.run.claim_token));
        assert!(store.execution(admitted.fence.clone()).await.is_ok());
    }

    native_pending_initial_items! {
    pub(crate) async fn pending<S: Store + 'static>(store: std::sync::Arc<S>) -> SessionId {
        use crate::session_owner::owned_scope::{
            PendingDriverTasks, PendingInitialProducer, PendingInitialProducerStatus,
        };
        let original =
            crate::session_owner::preparation_contract::resources::initial_unkeyed_observation(
                store.as_ref(),
            )
            .await;
        let binding = &original
            .observed_grant()
            .original_reservation()
            .owner
            .binding;
        let session = binding.session_id.clone();
        let context = crabber_core::SessionContext::new(
            session.clone(),
            binding.workspace_id.clone(),
            binding.directory.clone(),
        );
        let owner = PendingDriverTasks::default();
        let mut producer =
            PendingInitialProducer::register(&owner, store, context, original).unwrap();
        producer.start().unwrap();
        let observed = producer.join().await.unwrap();
        owner.wait_for_supervision().await;
        assert_eq!(
            observed.status(),
            &PendingInitialProducerStatus::ClosedAssignment,
            "original error: {:?}; observation failed: {}",
            observed.error(),
            observed.observation_failed()
        );
        session
    }

    fn event(run: &Run) -> crabber_core::EventRecord {
        crabber_core::EventRecord {
            cursor: None,
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            turn_id: None,
            kind: crabber_core::EventKind::RunSettled,
            payload: serde_json::Value::Null,
            correlation: None,
            live_only: false,
            created_at: time::OffsetDateTime::now_utc(),
        }
    }

    /// Contrast a genuine Raw holder with a copied genuine Pending fence. The
    /// three paths cover general writes, lease-only writes and a specialized
    /// state writer. No public capability transport creates the Pending cache.
    pub(crate) async fn cached_mutations(
        store: &dyn Store,
        pending: &Run,
        cached: &dyn crate::ExecutionStore,
    ) {
        let until = time::OffsetDateTime::now_utc() + time::Duration::minutes(10);
        let mut request = crate::admission_execution_contract::keyed(
            &SessionId::new(),
            time::OffsetDateTime::now_utc(),
        )
        .request;
        request.session_id = None;
        let raw = store.admit_run(request).await.unwrap();
        let control = store.execution(raw.fence).await.unwrap();
        control.renew_lease(until).await.unwrap();
        control.append_event(event(&raw.run)).await.unwrap();
        let state = store.public_state(&raw.session.id).await.unwrap();
        control
            .mutate_public_state(crate::PublicStateMutation {
                id: crabber_core::ControlId::new(),
                expected_revision: state.revision,
                contents: crabber_core::PublicStateContents::default(),
            })
            .await
            .unwrap();

        let before = store.public_state(&pending.session_id).await.unwrap();
        let events = store
            .list_events(&pending.session_id, None, 1000)
            .await
            .unwrap();
        assert!(store.get_run(&pending.id).await.is_err());
        assert!(
            store
                .execution(crabber_core::RunFence {
                    run_id: pending.id.clone(),
                    claim_token: pending.claim_token.clone()
                })
                .await
                .is_err()
        );
        assert_eq!(
            cached.renew_lease(until).await.unwrap_err(),
            crate::StoreError::HostUnsupported
        );
        assert_eq!(
            cached.append_event(event(pending)).await.unwrap_err(),
            crate::StoreError::HostUnsupported
        );
        assert!(
            cached
                .mutate_public_state(crate::PublicStateMutation {
                    id: crabber_core::ControlId::new(),
                    expected_revision: before.revision,
                    contents: crabber_core::PublicStateContents::default(),
                })
                .await
                .is_err()
        );
        assert_eq!(
            store.public_state(&pending.session_id).await.unwrap(),
            before
        );
        assert_eq!(
            store
                .list_events(&pending.session_id, None, 1000)
                .await
                .unwrap(),
            events
        );
    }
    }
}
