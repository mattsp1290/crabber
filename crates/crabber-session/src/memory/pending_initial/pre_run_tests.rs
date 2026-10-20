//! Genuine preparation registration can precede public Session/Run creation.
use super::*;
use crate::session_owner::{
    owned_scope::{
        PendingDriverTasks, PendingInitialProducer, PendingInitialProducerStatus as Status,
    },
    preparation::PreparedOperation,
};
use crabber_core::SessionContext;
use std::sync::Arc;
#[tokio::test]
async fn actual_absent_session_source_opens_and_seals_without_a_run() {
    let store = Arc::new(MemoryStore::new());
    let (root, bound) =
        crate::session_owner::preparation_contract::resources::declaration::setup_session(
            &*store, false,
        )
        .await;
    let original =
        crate::session_owner::preparation_contract::resources::initial_driver_observation(
            &*store,
            root,
            PreparedOperation::AdmitKeyed(Box::new(bound)),
        )
        .await;
    let binding = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding;
    let context = SessionContext::new(
        binding.session_id.clone(),
        binding.workspace_id.clone(),
        binding.directory.clone(),
    );
    let session = context.session_id().clone();
    {
        let state = store.state.lock().unwrap();
        assert!(state.sessions.is_empty() && state.runs.is_empty());
    }
    store
        .pending_initial_test_state.fault
        .store(2, std::sync::atomic::Ordering::SeqCst);
    let owner = PendingDriverTasks::default();
    let mut task =
        PendingInitialProducer::register(&owner, store.clone(), context, original).unwrap();
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    assert_eq!(observed.status(), &Status::Unresolved);
    let mut retry = observed.take_retry().unwrap();
    {
        let state = store.state.lock().unwrap();
        assert!(state.sessions.is_empty() && state.runs.is_empty());
        assert_eq!(state.pending_initial_protocols.len(), 1);
        assert!(contains_scope(&state, &session));
        assert!(routes(&state, &session, &mut Proof::bounded(32 << 20)).unwrap());
    }
    store
        .pending_initial_test_state.fault
        .store(0, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(retry.retry().await.unwrap(), Status::ClosedNoPublication);
    owner.wait_for_supervision().await;
    let state = store.state.lock().unwrap();
    assert!(state.sessions.is_empty() && state.runs.is_empty());
    assert!(routes(&state, &session, &mut Proof::bounded(32 << 20)).unwrap());
}
#[tokio::test]
async fn actual_pending_completion_creates_absent_session_with_original_assignment() {
    let store = Arc::new(MemoryStore::new());
    let (root, bound) =
        crate::session_owner::preparation_contract::resources::declaration::setup_session(
            &*store, false,
        )
        .await;
    let original =
        crate::session_owner::preparation_contract::resources::initial_driver_observation(
            &*store,
            root,
            PreparedOperation::AdmitKeyed(Box::new(bound)),
        )
        .await;
    let binding = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding;
    let context = SessionContext::new(
        binding.session_id.clone(),
        binding.workspace_id.clone(),
        binding.directory.clone(),
    );
    let session = context.session_id().clone();
    assert_eq!(
        super::tests::produce(store.clone(), context, original).await,
        Status::ClosedAssignment
    );
    let state = store.state.lock().unwrap();
    assert_eq!(state.sessions.len(), 1);
    assert_eq!(state.runs.len(), 1);
    let run = state.runs.values().next().unwrap();
    assert_eq!(run.session_id, session);
    assert!(state.execution_enrollment_unclassified.is_empty());
    assert_eq!(
        crate::memory::native_enrollment::original(
            &state,
            &session,
            &run.id,
            &mut Proof::bounded(32 << 20)
        )
        .unwrap(),
        crate::native_enrollment::Status::Pending
    );
}

fn empty_fork_request(store: &MemoryStore, source: &SessionContext) -> crate::ForkSession {
    let state = store.state.lock().unwrap();
    crate::ForkSession {
        id: crabber_core::ControlId::new(),
        source_session_id: source.session_id().clone(),
        workspace_id: source.workspace_id().to_owned(),
        directory: source.directory().to_owned(),
        expected_source_revision: *state
            .public_revisions
            .get(source.session_id())
            .unwrap(),
        boundary: crate::ForkBoundary::Empty,
        target_session_id: SessionId::new(),
        title: "bounded empty fork".into(),
        defaults: crate::SessionDefaults::default(),
        limits: crate::SnapshotLimits {
            messages: 1,
            tool_calls: 1,
            parts: 1,
            text_bytes: 1024,
            encoded_bytes: 8192,
        },
        max_proof_bytes: 32 << 20,
    }
}

#[tokio::test]
async fn unattributable_actual_roster_cannot_make_a_fork_target_free() {
    use crate::Store;
    let store = Arc::new(MemoryStore::new());
    let (source_context, _) = super::tests::original(&store, false).await;
    let (root, bound) =
        crate::session_owner::preparation_contract::resources::declaration::setup_session(
            &*store, false,
        )
        .await;
    let original =
        crate::session_owner::preparation_contract::resources::initial_driver_observation(
            &*store,
            root,
            PreparedOperation::AdmitKeyed(Box::new(bound)),
        )
        .await;
    let binding = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding;
    let context = SessionContext::new(
        binding.session_id.clone(),
        binding.workspace_id.clone(),
        binding.directory.clone(),
    );
    store
        .pending_initial_test_state.fault
        .store(2, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        super::tests::produce(store.clone(), context, original).await,
        Status::Unresolved
    );
    let request = empty_fork_request(&store, &source_context);
    let baseline = MemoryStore::new();
    *baseline.state.lock().unwrap() = store.state.lock().unwrap().clone();
    assert!(matches!(
        Store::fork_session(&baseline, request.clone())
            .await
            .unwrap(),
        crate::ForkOutcome::Applied(_)
    ));
    {
        let mut state = store.state.lock().unwrap();
        let key = state
            .pending_initial_protocols
            .keys()
            .next()
            .unwrap()
            .clone();
        state
            .pending_initial
            .test_erase_routes_keep_empty_roster(&key);
        state.pending_initial_protocols.clear();
        assert!(contains_scope(&state, &request.target_session_id));
        assert!(
            routes(
                &state,
                &request.target_session_id,
                &mut Proof::bounded(32 << 20)
            )
            .is_err()
        );
    }
    assert_eq!(
        Store::fork_session(&*store, request.clone())
            .await
            .unwrap_err(),
        crate::ForkError::TargetExists
    );
    assert!(
        !store
            .state
            .lock()
            .unwrap()
            .sessions
            .contains_key(&request.target_session_id)
    );
}
