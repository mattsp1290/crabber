//! Real original producer/native publications; private metadata is inspection only.
use super::*;
use crate::native_enrollment::pending_initial::Ledger;
use crate::{
    Store,
    session_owner::{
        owned_scope::{
            PendingDriverTasks, PendingInitialProducer, PendingInitialProducerStatus as Status,
        },
        preparation::{JoinedPreparation, PreparedOperation},
    },
};
use crabber_core::SessionContext;
use std::sync::Arc;
pub(super) async fn original(
    store: &MemoryStore,
    keyed: bool,
) -> (SessionContext, JoinedPreparation) {
    use crate::session_owner::preparation_contract::resources;
    let original = if keyed {
        let (root, bound) = resources::declaration::setup(store).await;
        resources::initial_driver_observation(
            store,
            root,
            PreparedOperation::AdmitKeyed(Box::new(bound)),
        )
        .await
    } else {
        resources::initial_unkeyed_observation(store).await
    };
    let b = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding;
    (
        SessionContext::new(
            b.session_id.clone(),
            b.workspace_id.clone(),
            b.directory.clone(),
        ),
        original,
    )
}
pub(super) async fn produce(
    store: Arc<MemoryStore>,
    context: SessionContext,
    original: JoinedPreparation,
) -> Status {
    let owner = PendingDriverTasks::default();
    let mut task = PendingInitialProducer::register(&owner, store, context, original).unwrap();
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    let result = observed.status().clone();
    eprintln!(
        "actual Memory Pending producer: {result:?}, disposition={:?}, task_error={:?}, observation_failed={}",
        observed.disposition(),
        observed.error(),
        observed.observation_failed()
    );
    owner.wait_for_supervision().await;
    result
}
#[tokio::test]
async fn original_pending_assignment_is_atomic_private_and_never_raw() {
    for keyed in [false, true] {
        let store = Arc::new(MemoryStore::new());
        let (context, original) = original(&store, keyed).await;
        let session = context.session_id().clone();
        assert_eq!(
            produce(store.clone(), context, original.clone()).await,
            Status::ClosedAssignment
        );
        let (run, fence) = {
            let state = store.state.lock().unwrap();
            let run = state
                .runs
                .values()
                .find(|run| run.session_id == session)
                .unwrap();
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
            assert!(
                !state
                    .execution_enrollment_unclassified
                    .contains_key(&(session.clone(), run.id.clone()))
            );
            assert_eq!(state.pending_initial_protocols.len(), 1);
            assert!(routes(&state, &session, &mut Proof::bounded(32 << 20)).unwrap());
            (
                run.id.clone(),
                crabber_core::RunFence {
                    run_id: run.id.clone(),
                    claim_token: run.claim_token.clone(),
                },
            )
        };
        assert!(store.get_run(&run).await.is_err());
        assert!(store.execution(fence).await.is_err());
        assert!(
            Store::complete_preparation(&*store, original)
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn genuine_cloned_source_cannot_rebind_or_close_the_winning_epoch() {
    let store = Arc::new(MemoryStore::new());
    let (context, original) = original(&store, false).await;
    let owner = PendingDriverTasks::default();
    let mut a =
        PendingInitialProducer::register(&owner, store.clone(), context.clone(), original.clone())
            .unwrap();
    let mut b = PendingInitialProducer::register(&owner, store.clone(), context, original).unwrap();
    a.start().unwrap();
    b.start().unwrap();
    let (sa, sb) = (
        a.join().await.unwrap().status().clone(),
        b.join().await.unwrap().status().clone(),
    );
    assert!(
        matches!(
            (&sa, &sb),
            (Status::ClosedAssignment, Status::Unresolved)
                | (Status::Unresolved, Status::ClosedAssignment)
        ),
        "{sa:?} {sb:?}"
    );
    owner.wait_for_supervision().await;
    let state = store.state.lock().unwrap();
    assert_eq!(state.runs.len(), 1);
    assert_eq!(state.pending_initial_protocols.len(), 1);
    assert!(
        routes(
            &state,
            &state.runs.values().next().unwrap().session_id,
            &mut Proof::bounded(32 << 20)
        )
        .unwrap()
    );
}
#[tokio::test]
async fn authentic_raw_completed_source_never_opens_or_adopts_pending() {
    for keyed in [false, true] {
        let store = Arc::new(MemoryStore::new());
        let (context, original) = original(&store, keyed).await;
        let completed = Store::complete_preparation(&*store, original.clone())
            .await
            .unwrap();
        let run = match completed {
            crate::session_owner::preparation::PreparationCompletion::Admitted(a) => a.run.id,
            crate::session_owner::preparation::PreparationCompletion::Keyed(
                crate::KeyedAdmitOutcome::Started { admitted, .. },
            ) => admitted.run.id,
            _ => panic!("fresh Raw completion"),
        };
        assert_eq!(
            produce(store.clone(), context, original.clone()).await,
            Status::SourceAlreadyCompletedRaw {
                run_id: run.clone()
            }
        );
        assert!(
            Store::complete_preparation(&*store, original)
                .await
                .unwrap()
                .is_replayed()
        );
        let state = store.state.lock().unwrap();
        assert!(state.pending_initial_protocols.is_empty());
        assert!(state.pending_initial.is_empty());
        assert!(
            state
                .execution_enrollment_unclassified
                .keys()
                .any(|(_, id)| id == &run)
        );
    }
}
#[tokio::test]
async fn actual_pending_backend_before_commit_and_lost_open_ack() {
    for boundary in [1, 2] {
        let store = Arc::new(MemoryStore::new());
        let (context, original) = original(&store, false).await;
        let session = context.session_id().clone();
        store
            .pending_initial_test_state.fault
            .store(boundary, std::sync::atomic::Ordering::SeqCst);
        let owner = PendingDriverTasks::default();
        let mut task =
            PendingInitialProducer::register(&owner, store.clone(), context, original).unwrap();
        task.start().unwrap();
        let observed = task.join().await.unwrap();
        assert_eq!(observed.status(), &Status::Unresolved);
        let mut retry = observed.take_retry().unwrap();
        {
            let state = store.state.lock().unwrap();
            assert!(state.runs.is_empty());
            assert_eq!(state.pending_initial_protocols.is_empty(), boundary == 1);
        }
        store
            .pending_initial_test_state.fault
            .store(0, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(retry.retry().await.unwrap(), Status::ClosedNoPublication);
        owner.wait_for_supervision().await;
        assert!(
            routes(
                &store.state.lock().unwrap(),
                &session,
                &mut Proof::bounded(32 << 20)
            )
            .unwrap()
        );
    }
}

#[tokio::test]
async fn coherent_genuine_losing_epoch_cannot_replace_original_winner() {
    let winner = Arc::new(MemoryStore::new());
    let (context, original) = original(&winner, false).await;
    let session = context.session_id().clone();
    // Both backends start from the same genuine protected original source.
    // Open commits, its ACK is lost, and actual original Join then closes the
    // epoch without ever publishing a Run. Both epoch identities are genuine.
    let loser = Arc::new(MemoryStore::new());
    *loser.state.lock().unwrap() = winner.state.lock().unwrap().clone();
    winner
        .pending_initial_test_state.fault
        .store(2, std::sync::atomic::Ordering::SeqCst);
    loser
        .pending_initial_test_state.fault
        .store(2, std::sync::atomic::Ordering::SeqCst);
    let owner = PendingDriverTasks::default();
    let mut a =
        PendingInitialProducer::register(&owner, winner.clone(), context.clone(), original.clone())
            .unwrap();
    let mut b = PendingInitialProducer::register(&owner, loser.clone(), context, original).unwrap();
    a.start().unwrap();
    b.start().unwrap();
    assert_eq!(a.join().await.unwrap().status(), &Status::Unresolved);
    let observed = b.join().await.unwrap();
    assert_eq!(observed.status(), &Status::Unresolved);
    let mut losing_retry = observed.take_retry().unwrap();
    {
        let a = winner.state.lock().unwrap();
        let mut b = loser.state.lock().unwrap();
        assert!(a.runs.is_empty() && b.runs.is_empty());
        let original_a = a.pending_initial_protocols.values().next().unwrap();
        let original_b = b.pending_initial_protocols.values().next().unwrap();
        assert!(original_a.key == original_b.key);
        assert_ne!(original_a.epoch, original_b.epoch);
        assert!(routes(&a, &session, &mut Proof::bounded(32 << 20)).unwrap());
        assert!(routes(&b, &session, &mut Proof::bounded(32 << 20)).unwrap());
        // Keep A's independent immutable original, replace ALL seven current
        // projections with the internally coherent genuine B relation.
        let replacement = b.pending_initial.clone();
        *b = a.clone();
        b.pending_initial = replacement;
        assert!(routes(&b, &session, &mut Proof::bounded(32 << 20)).is_err());
    }
    loser
        .pending_initial_test_state.fault
        .store(0, std::sync::atomic::Ordering::SeqCst);
    // Even the genuine losing original's actually joined, same-ID Seal cannot
    // use those coherent projections to close/rebind the retained winner.
    assert!(losing_retry.retry().await.is_err());
    owner.wait_for_supervision().await;
    assert!(loser.state.lock().unwrap().runs.is_empty());
}

#[tokio::test]
async fn retained_pre_run_original_detects_erasure_and_purges_only_same_tombstone() {
    let store = Arc::new(MemoryStore::new());
    let (context_a, original_a) = original(&store, false).await;
    let (context_b, original_b) = original(&store, false).await;
    let (session_a, session_b) = (
        context_a.session_id().clone(),
        context_b.session_id().clone(),
    );
    store
        .pending_initial_test_state.fault
        .store(2, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        produce(store.clone(), context_a, original_a).await,
        Status::Unresolved
    );
    assert_eq!(
        produce(store.clone(), context_b, original_b).await,
        Status::Unresolved
    );
    let mut state = store.state.lock().unwrap();
    assert!(state.runs.is_empty());
    let projections = state.pending_initial.clone();
    let originals = state.pending_initial_protocols.clone();
    state.pending_initial = Ledger::default();
    assert!(contains_scope(&state, &session_a));
    assert!(routes(&state, &session_a, &mut Proof::bounded(32 << 20)).is_err());
    state.pending_initial = projections;
    state.pending_initial_protocols.clear();
    assert!(routes(&state, &session_a, &mut Proof::bounded(32 << 20)).is_err());
    state.pending_initial_protocols = originals;
    assert!(purge(&mut state, &session_a, &mut Proof::bounded(32 << 20)).is_err());
    assert!(routes(&state, &session_a, &mut Proof::bounded(32 << 20)).unwrap());
    // These are the existing scoped tombstone test fixtures. This tests only
    // physical protocol removal, never deletion readiness or whole ownership.
    state.tombstones.insert(
        session_a.clone(),
        crate::deletion::contract::deleted_for(&session_b),
    );
    assert!(purge(&mut state, &session_a, &mut Proof::bounded(32 << 20)).is_err());
    assert!(routes(&state, &session_a, &mut Proof::bounded(32 << 20)).unwrap());
    state.tombstones.insert(
        session_a.clone(),
        crate::deletion::contract::deleted_for(&session_a),
    );
    purge(&mut state, &session_a, &mut Proof::bounded(32 << 20)).unwrap();
    assert!(!contains_scope(&state, &session_a));
    assert!(!routes(&state, &session_a, &mut Proof::bounded(32 << 20)).unwrap());
    assert!(routes(&state, &session_b, &mut Proof::bounded(32 << 20)).unwrap());
    assert!(super::super::deletion::ensure_live(&state, &session_a).is_err());
}

#[tokio::test]
async fn authentic_raw_metadata_cannot_hide_retained_pending_source_members() {
    let raw = Arc::new(MemoryStore::new());
    let (context, original) = original(&raw, false).await;
    let pending = Arc::new(MemoryStore::new());
    *pending.state.lock().unwrap() = raw.state.lock().unwrap().clone();
    Store::complete_preparation(&*raw, original.clone())
        .await
        .unwrap();
    pending
        .pending_initial_test_state.fault
        .store(2, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        produce(pending.clone(), context.clone(), original.clone()).await,
        Status::Unresolved
    );
    {
        let source = pending.state.lock().unwrap();
        assert!(source.runs.is_empty());
        let mut target = raw.state.lock().unwrap();
        assert_eq!(target.runs.len(), 1);
        // Both facts are actual outputs from genuine cloned originals. This
        // impossible combined state must refuse rather than let authentic Raw
        // history suppress independently retained Pending source membership.
        target.pending_initial = source.pending_initial.clone();
        target.pending_initial_protocols = source.pending_initial_protocols.clone();
    }
    assert_eq!(
        produce(raw.clone(), context, original).await,
        Status::Unresolved
    );
    let state = raw.state.lock().unwrap();
    assert_eq!(state.runs.len(), 1);
    assert_eq!(state.pending_initial_protocols.len(), 1);
}
