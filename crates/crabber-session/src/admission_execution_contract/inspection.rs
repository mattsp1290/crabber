//! Source inspection is observation only, including actual bound admission evidence.
#[path = "atomic_recovery.rs"]
mod atomic_recovery;
use super::*;
pub(super) async fn run(store: &dyn Store, clock: &Arc<ManualClock>) {
    let now = clock.now();
    let session = SessionId::new();
    let original =
        crate::execution_binding_contract::fixture(&session, now, "recovery-source", 'b');
    let KeyedAdmitOutcome::Started { receipt, admitted } =
        store.admit_bound_keyed_run(original.clone()).await.unwrap()
    else {
        panic!("fresh bound admission")
    };
    let old = store.execution(admitted.fence.clone()).await.unwrap();
    let record = store
        .load_admission_execution(&session, &original.keyed.options.key)
        .await
        .unwrap()
        .unwrap();
    let request = ClaimUnstartedAdmissionRequest {
        session_id: session,
        key: original.keyed.options.key.clone(),
        expected_fence: admitted.fence,
        expected_owner: admitted.run.owner.clone(),
        fingerprint: receipt.fingerprint.clone(),
        behavior_fingerprint: original.keyed.options.behavior_fingerprint.clone(),
        semantic_digest: receipt.semantic_digest.clone(),
        capsule_digest: record.capsule.digest().unwrap(),
        owner: "recovery-source-owner".into(),
        lease: Duration::from_secs(30),
    };
    assert_eq!(
        store
            .inspect_admission_recovery(request.clone())
            .await
            .unwrap_err(),
        AdmissionExecutionError::LiveLease
    );
    clock.set(now + time::Duration::seconds(30));
    inspect_recovery_is_observation(store, &request, &admitted.run).await;
    let source = store.inspect_admission_recovery(request).await.unwrap();
    Box::pin(atomic_recovery::run(store, source, original, receipt, old)).await;
    rotation_before_reserve_refuses(store, clock).await;
}

async fn inspect_recovery_is_observation(
    store: &dyn Store,
    request: &ClaimUnstartedAdmissionRequest,
    original: &Run,
) {
    let before = store.session_revision(&request.session_id).await.unwrap();
    let source = store
        .inspect_admission_recovery(request.clone())
        .await
        .unwrap();
    assert_eq!(source.expected_revision(), before);
    assert_eq!(source.expected_issuance(), 1);
    assert_eq!(source.request().expected_fence, request.expected_fence);
    assert!(!format!("{source:?}").contains(&request.expected_fence.claim_token));
    let repeated = store
        .inspect_admission_recovery(request.clone())
        .await
        .unwrap();
    assert_eq!(
        source.fingerprint().unwrap(),
        repeated.fingerprint().unwrap()
    );
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        before
    );
    assert_eq!(
        store.get_run(&original.id).await.unwrap().unwrap(),
        *original
    );
    let mut malformed = request.clone();
    malformed.session_id = SessionId("\0".into());
    assert_eq!(
        store
            .inspect_admission_recovery(malformed)
            .await
            .unwrap_err(),
        AdmissionExecutionError::SemanticConflict
    );
    let mut oversized = request.clone();
    oversized.expected_fence.run_id = RunId("x".repeat(129));
    assert_eq!(
        store
            .inspect_admission_recovery(oversized)
            .await
            .unwrap_err(),
        AdmissionExecutionError::SemanticConflict
    );
    let mut changed = request.clone();
    changed.capsule_digest = "c".repeat(64);
    assert_eq!(
        store.inspect_admission_recovery(changed).await.unwrap_err(),
        AdmissionExecutionError::SemanticConflict
    );
    let mut changed = request.clone();
    changed.expected_fence.claim_token = "different-native-owner".into();
    assert_eq!(
        store.inspect_admission_recovery(changed).await.unwrap_err(),
        AdmissionExecutionError::StaleOwner
    );
}

async fn rotation_before_reserve_refuses(store: &dyn Store, clock: &Arc<ManualClock>) {
    let now = clock.now();
    let session = SessionId::new();
    let bound = crate::execution_binding_contract::fixture(&session, now, "source-rotation", 'b');
    let KeyedAdmitOutcome::Started { receipt, admitted } =
        store.admit_bound_keyed_run(bound.clone()).await.unwrap()
    else {
        panic!("fresh source")
    };
    let record = store
        .load_admission_execution(&session, &bound.keyed.options.key)
        .await
        .unwrap()
        .unwrap();
    let request = ClaimUnstartedAdmissionRequest {
        session_id: session.clone(),
        key: bound.keyed.options.key.clone(),
        expected_fence: admitted.fence,
        expected_owner: admitted.run.owner,
        fingerprint: receipt.fingerprint,
        semantic_digest: receipt.semantic_digest,
        behavior_fingerprint: bound.keyed.options.behavior_fingerprint.clone(),
        capsule_digest: record.capsule.digest().unwrap(),
        owner: "source-contender".into(),
        lease: Duration::from_secs(30),
    };
    clock.set(now + time::Duration::seconds(30));
    let source = store
        .inspect_admission_recovery(request.clone())
        .await
        .unwrap();
    let reservation = atomic_recovery::reservation(&source, &bound);
    let winner = store.claim_unstarted_admission(request).await.unwrap();
    assert!(
        store
            .reserve_preparation(reservation.clone())
            .await
            .is_err()
    );
    assert!(
        store
            .lookup_session_owner(&session, &reservation.owner.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.get_run(&winner.run.id).await.unwrap().unwrap(),
        winner.run
    );
}

/// Actual immutable admission and live preparation; only corruption hooks are raw.
pub(crate) async fn reserved_fixture(
    store: &impl Store,
    clock: &Arc<ManualClock>,
) -> (
    crate::session_owner::preparation::AdmissionRecoveryPreparation,
    crate::session_owner::preparation::PreparationGrant,
    BoundKeyedAdmitRequest,
) {
    use crate::session_owner::preparation::PreparationGrant;
    let now = clock.now();
    let session = SessionId::new();
    let mut bound = crate::execution_binding_contract::fixture(&session, now, "guard-source", 'b');
    let KeyedAdmitOutcome::Started { receipt, admitted } =
        store.admit_bound_keyed_run(bound.clone()).await.unwrap()
    else {
        panic!("actual admission")
    };
    let record = store
        .load_admission_execution(&session, &bound.keyed.options.key)
        .await
        .unwrap()
        .unwrap();
    let request = ClaimUnstartedAdmissionRequest {
        session_id: session,
        key: bound.keyed.options.key.clone(),
        expected_fence: admitted.fence,
        expected_owner: admitted.run.owner,
        fingerprint: receipt.fingerprint.clone(),
        semantic_digest: receipt.semantic_digest.clone(),
        behavior_fingerprint: bound.keyed.options.behavior_fingerprint.clone(),
        capsule_digest: record.capsule.digest().unwrap(),
        owner: "guard-recovery-owner".into(),
        lease: Duration::from_secs(30),
    };
    clock.set(now + time::Duration::seconds(30));
    let source = store.inspect_admission_recovery(request).await.unwrap();
    bound
        .keyed
        .request
        .owner
        .clone_from(&source.request().owner);
    let reservation = atomic_recovery::reservation(&source, &bound);
    let authority =
        atomic_recovery::reserve_and_begin(store, &source, &reservation, &receipt).await;
    let grant = PreparationGrant::from_fresh_begin(reservation, authority).unwrap();
    (source, grant, bound)
}
pub(crate) async fn corrupted_carrier_refuses(
    store: &impl Store,
    source: crate::session_owner::preparation::AdmissionRecoveryPreparation,
    grant: crate::session_owner::preparation::PreparationGrant,
    bound: BoundKeyedAdmitRequest,
) {
    use crate::session_owner::preparation::{
        JoinedPreparation, PreparedAdmissionRecovery, PreparedOperation,
    };
    assert!(
        store
            .inspect_admission_recovery(source.request().clone())
            .await
            .is_err()
    );
    let before = store
        .session_revision(&source.request().session_id)
        .await
        .unwrap();
    let join_id = grant
        .original_reservation()
        .reconciliation()
        .unwrap()
        .join_id;
    let ((), joined) = JoinedPreparation::observe(
        grant,
        tokio::spawn(async move {
            Ok::<_, ()>((
                (),
                PreparedOperation::AdmissionRecovery(Box::new(PreparedAdmissionRecovery {
                    source,
                    binding: Box::new(bound),
                })),
            ))
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let session = joined
        .observed_grant()
        .original_reservation()
        .owner
        .binding
        .session_id
        .clone();
    assert!(store.complete_preparation(joined).await.is_err());
    assert_eq!(store.session_revision(&session).await.unwrap(), before);
    assert!(
        store
            .lookup_session_owner(&session, &join_id)
            .await
            .unwrap()
            .is_none()
    );
}
