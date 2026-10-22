//! Real task Join/native claim, exact metadata replay, and contender exclusion.
use super::super::*;
use crate::session_owner::preparation::{
    self, AdmissionRecoveryOutcome, ExistingWorkTarget, JoinedPreparation, PreparationCompletion,
    PreparationGrant, PreparationKey, PreparationPurpose, PreparationSource,
    PreparedAdmissionRecovery, PreparedOperation, ReservePreparation,
};
use crate::session_owner::{MutateSessionOwner, SessionOwnerCommand, SessionOwnerOutcome};

pub(super) async fn run(
    store: &dyn Store,
    source: preparation::AdmissionRecoveryPreparation,
    mut bound: BoundKeyedAdmitRequest,
    receipt: AdmissionReceipt,
    old: Box<dyn ExecutionStore>,
) {
    bound
        .keyed
        .request
        .owner
        .clone_from(&source.request().owner);
    let request = reservation(&source, &bound);
    let authority = reserve_and_begin(store, &source, &request, &receipt).await;
    let grant = PreparationGrant::from_fresh_begin(request, authority).unwrap();
    let recovery = PreparedAdmissionRecovery {
        source: source.clone(),
        binding: Box::new(bound),
    };
    recovery.validate().unwrap();
    reject_replacement(store, &grant, &recovery).await;
    let ((), joined) = JoinedPreparation::observe(
        grant,
        tokio::spawn(async move {
            Ok::<_, ()>(((), PreparedOperation::AdmissionRecovery(Box::new(recovery))))
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let PreparationCompletion::AdmissionRecovery(AdmissionRecoveryOutcome::Claimed(claimed)) =
        store.complete_preparation(joined.clone()).await.unwrap()
    else {
        panic!("actual claim")
    };
    assert_eq!(claimed.record.receipt, receipt);
    assert_ne!(
        claimed.fence.claim_token,
        source.request().expected_fence.claim_token
    );
    let revision = store.session_revision(&receipt.session_id).await.unwrap();
    assert!(revision > source.expected_revision());
    replay(store, &joined, &receipt, revision).await;
    assert_eq!(
        old.begin_admission_execution().await.unwrap_err(),
        AdmissionExecutionError::StaleOwner
    );
    let fresh = store.execution(claimed.fence).await.unwrap();
    fresh.begin_admission_execution().await.unwrap();
    replay(store, &joined, &receipt, revision).await;
    fresh
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            EventRecord {
                cursor: None,
                session_id: receipt.session_id.clone(),
                run_id: receipt.run_id.clone(),
                turn_id: None,
                kind: EventKind::RunSettled,
                payload: serde_json::Value::Null,
                correlation: None,
                live_only: false,
                created_at: time::OffsetDateTime::UNIX_EPOCH,
            },
        )
        .await
        .unwrap();
    replay(
        store,
        &joined,
        &receipt,
        store.session_revision(&receipt.session_id).await.unwrap(),
    )
    .await;
}
pub(super) fn reservation(
    source: &preparation::AdmissionRecoveryPreparation,
    bound: &BoundKeyedAdmitRequest,
) -> ReservePreparation {
    let key = PreparationKey::AdmissionRecovery {
        key: source.request().key.clone(),
        issuance: source.expected_issuance(),
    };
    let mut owner = crate::session_owner_contract::reservation(source.request().session_id.clone());
    owner
        .binding
        .workspace_id
        .clone_from(&bound.binding.request.workspace_id);
    owner
        .binding
        .directory
        .clone_from(&bound.binding.request.directory);
    owner
        .binding
        .behavior_fingerprint
        .clone_from(&bound.binding.behavior_fingerprint);
    ReservePreparation {
        owner,
        key: key.clone(),
        source: Some(PreparationSource::AdmissionRecovery(source.clone())),
        resource_contract: None,
        expected_revision: source.expected_revision(),
        purpose: PreparationPurpose::ExistingRun {
            run_id: source.request().expected_fence.run_id.clone(),
            work: ExistingWorkTarget::AdmissionRecovery {
                key: source.request().key.clone(),
                issuance: source.expected_issuance(),
            },
        },
        intent: source.fingerprint().unwrap(),
    }
    .bind_key(key)
    .unwrap()
}
pub(super) async fn reserve_and_begin(
    store: &dyn Store,
    source: &preparation::AdmissionRecoveryPreparation,
    request: &ReservePreparation,
    receipt: &AdmissionReceipt,
) -> crate::session_owner::SessionOwnerAuthority {
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store.reserve_preparation(request.clone()).await.unwrap()
    else {
        panic!("fresh preparation")
    };
    assert_eq!(
        store
            .claim_unstarted_admission(source.request().clone())
            .await
            .unwrap_err(),
        AdmissionExecutionError::PreparationBusy
    );
    assert_eq!(
        store
            .get_run(&receipt.run_id)
            .await
            .unwrap()
            .unwrap()
            .claim_token,
        source.request().expected_fence.claim_token
    );
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: request.reconciliation().unwrap().begin_id,
                session_id: source.request().session_id.clone(),
                target: authority.target().clone(),
                command: SessionOwnerCommand::Begin,
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .claim_unstarted_admission(source.request().clone())
            .await
            .unwrap_err(),
        AdmissionExecutionError::PreparationBusy
    );
    authority
}

async fn replay(
    store: &dyn Store,
    joined: &JoinedPreparation,
    receipt: &AdmissionReceipt,
    revision: SessionRevision,
) {
    let PreparationCompletion::AdmissionRecovery(AdmissionRecoveryOutcome::Replayed(saved)) =
        store.complete_preparation(joined.clone()).await.unwrap()
    else {
        panic!("metadata only")
    };
    assert_eq!(saved, *receipt);
    assert_eq!(
        store.session_revision(&receipt.session_id).await.unwrap(),
        revision
    );
}

async fn reject_replacement(
    store: &dyn Store,
    grant: &PreparationGrant,
    original: &PreparedAdmissionRecovery,
) {
    let mut key_changed = original.clone();
    key_changed.binding.keyed.options.key = AdmissionKey::new("changed-recovery-key").unwrap();
    let mut binding_changed = original.clone();
    let source = original.source.request();
    binding_changed.binding.binding.provider = crate::execution_binding_contract::fixture(
        &source.session_id,
        time::OffsetDateTime::UNIX_EPOCH,
        "changed-recovery-backend",
        'b',
    )
    .binding
    .provider;
    for operation in [
        PreparedOperation::AdmissionRecovery(Box::new(key_changed)),
        PreparedOperation::AdmissionRecovery(Box::new(binding_changed)),
        PreparedOperation::AdmitKeyed(original.binding.clone()),
    ] {
        let ((), joined) = JoinedPreparation::observe(
            grant.clone(),
            tokio::spawn(async move { Ok::<_, ()>(((), operation)) }),
        )
        .await
        .unwrap()
        .unwrap();
        let before = store.session_revision(&source.session_id).await.unwrap();
        assert!(store.complete_preparation(joined).await.is_err());
        assert_eq!(
            store.session_revision(&source.session_id).await.unwrap(),
            before
        );
        assert!(
            store
                .lookup_session_owner(
                    &source.session_id,
                    &grant
                        .original_reservation()
                        .reconciliation()
                        .unwrap()
                        .join_id,
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .get_run(&source.expected_fence.run_id)
                .await
                .unwrap()
                .unwrap()
                .claim_token,
            source.expected_fence.claim_token
        );
    }
}
