//! Genuine actual Initial return routes; no fabricated source or authority.
use super::Harness;
use crate::{BoundAdmitRequest, BoundKeyedAdmitRequest, KeyedAdmitOutcome, Store};
use crabber_core::{SessionId, SessionRevision};
use time::OffsetDateTime;
#[derive(Clone, Copy)]
enum Case {
    Ordinary,
    Bound,
    Keyed,
    BoundKeyed,
}
pub(crate) async fn run(store: &impl Harness) {
    for case in [Case::Ordinary, Case::Bound, Case::Keyed, Case::BoundKeyed] {
        for lost in [false, true] {
            cell(store, case, lost).await;
        }
    }
    for lost in [false, true] {
        queue(store, lost).await;
    }
    for keyed in [false, true] {
        for lost in [false, true] {
            prepared(store, keyed, lost).await;
        }
    }
}
async fn cell(store: &impl Harness, case: Case, lost: bool) {
    let session = SessionId::new();
    let bound = crate::execution_binding_contract::fixture(
        &session,
        OffsetDateTime::UNIX_EPOCH,
        "raw-initial",
        'b',
    );
    store
        .manage_session(crate::ManageSession {
            session_id: session.clone(),
            id: crabber_core::ControlId::new(),
            expected_revision: SessionRevision::INITIAL,
            change: crate::SessionChange::Create {
                workspace_id: bound.keyed.request.workspace_id.clone(),
                directory: bound.keyed.request.directory.clone(),
                title: "raw initial".into(),
                defaults: crate::SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    let before = store.initial_retained(&session).await;
    store.probe().arm_initial(&session, true);
    assert!(invoke(store, &bound, case).await.is_err());
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(store.initial_retained(&session).await, before);
    store.probe().arm_initial(&session, false);
    store.initial_fault(if lost { 2 } else { 0 });
    let result = invoke(store, &bound, case).await;
    store.initial_fault(0);
    assert_eq!(result.is_err(), lost);
    assert_eq!(store.probe().hits().len(), 1);
    let committed = store.initial_retained(&session).await;
    assert_ne!(committed, before);
    store.probe().arm_initial(&session, true);
    let result = invoke(store, &bound, case).await;
    assert_eq!(
        result.is_ok(),
        matches!(case, Case::Keyed | Case::BoundKeyed)
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(store.initial_retained(&session).await, committed);
}
async fn invoke(
    store: &impl Store,
    bound: &BoundKeyedAdmitRequest,
    case: Case,
) -> Result<(), crate::StoreError> {
    match case {
        Case::Ordinary => store
            .admit_run(bound.keyed.request.clone())
            .await
            .map(|_| ()),
        Case::Bound => store
            .admit_bound_run(BoundAdmitRequest {
                request: bound.keyed.request.clone(),
                binding: bound.binding.clone(),
            })
            .await
            .map(|_| ()),
        Case::Keyed => store.admit_keyed_run(bound.keyed.clone()).await.map(|_| ()),
        Case::BoundKeyed => store.admit_bound_keyed_run(bound.clone()).await.map(|_| ()),
    }
}
async fn queue(store: &impl Harness, lost: bool) {
    let fixture = crate::queue_contract::fixture(store).await;
    store.mutate_queue(fixture.enqueue.clone()).await.unwrap();
    let authority = crate::queue_contract::claim(store, &fixture).await;
    let prepare = crate::queue_contract::mutation(
        store,
        &fixture,
        crate::QueueAction::Prepare {
            authority: authority.clone(),
            prepared: Box::new(fixture.prepared.clone()),
        },
    )
    .await;
    store.mutate_queue(prepare).await.unwrap();
    let request = crate::queue_contract::mutation(
        store,
        &fixture,
        crate::QueueAction::Admit {
            authority,
            owner: "raw-queue".into(),
            lease: std::time::Duration::from_secs(30),
        },
    )
    .await;
    let before = store.initial_retained(&fixture.session).await;
    store.probe().arm_initial(&fixture.session, true);
    assert!(store.mutate_queue(request.clone()).await.is_err());
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(store.initial_retained(&fixture.session).await, before);
    store.probe().arm_initial(&fixture.session, false);
    store.initial_fault(if lost { 2 } else { 0 });
    let result = store.mutate_queue(request.clone()).await;
    store.initial_fault(0);
    assert_eq!(result.is_err(), lost);
    if let Ok(crate::QueueOutcome::Applied { admitted, .. }) = result {
        assert!(admitted.is_some());
    }
    assert_eq!(store.probe().hits().len(), 1);
    let committed = store.initial_retained(&fixture.session).await;
    assert_ne!(committed, before);
    store.probe().arm_initial(&fixture.session, true);
    assert!(matches!(
        store.mutate_queue(request).await.unwrap(),
        crate::QueueOutcome::Replayed(_)
    ));
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(store.initial_retained(&fixture.session).await, committed);
}
async fn prepared(store: &impl Harness, keyed: bool, lost: bool) {
    use crate::session_owner::{
        preparation::{PreparationCompletion, PreparedOperation},
        preparation_contract::resources,
    };
    let joined = if keyed {
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
    let session = joined
        .observed_grant()
        .original_reservation()
        .owner
        .binding
        .session_id
        .clone();
    let before = store.initial_retained(&session).await;
    store.probe().arm_initial(&session, true);
    assert!(store.complete_preparation(joined.clone()).await.is_err());
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(store.initial_retained(&session).await, before);
    store.probe().arm_initial(&session, false);
    store.initial_fault(if lost { 2 } else { 0 });
    let result = store.complete_preparation(joined.clone()).await;
    store.initial_fault(0);
    assert_eq!(result.is_err(), lost);
    if let Ok(result) = result {
        assert!(matches!(
            result,
            PreparationCompletion::Admitted(_)
                | PreparationCompletion::Keyed(KeyedAdmitOutcome::Started { .. })
        ));
    }
    assert_eq!(store.probe().hits().len(), 1);
    let committed = store.initial_retained(&session).await;
    assert_ne!(committed, before);
    store.probe().arm_initial(&session, true);
    assert!(
        store
            .complete_preparation(joined)
            .await
            .unwrap()
            .is_replayed()
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(store.initial_retained(&session).await, committed);
}
