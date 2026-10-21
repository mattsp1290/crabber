//! Actual captured sources must spend the enclosing preparation allowance.
use crate::session_owner::preparation::ReservePreparation;
use crate::{Store, public_session::v2::proof::Proof};
#[tokio::test]
async fn expired_and_admission_recovery_preflight_keep_remaining_proof() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        let expired = Box::pin(crate::run_control_contract::expired::pending(
            &store, &clock,
        ))
        .await;
        check(
            &store,
            expired.joined.observed_grant().original_reservation(),
        )
        .await;
        let (_, grant, _) = Box::pin(
            crate::admission_execution_contract::inspection::reserved_fixture(&store, &clock),
        )
        .await;
        check(&store, grant.original_reservation()).await;
    })
    .await
    .unwrap();
}
async fn check(store: &crate::PostgresStore, request: &ReservePreparation) {
    let session = &request.owner.binding.session_id;
    let revision = store.session_revision(session).await.unwrap();
    let events = store.list_events(session, None, 1000).await.unwrap();
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    let mut proof = Proof::bounded(32 << 20);
    proof.charge(37).unwrap();
    proof.measure(request).unwrap();
    Box::pin(store.preparation_source_context(&mut tx, request, &mut proof))
        .await
        .unwrap();
    let n = proof.used();
    tx.rollback().await.unwrap();
    for cap in [n - 1, n] {
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let mut proof = Proof::bounded(cap);
        proof.charge(37).unwrap();
        proof.measure(request).unwrap();
        let result = Box::pin(store.preparation_source_context(&mut tx, request, &mut proof)).await;
        tx.rollback().await.unwrap();
        assert_eq!(result.is_ok(), cap == n);
        if cap == n {
            assert_eq!(proof.used(), n);
        } else {
            assert!(proof.charge(0).is_err());
        }
        assert_eq!(store.session_revision(session).await.unwrap(), revision);
        assert_eq!(
            store.list_events(session, None, 1000).await.unwrap(),
            events
        );
    }
}
