//! One supplied budget follows fresh publication and metadata-only original replay.
use crate::session_owner::preparation::{ExpiredRunOutcome, PreparationCompletion};
use crate::{Store, public_session::v2::proof::Proof};
use std::sync::{Arc, atomic::Ordering};
#[tokio::test]
async fn prepared_expired_fresh_and_replay_use_one_exact_proof() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let pending = Box::pin(crate::run_control_contract::expired::pending(
        &store, &clock,
    ))
    .await;
    let receipt = Box::pin(fresh_with_budget(&store, &pending)).await;
    Box::pin(replay_with_budget(&store, &pending, &receipt, &clock)).await;
}

#[tokio::test]
async fn expired_context_authenticates_released_history_under_same_remaining_proof() {
    use crabber_core::Clock;
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let store = store.with_clock(clock.clone());
    let (request, original_cursor) = released_history_request(&store, &clock).await;
    let before = store.session_revision(&request.session_id).await.unwrap();
    let history = store
        .list_events(&request.session_id, None, 1000)
        .await
        .unwrap();
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    let mut proof = Proof::bounded(32 << 20);
    proof.charge(31).unwrap();
    super::context_with_proof(&mut tx, request.clone(), clock.now(), &mut proof)
        .await
        .unwrap();
    let n = proof.used();
    tx.rollback().await.unwrap();
    for cap in [n - 1, n] {
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let mut proof = Proof::bounded(cap);
        proof.charge(31).unwrap();
        let result =
            super::context_with_proof(&mut tx, request.clone(), clock.now(), &mut proof).await;
        tx.rollback().await.unwrap();
        assert_eq!(result.is_ok(), cap == n);
        if cap == n {
            assert_eq!(proof.used(), n);
        } else {
            assert!(proof.charge(0).is_err());
        }
    }
    // Intact Released metadata cannot replace an erased independent original payload.
    // Keep the referenced row so the relational foreign key remains valid.
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE events SET data='null' WHERE session_id=$1 AND seq=$2")
        .bind(&request.session_id.0)
        .bind(i64::try_from(original_cursor.0).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    let result = super::context_with_proof(
        &mut tx,
        request.clone(),
        clock.now(),
        &mut Proof::bounded(32 << 20),
    )
    .await;
    tx.rollback().await.unwrap();
    assert!(result.is_err());
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        before
    );
    assert_eq!(
        store
            .list_events(&request.session_id, None, 1000)
            .await
            .unwrap(),
        history
    );
}

async fn fresh_with_budget(
    store: &crate::PostgresStore,
    pending: &crate::run_control_contract::expired::Pending,
) -> crate::session_owner::preparation::ExpiredRunReceipt {
    let session = &pending.source.request().session_id;
    let run = &pending.source.request().expected_fence.run_id;
    let before_run = store.get_run(run).await.unwrap();
    let before_revision = store.session_revision(session).await.unwrap();
    let before_events = store.list_events(session, None, 100).await.unwrap();
    store.binding_fault.store(1, Ordering::SeqCst);
    let mut proof = Proof::bounded(32 << 20);
    proof.charge(23).unwrap();
    assert!(
        Box::pin(store.complete_preparation_captured_with_proof(
            pending.joined.clone(),
            false,
            &mut proof
        ))
        .await
        .is_err()
    );
    let n = proof.used();
    store.binding_fault.store(0, Ordering::SeqCst);
    assert_eq!(store.get_run(run).await.unwrap(), before_run);
    assert_eq!(
        store.session_revision(session).await.unwrap(),
        before_revision
    );
    assert_eq!(
        store.list_events(session, None, 100).await.unwrap(),
        before_events
    );
    let mut proof = Proof::bounded(n - 1);
    proof.charge(23).unwrap();
    assert!(
        Box::pin(store.complete_preparation_captured_with_proof(
            pending.joined.clone(),
            false,
            &mut proof
        ))
        .await
        .is_err()
    );
    assert!(proof.charge(0).is_err());
    assert_eq!(store.get_run(run).await.unwrap(), before_run);
    assert_eq!(
        store.session_revision(session).await.unwrap(),
        before_revision
    );
    assert_eq!(
        store.list_events(session, None, 100).await.unwrap(),
        before_events
    );
    let mut proof = Proof::bounded(n);
    proof.charge(23).unwrap();
    let fresh = Box::pin(store.complete_preparation_captured_with_proof(
        pending.joined.clone(),
        false,
        &mut proof,
    ))
    .await
    .unwrap();
    assert_eq!(proof.used(), n);
    let PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Claimed { receipt, .. }) =
        fresh.completion
    else {
        panic!("actual fresh claim");
    };
    receipt
}
async fn replay_with_budget(
    store: &crate::PostgresStore,
    pending: &crate::run_control_contract::expired::Pending,
    receipt: &crate::session_owner::preparation::ExpiredRunReceipt,
    clock: &crabber_core::ManualClock,
) {
    let session = &pending.source.request().session_id;
    let run = &pending.source.request().expected_fence.run_id;
    store
        .manage_session(crate::ManageSession {
            id: crabber_core::ControlId::new(),
            session_id: session.clone(),
            expected_revision: receipt.revision,
            change: crate::SessionChange::Rename {
                title: "later metadata revision".into(),
            },
        })
        .await
        .unwrap();
    assert!(store.session_revision(session).await.unwrap() > receipt.revision);
    let committed_run = store.get_run(run).await.unwrap();
    let committed_revision = store.session_revision(session).await.unwrap();
    let committed_events = store.list_events(session, None, 100).await.unwrap();
    // Current lease/revision advancement must not turn original replay into fresh work.
    clock.set(time::OffsetDateTime::UNIX_EPOCH + time::Duration::hours(2));
    let mut proof = Proof::bounded(32 << 20);
    proof.charge(23).unwrap();
    let replay = Box::pin(store.complete_preparation_captured_with_proof(
        pending.joined.clone(),
        false,
        &mut proof,
    ))
    .await
    .unwrap();
    let replay_n = proof.used();
    assert!(
        matches!(replay.completion, PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Replayed(ref saved)) if saved == receipt)
    );
    for cap in [replay_n - 1, replay_n] {
        let mut proof = Proof::bounded(cap);
        proof.charge(23).unwrap();
        let result = Box::pin(store.complete_preparation_captured_with_proof(
            pending.joined.clone(),
            false,
            &mut proof,
        ))
        .await;
        assert_eq!(result.is_ok(), cap == replay_n);
        if cap == replay_n {
            assert_eq!(proof.used(), replay_n);
        } else {
            assert!(proof.charge(0).is_err());
        }
        assert_eq!(store.get_run(run).await.unwrap(), committed_run);
        assert_eq!(
            store.session_revision(session).await.unwrap(),
            committed_revision
        );
        assert_eq!(
            store.list_events(session, None, 100).await.unwrap(),
            committed_events
        );
    }
}

async fn released_history_request(
    store: &crate::PostgresStore,
    clock: &crabber_core::ManualClock,
) -> (
    crate::session_owner::preparation::InspectExpiredRunPreparation,
    crabber_core::EventCursor,
) {
    use crabber_core::Clock;
    let f = Box::pin(crate::approval_contract::fixture::staged(store, clock)).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    let cancel = crate::approval_contract::termination::request(store, &f, false).await;
    let terminated = store.terminate_approval(cancel).await.unwrap();
    assert_eq!(
        store
            .approval_snapshot(&f.run.session_id, &f.run.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        crate::ApprovalContinuationState::Released
    );
    let mut bound = crate::typed_admission_contract::prepare(
        crate::execution_binding_contract::fixture(
            &f.run.session_id,
            clock.now(),
            "after-released-history",
            'b',
        ),
        crabber_core::InputEnvelope::text("next eligible original", terminated.revision).unwrap(),
        clock.now(),
    );
    bound.keyed.request.expected_revision = Some(terminated.revision);
    let crate::KeyedAdmitOutcome::Started { admitted, .. } =
        store.admit_bound_keyed_run(bound).await.unwrap()
    else {
        panic!("genuine next initial Run");
    };
    store
        .execution(admitted.fence.clone())
        .await
        .unwrap()
        .begin_admission_execution()
        .await
        .unwrap();
    clock.set(admitted.run.lease_until);
    let request = crate::session_owner::preparation::InspectExpiredRunPreparation {
        session_id: admitted.session.id.clone(),
        expected_fence: admitted.fence,
        owner: "history-proof".into(),
        lease: std::time::Duration::from_secs(30),
    };
    (request, terminated.event_cursor)
}
