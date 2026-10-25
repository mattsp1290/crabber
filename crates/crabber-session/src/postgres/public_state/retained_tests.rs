//! Saved receipt validation retains the exact canonical event pair under one Proof.
use crate::public_session::v2::proof::Proof;
use crate::{PublicSourceAuthority, PublicSourceMutation, Store};
use crabber_core::{ControlId, PublicPatchOperation, PublicSourceChange};

#[tokio::test]
async fn retained_publication_pair_has_exact_shared_budget_and_originals() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let (admitted, _, authority, _) = Box::pin(crate::phase_contract::fixture(&store)).await;
        let session = &admitted.session.id;
        let request = PublicSourceMutation {
            id: ControlId::new(),
            expected_revision: store.session_revision(session).await.unwrap(),
            change: PublicSourceChange::StateDelta {
                delta: vec![
                    PublicPatchOperation::new(
                        serde_json::json!({"op":"add","path":"","value":{"nested":null}}),
                    )
                    .unwrap(),
                ],
            },
        };
        let receipt = store
            .mutate_bound_public_source(
                request.clone(),
                PublicSourceAuthority::Attempt(authority.clone()),
            )
            .await
            .unwrap();
        let snapshot = store.public_state(session).await.unwrap();
        let history = store.list_events(session, None, 100).await.unwrap();
        let original_before =
            retained_budget(&store, session, &request.id, &receipt, &history).await;
        // Erase the original payload while preserving the receipt foreign key.
        for cursor in [original_before, receipt.event_cursor] {
            let mut tx = crate::postgres::transaction::begin(&store.pool)
                .await
                .unwrap();
            sqlx::query("UPDATE events SET data='null' WHERE session_id=$1 AND seq=$2")
                .bind(&session.0)
                .bind(i64::try_from(cursor.0).unwrap())
                .execute(&mut *tx)
                .await
                .unwrap();
            let result = super::saved_with_proof(
                &mut tx,
                session,
                &request.id,
                &mut Proof::bounded(32 << 20),
            )
            .await;
            tx.rollback().await.unwrap();
            assert!(result.is_err(), "each independent original is mandatory");
        }
        assert_eq!(store.public_state(session).await.unwrap(), snapshot);
        assert_eq!(
            store.list_events(session, None, 100).await.unwrap(),
            history
        );
        assert_eq!(
            store
                .mutate_bound_public_source(request, PublicSourceAuthority::Attempt(authority))
                .await
                .unwrap(),
            receipt
        );
        assert_eq!(
            store.list_events(session, None, 100).await.unwrap(),
            history
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retained_activity_pair_rejects_each_wrong_scope_original() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let (admitted, _, authority, _) = Box::pin(crate::phase_contract::fixture(&store)).await;
        let session = &admitted.session.id;
        let (request, receipt) = activity_delta(&store, session, &authority).await;
        let before = store.public_state(session).await.unwrap();
        let history = store.list_events(session, None, 100).await.unwrap();
        let publication = store
            .lookup_public_source_publication(session, &request.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(publication.receipt, receipt);
        assert_eq!(publication.events.len(), 2);
        assert!(publication.events[0].cursor < publication.events[1].cursor);
        assert_eq!(publication.events[1].cursor, Some(receipt.event_cursor));
        assert_eq!(
            before.contents.activities()[0].content(),
            &serde_json::json!({"count":1,"nested":null})
        );
        for original in &publication.events {
            wrong_scope(&store, &request, original).await;
        }
        assert_eq!(
            store
                .mutate_bound_public_source(request, PublicSourceAuthority::Attempt(authority))
                .await
                .unwrap(),
            receipt
        );
        assert_eq!(store.public_state(session).await.unwrap(), before);
        assert_eq!(
            store.list_events(session, None, 100).await.unwrap(),
            history
        );
    })
    .await
    .unwrap();
}
async fn wrong_scope(
    store: &crate::PostgresStore,
    request: &PublicSourceMutation,
    original: &crabber_core::EventRecord,
) {
    let mut changed = original.clone();
    changed.run_id = crabber_core::RunId::new();
    let encoded =
        crate::public_state::encode_source_event(&changed, &mut Proof::bounded(32 << 20)).unwrap();
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE events SET data=$1 WHERE session_id=$2 AND seq=$3")
        .bind(encoded)
        .bind(&original.session_id.0)
        .bind(i64::try_from(original.cursor.unwrap().0).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    let result = super::saved_with_proof(
        &mut tx,
        &original.session_id,
        &request.id,
        &mut Proof::bounded(32 << 20),
    )
    .await;
    tx.rollback().await.unwrap();
    assert!(
        result.is_err(),
        "actual before/final payload must retain its exact scope"
    );
}

async fn retained_budget(
    store: &crate::PostgresStore,
    session: &crabber_core::SessionId,
    id: &ControlId,
    receipt: &crate::PublicStateReceipt,
    history: &[crabber_core::EventRecord],
) -> crabber_core::EventCursor {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    let mut proof = Proof::bounded(32 << 20);
    // Earlier work consumes this SAME allowance; the reader must not replace it.
    proof.charge(19).unwrap();
    let saved = super::saved_with_proof(&mut tx, session, id, &mut proof)
        .await
        .unwrap()
        .unwrap();
    let n = proof.used();
    assert_eq!(&saved.receipt, receipt);
    assert_eq!(saved.event.cursor, Some(receipt.event_cursor));
    let before = saved.before.unwrap();
    assert!(before.cursor < saved.event.cursor);
    assert!(history.contains(&before));
    assert!(history.contains(&saved.event));
    tx.rollback().await.unwrap();
    for cap in [n - 1, n] {
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let mut proof = Proof::bounded(cap);
        proof.charge(19).unwrap();
        let result = super::saved_with_proof(&mut tx, session, id, &mut proof).await;
        tx.rollback().await.unwrap();
        assert_eq!(result.is_ok(), cap == n);
        if cap == n {
            assert_eq!(proof.used(), n);
        } else {
            assert!(proof.charge(0).is_err());
        }
    }
    before.cursor.unwrap()
}

async fn activity_delta(
    store: &crate::PostgresStore,
    session: &crabber_core::SessionId,
    authority: &crate::AttemptAuthority,
) -> (PublicSourceMutation, crate::PublicStateReceipt) {
    let initial = PublicSourceMutation {
        id: ControlId::new(),
        expected_revision: store.session_revision(session).await.unwrap(),
        change: PublicSourceChange::ActivitySnapshot {
            activity: crabber_core::PublicActivity::new(
                "progress",
                "counter",
                serde_json::json!({"count":0,"nested":null}),
            )
            .unwrap(),
        },
    };
    store
        .mutate_bound_public_source(initial, PublicSourceAuthority::Attempt(authority.clone()))
        .await
        .unwrap();
    let request = PublicSourceMutation {
        id: ControlId::new(),
        expected_revision: store.session_revision(session).await.unwrap(),
        change: PublicSourceChange::ActivityDelta {
            activity_id: "progress".into(),
            activity_type: "counter".into(),
            delta: vec![
                PublicPatchOperation::new(
                    serde_json::json!({"op":"replace","path":"/count","value":1,"extra":null}),
                )
                .unwrap(),
            ],
        },
    };
    let receipt = store
        .mutate_bound_public_source(
            request.clone(),
            PublicSourceAuthority::Attempt(authority.clone()),
        )
        .await
        .unwrap();
    (request, receipt)
}
