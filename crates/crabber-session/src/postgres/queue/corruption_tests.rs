use crate::{Store, queue::*, queue_contract::corruption};
#[tokio::test]
async fn queue_rejects_replaced_prepared_and_coherent_input() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    for change_input in [false, true] {
        let (fixture, prepare, authority) = corruption::prepared(&store).await;
        let encoded: String = sqlx::query_scalar(
            "SELECT data_json FROM future_queue WHERE session_id=$1 AND queue_id=$2",
        )
        .bind(&fixture.session.0)
        .bind(&fixture.queue.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        let mut record: Record = decode(&encoded).unwrap();
        corruption::replace(&mut record, change_input);
        sqlx::query("UPDATE future_queue SET data_json=$1 WHERE session_id=$2 AND queue_id=$3")
            .bind(encode(&record).unwrap())
            .bind(&fixture.session.0)
            .bind(&fixture.queue.0)
            .execute(&store.pool)
            .await
            .unwrap();
        corruption::refused(&store, &fixture, prepare, authority).await;
    }
}
#[tokio::test]
async fn queue_bounds_corrupt_index_id_before_transfer() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let fixture = crate::queue_contract::fixture(&store).await;
    store.mutate_queue(fixture.enqueue).await.unwrap();
    sqlx::query("UPDATE future_queue SET queue_id=$1 WHERE session_id=$2")
        .bind("x".repeat(4096))
        .bind(&fixture.session.0)
        .execute(&store.pool)
        .await
        .unwrap();
    let request = QueuePageRequest {
        session_id: fixture.session,
        limit: 64,
        after_sequence: None,
        expected_revision: None,
    };
    assert!(store.list_queue(request).await.is_err());
}

async fn write_record(store: &crate::PostgresStore, record: &Record) {
    sqlx::query("UPDATE future_queue SET state=$1,summary_json=$2,data_json=$3 WHERE session_id=$4 AND queue_id=$5")
        .bind(format!("{:?}",record.summary.state)).bind(encode(&record.summary).unwrap()).bind(encode(record).unwrap()).bind(&record.summary.session_id.0).bind(&record.summary.queue_id.0).execute(&store.pool).await.unwrap();
}
async fn read_record(
    store: &crate::PostgresStore,
    session: &crabber_core::SessionId,
    queue: &crabber_core::QueueId,
) -> Record {
    let encoded: String = sqlx::query_scalar(
        "SELECT data_json FROM future_queue WHERE session_id=$1 AND queue_id=$2",
    )
    .bind(&session.0)
    .bind(&queue.0)
    .fetch_one(&store.pool)
    .await
    .unwrap();
    decode(&encoded).unwrap()
}
#[tokio::test]
async fn queue_rejects_cancelled_state_and_old_claim_resurrection() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    for claimed in [false, true] {
        let fixture = crate::queue_contract::fixture(&store).await;
        store.mutate_queue(fixture.enqueue.clone()).await.unwrap();
        if claimed {
            crate::queue_contract::claim(&store, &fixture).await;
        }
        let old = read_record(&store, &fixture.session, &fixture.queue).await;
        let cancel = crate::queue_contract::mutation(
            &store,
            &fixture,
            QueueAction::Cancel {
                queue_id: fixture.queue.clone(),
            },
        )
        .await;
        store.mutate_queue(cancel.clone()).await.unwrap();
        write_record(&store, &old).await;
        assert!(
            store
                .retained_queue(&fixture.session, &fixture.queue)
                .await
                .is_err()
        );
        assert!(
            store
                .get_queue(&fixture.session, &fixture.queue)
                .await
                .is_err()
        );
        let claim = crate::queue_contract::mutation(
            &store,
            &fixture,
            QueueAction::Claim {
                owner: "resurrection".into(),
                lease: std::time::Duration::from_secs(30),
            },
        )
        .await;
        assert!(store.mutate_queue(claim).await.is_err());
        assert!(matches!(
            store.mutate_queue(cancel).await.unwrap(),
            QueueOutcome::Replayed(_)
        ));
    }
}
#[tokio::test]
async fn queue_rejects_replaced_admission_mapping() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let (fixture, _, authority) = corruption::prepared(&store).await;
    let admit = crate::queue_contract::mutation(
        &store,
        &fixture,
        QueueAction::Admit {
            authority,
            owner: "mapping".into(),
            lease: std::time::Duration::from_secs(30),
        },
    )
    .await;
    store.mutate_queue(admit.clone()).await.unwrap();
    let mut record = read_record(&store, &fixture.session, &fixture.queue).await;
    record.summary.admission.as_mut().unwrap().run_id = crabber_core::RunId::new();
    write_record(&store, &record).await;
    assert!(
        store
            .retained_queue(&fixture.session, &fixture.queue)
            .await
            .is_err()
    );
    assert!(
        store
            .get_queue(&fixture.session, &fixture.queue)
            .await
            .is_err()
    );
    let cancel = crate::queue_contract::mutation(
        &store,
        &fixture,
        QueueAction::Cancel {
            queue_id: fixture.queue.clone(),
        },
    )
    .await;
    assert!(store.mutate_queue(cancel).await.is_err());
    assert!(matches!(
        store.mutate_queue(admit).await.unwrap(),
        QueueOutcome::Replayed(_)
    ));
}
