use crate::session_owner_contract;
#[tokio::test]
async fn session_owner_contract() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    session_owner_contract::run_contract(|clock| store.clone().with_clock(clock)).await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    session_owner_contract::faults::run(&store, &clock, |value| {
        store
            .session_owner_fault
            .store(value, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
    clock.set(time::OffsetDateTime::UNIX_EPOCH);
    session_owner_contract::parent::run(&store, &clock).await;
    clock.set(time::OffsetDateTime::UNIX_EPOCH);
    super::corruption::run(&store).await;
    session_owner_contract::process::run(
        &store,
        "postgres::session_owner::tests::session_owner_process_helper",
        None,
    )
    .await;
}

#[tokio::test]
async fn session_owner_process_helper() {
    let Some(command) = session_owner_contract::process::command() else {
        return;
    };
    let url = crate::postgres::tests::test_url().unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let store = store.with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(10),
    )));
    session_owner_contract::process::execute(&store, command).await;
}

#[tokio::test]
async fn preparation_protocol_contract() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    Box::pin(crate::session_owner::preparation_contract::run(&store)).await;
}

#[tokio::test]
async fn preparation_protocol_commit_faults() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::session_owner::preparation_contract::faults::run(
        &store,
        |point| {
            store
                .session_owner_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        },
        |point| {
            store
                .binding_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .await;
}

#[tokio::test]
async fn preparation_absent_order_rejects_deleted_history_plus_foreign_publication() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let (grant, bound, old) =
        crate::session_owner::preparation_contract::absent::fixture_for_order(&store).await;
    assert_eq!(
        sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
            .bind(&bound.binding.request.session_id.0)
            .bind(&old.0)
            .execute(&store.pool)
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    crate::session_owner::preparation_contract::absent::foreign_refuses(&store, grant, bound).await;
}

#[tokio::test]
async fn preparation_absent_order_rejects_foreign_join_relabeled_renew() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let (grant, bound, join) =
        crate::session_owner::preparation_contract::absent::fixture_for_relabel(&store).await;
    assert_eq!(sqlx::query("UPDATE session_owner_audits SET operation='Renew' WHERE session_id=$1 AND operation_id=$2")
        .bind(&bound.binding.request.session_id.0).bind(&join.0).execute(&store.pool).await.unwrap().rows_affected(), 1);
    let mut tx = store.pool.begin().await.unwrap();
    assert_eq!(
        super::reads::publication_count(&mut tx, &bound.binding.request.session_id)
            .await
            .unwrap_err(),
        super::Error::UnknownStoreOutcome
    );
    tx.rollback().await.unwrap();
    crate::session_owner::preparation_contract::absent::relabel_refuses(&store, grant, bound).await;
}
