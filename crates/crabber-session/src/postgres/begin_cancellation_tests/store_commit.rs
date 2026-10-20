//! A lost native Store commit ACK is reconciled from the durable original receipt.
use super::*;
use crate::{PublicStateMutation, Store};
use crabber_core::{ControlId, EventKind};
use time::OffsetDateTime;

#[tokio::test]
async fn cancelled_store_commit_reconciles_the_original_receipt_and_replays_once() {
    let Some(url) = super::super::tests::test_url() else {
        return;
    };
    let _guard = super::super::tests::TEST_LOCK.lock().await;
    super::super::PostgresStore::migrate(&url).await.unwrap();
    let mut store = super::super::PostgresStore::connect(&url).await.unwrap();
    let admitted = crate::public_state_contract::admit(&store, OffsetDateTime::now_utc()).await;
    let initial = store.public_state(&admitted.session.id).await.unwrap();
    let options = sqlx::postgres::PgConnectOptions::from_str(&url).unwrap();
    let proxy = Proxy::start(&options).await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(proxy.options(options))
        .await
        .unwrap();
    store.pool.close().await;
    store.pool = pool.clone();
    let before: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let execution = store.execution(admitted.fence).await.unwrap();
    let mutation = PublicStateMutation {
        id: ControlId::new(),
        expected_revision: initial.revision,
        contents: crate::public_state_contract::contents(),
    };
    proxy.arm_query(b"COMMIT\0", b'I');
    let mut original = Box::pin(execution.mutate_public_state(mutation.clone()));
    tokio::select! {
        _ = &mut original => panic!("Store returned before its actual held commit ACK"),
        held = timeout(Duration::from_secs(5), proxy.held()) => held.unwrap(),
    }
    drop(original);
    proxy.release();
    let receipt = store
        .lookup_public_state_mutation(&admitted.session.id, &mutation.id)
        .await
        .unwrap()
        .unwrap();
    let reconciled: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let current = store.public_state(&admitted.session.id).await.unwrap();
    let replay = execution
        .mutate_public_state(mutation.clone())
        .await
        .unwrap();
    let after_replay = store.public_state(&admitted.session.id).await.unwrap();
    let events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap();
    let mut changed = mutation.clone();
    changed.contents = crabber_core::PublicStateContents::default();
    let conflict = execution.mutate_public_state(changed).await;
    let after: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    drop(execution);
    pool.close().await;
    proxy.join().await;
    assert_ne!(
        before, after,
        "ambiguous Store commit connection was reused"
    );
    assert_eq!(
        reconciled, after,
        "normal Store commits must retain connection reuse"
    );
    assert_eq!(receipt, replay);
    assert_eq!(receipt.run_id, admitted.run.id);
    assert_eq!(receipt.operation_id, mutation.id);
    assert_eq!(current.contents, mutation.contents);
    assert_eq!(current.revision, receipt.revision);
    assert_eq!(
        current, after_replay,
        "identical retry must not advance revision"
    );
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, EventKind::PublicStateSnapshot);
    assert_eq!(events[0].cursor, Some(receipt.event_cursor));
    assert_eq!(conflict, Err(crate::PublicStateError::SemanticConflict));
}
