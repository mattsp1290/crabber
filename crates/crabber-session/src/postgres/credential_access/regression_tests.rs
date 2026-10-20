//! A genuine replacement issuer must keep an old projection fenced out.
use crate::Store;
use crabber_core::{Clock, EventKind, ManualClock};
use std::sync::Arc;

#[tokio::test]
async fn raw_execution_keeps_existing_current_issuer_fence_guard() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let admitted = crate::public_state_contract::admit(&store, clock.now()).await;
    let cached = store.execution(admitted.fence.clone()).await.unwrap();
    clock.set(admitted.run.lease_until);
    let replacement = store
        .claim_expired_run(&admitted.run.id, "replacement")
        .await
        .unwrap();
    assert_ne!(replacement.claim_token, admitted.fence.claim_token);
    let current = store.get_run(&admitted.run.id).await.unwrap().unwrap();
    let saved: String = sqlx::query_scalar("SELECT data::text FROM runs WHERE id=$1")
        .bind(&admitted.run.id.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let mut restored = admitted.run.clone();
    restored.lease_until = current.lease_until;
    let before_events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET data=$2::jsonb WHERE id=$1")
        .bind(&admitted.run.id.0)
        .bind(serde_json::to_string(&restored).unwrap())
        .execute(&store.pool)
        .await
        .unwrap();
    let refused = store.execution(admitted.fence.clone()).await.is_err();
    let cached_refused = cached
        .append_event(crate::abandonment_contract::event(
            &admitted.run,
            EventKind::ExtensionNotice,
        ))
        .await
        .is_err();
    sqlx::query("UPDATE runs SET data=$2::jsonb WHERE id=$1")
        .bind(&admitted.run.id.0)
        .bind(saved)
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(store.execution(replacement).await.is_ok());
    let after_events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap();
    assert!(
        refused,
        "restoring a previous projection cannot restore its authority"
    );
    assert!(
        cached_refused,
        "a cached old worker must retain the current issuer fence guard"
    );
    assert_eq!(after_events, before_events);
}
