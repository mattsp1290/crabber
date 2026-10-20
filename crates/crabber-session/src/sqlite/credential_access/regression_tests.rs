//! Genuine cached credentials must keep row routing and current issuer guards.
use crate::Store;
use crabber_core::{Clock, EventKind, ManualClock};
use std::sync::Arc;

#[tokio::test]
async fn cached_raw_execution_cannot_borrow_a_distinct_runs_origin() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cached-raw-routing.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = crate::SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let first = crate::public_state_contract::admit(&store, clock.now()).await;
    let other = crate::public_state_contract::admit(&store, clock.now()).await;
    let cached_first = store.execution(first.fence.clone()).await.unwrap();
    let cached_other = store.execution(other.fence.clone()).await.unwrap();
    let mut event = crate::abandonment_contract::event(&other.run, EventKind::ExtensionNotice);
    event.payload = serde_json::json!({"credential_test":"genuine-other-positive"});
    cached_other.append_event(event.clone()).await.unwrap();
    let before_other = store
        .list_events(&other.session.id, None, 100)
        .await
        .unwrap();
    let before_first = store
        .list_events(&first.session.id, None, 100)
        .await
        .unwrap();
    let saved: String = sqlx::query_scalar("SELECT data FROM runs WHERE id=$1")
        .bind(&first.run.id.0)
        .fetch_one(&store.readers)
        .await
        .unwrap();
    let mut foreign = other.run.clone();
    foreign.claim_token = first.fence.claim_token.clone();
    sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
        .bind(&first.run.id.0)
        .bind(serde_json::to_string(&foreign).unwrap())
        .execute(&store.writer)
        .await
        .unwrap();
    event.payload = serde_json::json!({"credential_test":"cross-row-must-refuse"});
    let refused = cached_first.append_event(event).await.is_err();
    // Restore the exact row before assertions or any ordinary fixture access.
    sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
        .bind(&first.run.id.0)
        .bind(saved)
        .execute(&store.writer)
        .await
        .unwrap();
    let after_other = store
        .list_events(&other.session.id, None, 100)
        .await
        .unwrap();
    let after_first = store
        .list_events(&first.session.id, None, 100)
        .await
        .unwrap();
    cached_first
        .append_event(crate::abandonment_contract::event(
            &first.run,
            EventKind::ExtensionNotice,
        ))
        .await
        .unwrap();
    assert!(
        refused,
        "a genuine cached worker must refuse a foreign Run projection"
    );
    assert_eq!(
        after_other, before_other,
        "cached A must not mutate genuine B"
    );
    assert_eq!(after_first, before_first);
}

#[tokio::test]
async fn raw_execution_keeps_existing_current_issuer_fence_guard() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("raw-current-fence.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = crate::SqliteStore::connect(&path)
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
    let saved: String = sqlx::query_scalar("SELECT data FROM runs WHERE id=$1")
        .bind(&admitted.run.id.0)
        .fetch_one(&store.readers)
        .await
        .unwrap();
    let mut restored = admitted.run.clone();
    restored.lease_until = current.lease_until;
    let before_events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
        .bind(&admitted.run.id.0)
        .bind(serde_json::to_string(&restored).unwrap())
        .execute(&store.writer)
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
    sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
        .bind(&admitted.run.id.0)
        .bind(saved)
        .execute(&store.writer)
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
