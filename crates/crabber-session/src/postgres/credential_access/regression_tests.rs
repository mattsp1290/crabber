//! A genuine replacement issuer must keep an old projection fenced out.
use crate::Store;
use crabber_core::{Clock, EventKind, ManualClock};
use std::sync::Arc;

#[tokio::test]
async fn raw_execution_keeps_existing_current_issuer_fence_guard() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
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
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn workspace_discovery_excludes_foreign_corrupt_runs_before_decode() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        sqlx::query("TRUNCATE sessions CASCADE")
            .execute(&store.pool)
            .await
            .unwrap();
        let mut runs = Vec::new();
        for workspace in ["discovery-own", "discovery-foreign"] {
            let mut request = crate::execution_binding_contract::fixture(
                &crabber_core::SessionId::new(),
                time::OffsetDateTime::UNIX_EPOCH,
                "discovery-backend",
                'b',
            )
            .keyed
            .request;
            request.session_id = None;
            request.workspace_id = workspace.into();
            runs.push(store.admit_run(request).await.unwrap().run);
        }
        let foreign = &runs[1];
        let saved: String = sqlx::query_scalar("SELECT data::text FROM runs WHERE id=$1")
            .bind(&foreign.id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE runs SET data='{}'::jsonb WHERE id=$1")
            .bind(&foreign.id.0)
            .execute(&store.pool)
            .await
            .unwrap();
        let own = store
            .list_workspace_unfinished_runs(crate::WorkspaceRunsRequest {
                workspace_id: "discovery-own".into(),
            })
            .await;
        let other = store
            .list_workspace_unfinished_runs(crate::WorkspaceRunsRequest {
                workspace_id: "discovery-foreign".into(),
            })
            .await;
        let global = store.list_unfinished_runs().await;
        sqlx::query("UPDATE runs SET data=$2::jsonb WHERE id=$1")
            .bind(&foreign.id.0)
            .bind(saved)
            .execute(&store.pool)
            .await
            .unwrap();
        assert_eq!(own.unwrap(), vec![runs[0].clone()]);
        assert!(other.is_err());
        assert!(global.is_err());
    })
    .await
    .unwrap();
}
