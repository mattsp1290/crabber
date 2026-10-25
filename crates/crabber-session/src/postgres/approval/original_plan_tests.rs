//! A bounded origin read must survive unrelated oversized private carriers, but reject a changed batch.
use crate::Store;
#[tokio::test]
async fn original_plans_select_only_authenticated_original_batches() {
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
        for (approval, table, session, run, target, plan) in cases(&store, &clock).await {
            let revision = store.session_revision(&session).await.unwrap();
            let saved_run: sqlx::types::Json<serde_json::Value> =
                sqlx::query_scalar("SELECT data FROM runs WHERE id=$1")
                    .bind(&run.0)
                    .fetch_one(&store.pool)
                    .await
                    .unwrap();
            let mut changed_run = saved_run.0.clone();
            let saved_record: String =
                sqlx::query_scalar(&format!("SELECT data_json FROM {table} WHERE id=$1"))
                    .bind(&target.id.0)
                    .fetch_one(&store.pool)
                    .await
                    .unwrap();
            let mut record: serde_json::Value = serde_json::from_str(&saved_record).unwrap();
            changed_run["plan_fingerprint"] = serde_json::json!("mutable replacement");
            changed_run["checkpoint"] = serde_json::json!({"private":"x".repeat(4*1024*1024)});
            record["workspace_id"] = serde_json::json!("x".repeat(4 * 1024 * 1024));
            sqlx::query("UPDATE runs SET data=$1 WHERE id=$2")
                .bind(sqlx::types::Json(changed_run))
                .bind(&run.0)
                .execute(&store.pool)
                .await
                .unwrap();
            sqlx::query(&format!("UPDATE {table} SET data_json=$1 WHERE id=$2"))
                .bind(serde_json::to_string(&record).unwrap())
                .bind(&target.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
            let actual = if approval {
                store
                    .original_approval_plan(&session, &run, &target)
                    .await
                    .unwrap()
            } else {
                store
                    .original_tool_continuation_plan(&session, &run, &target)
                    .await
                    .unwrap()
            };
            assert_eq!(actual, plan);
            record["batch"]["expected_plan"] = serde_json::json!("f".repeat(64));
            sqlx::query(&format!("UPDATE {table} SET data_json=$1 WHERE id=$2"))
                .bind(serde_json::to_string(&record).unwrap())
                .bind(&target.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
            if approval {
                assert_eq!(
                    store
                        .original_approval_plan(&session, &run, &target)
                        .await
                        .unwrap_err(),
                    crate::ApprovalError::UnknownStoreOutcome
                );
            } else {
                assert_eq!(
                    store
                        .original_tool_continuation_plan(&session, &run, &target)
                        .await
                        .unwrap_err(),
                    crate::ToolContinuationError::UnknownStoreOutcome
                );
            }
            assert_eq!(store.session_revision(&session).await.unwrap(), revision);
            sqlx::query(&format!("UPDATE {table} SET data_json=$1 WHERE id=$2"))
                .bind(saved_record)
                .bind(&target.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
            sqlx::query("UPDATE runs SET data=$1 WHERE id=$2")
                .bind(saved_run)
                .bind(&run.0)
                .execute(&store.pool)
                .await
                .unwrap();
        }
    })
    .await
    .unwrap();
}

type Case = (
    bool,
    &'static str,
    crabber_core::SessionId,
    crabber_core::RunId,
    crate::ContinuationTarget,
    crabber_core::InputFingerprint,
);
async fn cases(store: &impl Store, clock: &crabber_core::ManualClock) -> [Case; 2] {
    let a = crate::approval_contract::fixture::staged(store, clock).await;
    a.execution
        .suspend_for_approval(a.batch.clone(), a.owner.clone())
        .await
        .unwrap();
    let t = crate::tool_continuation_contract::fixture::fixture(store).await;
    t.execution
        .suspend_tool_continuation(t.suspension.clone(), t.authority.clone())
        .await
        .unwrap();
    [
        (
            true,
            "continuations",
            a.run.session_id.clone(),
            a.run.id.clone(),
            a.batch.continuation.clone(),
            a.batch.expected_plan.clone(),
        ),
        (
            false,
            "tool_continuations",
            t.admitted.session.id.clone(),
            t.admitted.run.id.clone(),
            t.suspension.continuation.clone(),
            t.suspension.expected_plan.clone(),
        ),
    ]
}
