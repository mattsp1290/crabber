#[tokio::test]
async fn tool_continuation_contract() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let native = crate::PostgresStore::connect(&url).await.unwrap();
        crate::tool_continuation_contract::run_contract(|clock| native.with_clock(clock)).await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn tool_continuation_faults() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let native = crate::PostgresStore::connect(&url).await.unwrap();
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = native.with_clock(clock.clone());
        crate::tool_continuation_contract::run_fault_contract(&store, &clock, |fault| {
            store
                .tool_continuation_fault
                .store(fault, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn process_helper() {
    let Some(directory) = crate::tool_continuation_contract::process::directory() else {
        return;
    };
    let url = crabber_test_support::postgres::TestDatabase::attached_url()
        .await
        .unwrap();
    let native = crate::PostgresStore::connect(&url).await.unwrap();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = native.with_clock(clock.clone());
    if std::env::var_os("CRABBER_TOOL_SQL_WRITE").is_some() {
        crate::tool_continuation_contract::process::write(&store, &clock, &directory).await;
    } else {
        crate::tool_continuation_contract::process::verify(&store, &directory).await;
    }
}
#[tokio::test]
async fn committed_controls_survive_killed_writer() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        crate::tool_continuation_contract::process::journey(
            "postgres::tool_continuation::tests::process_helper",
            crabber_test_support::ChildDatabase::Postgres(&url),
        )
        .await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn original_checkpoint_origin_and_answers_are_anchored() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let native = crate::PostgresStore::connect(&url).await.unwrap();
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = native.with_clock(clock.clone());
        for kind in 0..10 {
            let fixture =
                crate::tool_continuation_contract::corruption::setup(&store, &clock, kind).await;
            let encoded: String =
                sqlx::query_scalar("SELECT data_json FROM tool_continuations WHERE id=$1")
                    .bind(&fixture.suspension.continuation.id.0)
                    .fetch_one(&store.pool)
                    .await
                    .unwrap();
            let mut record: crate::tool_continuation::ToolContinuationRecord =
                serde_json::from_str(&encoded).unwrap();
            crate::tool_continuation_contract::corruption::replace(&mut record, kind);
            sqlx::query("UPDATE tool_continuations SET data_json=$1 WHERE id=$2")
                .bind(serde_json::to_string(&record).unwrap())
                .bind(&fixture.suspension.continuation.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
            if kind >= 8 {
                sqlx::query("DELETE FROM tool_continuation_barriers WHERE session_id=$1")
                    .bind(&fixture.admitted.session.id.0)
                    .execute(&store.pool)
                    .await
                    .unwrap();
            }
            if kind == 9 {
                for table in ["tool_continuation_interactions", "tool_continuation_phases"] {
                    sqlx::query(&format!("DELETE FROM {table} WHERE continuation_id=$1"))
                        .bind(&fixture.suspension.continuation.id.0)
                        .execute(&store.pool)
                        .await
                        .unwrap();
                }
                let error = sqlx::query("DELETE FROM tool_continuations WHERE id=$1")
                    .bind(&fixture.suspension.continuation.id.0)
                    .execute(&store.pool)
                    .await
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("crabber_continuation_input_identity")
                );
            }
            crate::tool_continuation_contract::corruption::refused(&store, &fixture, kind).await;
        }
    })
    .await
    .unwrap();
}
