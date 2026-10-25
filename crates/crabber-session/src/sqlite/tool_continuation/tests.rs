#[tokio::test]
async fn tool_continuation_contract() {
    let (native, _, _directory) = crate::sqlite::tests::temp_store().await;
    crate::tool_continuation_contract::run_contract(|clock| native.with_clock(clock)).await;
}
#[tokio::test]
async fn tool_continuation_faults() {
    let (native, _, _directory) = crate::sqlite::tests::temp_store().await;
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
}

#[tokio::test]
async fn process_helper() {
    let Some(directory) = crate::tool_continuation_contract::process::directory() else {
        return;
    };
    let database = std::env::var("CRABBER_TOOL_SQL_DATABASE").unwrap();
    let native = crate::SqliteStore::connect(database).await.unwrap();
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
    let (_native, database, _directory) = crate::sqlite::tests::temp_store().await;
    crate::tool_continuation_contract::process::journey(
        "sqlite::tool_continuation::tests::process_helper",
        crabber_test_support::ChildDatabase::Sqlite(&database),
    )
    .await;
}

#[tokio::test]
async fn original_checkpoint_origin_and_answers_are_anchored() {
    let (native, _, _directory) = crate::sqlite::tests::temp_store().await;
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
                .fetch_one(&store.writer)
                .await
                .unwrap();
        let mut record: crate::tool_continuation::ToolContinuationRecord =
            serde_json::from_str(&encoded).unwrap();
        crate::tool_continuation_contract::corruption::replace(&mut record, kind);
        sqlx::query("UPDATE tool_continuations SET data_json=$1 WHERE id=$2")
            .bind(serde_json::to_string(&record).unwrap())
            .bind(&fixture.suspension.continuation.id.0)
            .execute(&store.writer)
            .await
            .unwrap();
        if kind >= 8 {
            sqlx::query("DELETE FROM tool_continuation_barriers WHERE session_id=$1")
                .bind(&fixture.admitted.session.id.0)
                .execute(&store.writer)
                .await
                .unwrap();
        }
        if kind == 9 {
            for table in ["tool_continuation_interactions", "tool_continuation_phases"] {
                sqlx::query(&format!("DELETE FROM {table} WHERE continuation_id=$1"))
                    .bind(&fixture.suspension.continuation.id.0)
                    .execute(&store.writer)
                    .await
                    .unwrap();
            }
            let error = sqlx::query("DELETE FROM tool_continuations WHERE id=$1")
                .bind(&fixture.suspension.continuation.id.0)
                .execute(&store.writer)
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
}
