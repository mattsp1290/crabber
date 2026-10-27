use crate::*;
#[tokio::test]
async fn newest_tools_and_raw_carrier_bytes() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        PostgresStore::migrate(&url).await.unwrap();
        let store = PostgresStore::connect(&url).await.unwrap();
        crate::public_history::newest_tests::tools(&store).await;
        let (session, _) = crate::public_history::tests::seed(&store).await;
        sqlx::query("UPDATE messages SET snapshot_record=$2||snapshot_record,snapshot_bytes=snapshot_bytes+4096 WHERE session_id=$1")
            .bind(&session.0).bind(" ".repeat(4096)).execute(&store.pool).await.unwrap();
        let total: i64 = sqlx::query_scalar("SELECT CAST(sum(snapshot_bytes) AS BIGINT) FROM messages WHERE session_id=$1")
            .bind(&session.0).fetch_one(&store.pool).await.unwrap();
        let newest: i64 = sqlx::query_scalar("SELECT snapshot_bytes FROM messages WHERE session_id=$1 ORDER BY seq DESC LIMIT 1")
            .bind(&session.0).fetch_one(&store.pool).await.unwrap();
        crate::public_history::newest_tests::raw_bytes(&store, &session, usize::try_from(total).unwrap(), usize::try_from(newest).unwrap()).await;
    }).await.unwrap();
}
