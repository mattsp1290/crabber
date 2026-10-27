use crate::*;
#[tokio::test]
async fn newest_tools_and_raw_carrier_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("newest.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::connect(path).await.unwrap();
    crate::public_history::newest_tests::tools(&store).await;
    let (session, _) = crate::public_history::tests::seed(&store).await;
    sqlx::query(
        "UPDATE messages SET data=$2||data,snapshot_bytes=snapshot_bytes+4096 WHERE session_id=$1",
    )
    .bind(&session.0)
    .bind(" ".repeat(4096))
    .execute(&store.writer)
    .await
    .unwrap();
    let total: i64 = sqlx::query_scalar(
        "SELECT CAST(sum(snapshot_bytes) AS BIGINT) FROM messages WHERE session_id=$1",
    )
    .bind(&session.0)
    .fetch_one(&store.readers)
    .await
    .unwrap();
    let newest: i64 = sqlx::query_scalar(
        "SELECT snapshot_bytes FROM messages WHERE session_id=$1 ORDER BY seq DESC LIMIT 1",
    )
    .bind(&session.0)
    .fetch_one(&store.readers)
    .await
    .unwrap();
    crate::public_history::newest_tests::raw_bytes(
        &store,
        &session,
        usize::try_from(total).unwrap(),
        usize::try_from(newest).unwrap(),
    )
    .await;
}
