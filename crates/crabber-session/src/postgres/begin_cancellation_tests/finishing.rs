//! Cancel the actual owned or borrowed operation after PostgreSQL has applied it.
use super::*;
use std::future::Future;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Commit,
    Rollback,
    Savepoint,
    Release,
    RollbackTo,
}

async fn cancel_ack(operation: impl Future, proxy: &Proxy) {
    let mut original = Box::pin(operation);
    tokio::select! {
        _ = &mut original => panic!("transaction returned before its held acknowledgment"),
        held = timeout(Duration::from_secs(5), proxy.held()) => held.unwrap(),
    }
    // Dispose the same incomplete future, then release the real server frame.
    drop(original);
    proxy.release();
}

async fn cancellation(url: &str, operation: Operation) -> (bool, i64, bool) {
    let options = sqlx::postgres::PgConnectOptions::from_str(url).unwrap();
    let proxy = Proxy::start(&options).await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(proxy.options(options))
        .await
        .unwrap();
    let table = format!("cancellation_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"))
        .execute(&pool)
        .await
        .unwrap();
    let before: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let mut parent = crate::postgres::transaction::begin(&pool).await.unwrap();
    sqlx::query(&format!("INSERT INTO {table} VALUES (1)"))
        .execute(&mut *parent)
        .await
        .unwrap();
    let mut parent_refused = true;
    match operation {
        Operation::Commit => {
            proxy.arm_query(b"COMMIT\0", b'I');
            cancel_ack(parent.commit(), &proxy).await;
        }
        Operation::Rollback => {
            proxy.arm_query(b"ROLLBACK\0", b'I');
            cancel_ack(parent.rollback(), &proxy).await;
        }
        Operation::Savepoint => {
            proxy.arm_query(b"SAVEPOINT _sqlx_savepoint_1\0", b'T');
            cancel_ack(parent.begin(), &proxy).await;
            parent_refused = parent.commit().await.is_err();
        }
        Operation::Release | Operation::RollbackTo => {
            let child = parent.begin().await.unwrap();
            if matches!(operation, Operation::Release) {
                proxy.arm_query(b"RELEASE SAVEPOINT _sqlx_savepoint_1\0", b'T');
                cancel_ack(child.commit(), &proxy).await;
            } else {
                proxy.arm_query(b"ROLLBACK TO SAVEPOINT _sqlx_savepoint_1\0", b'T');
                cancel_ack(child.rollback(), &proxy).await;
            }
            parent_refused = parent.commit().await.is_err();
        }
    }
    let after: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(&pool)
        .await
        .unwrap();
    let mut fresh = crate::postgres::transaction::begin(&pool).await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *fresh)
        .await
        .unwrap();
    fresh.rollback().await.unwrap();
    sqlx::query(&format!("DROP TABLE {table}"))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    proxy.join().await;
    (before != after, count, parent_refused)
}

#[tokio::test]
async fn cancelled_finishing_and_savepoint_acknowledgments_discard_the_original_connection() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        for operation in [
            Operation::Commit,
            Operation::Rollback,
            Operation::Savepoint,
            Operation::Release,
            Operation::RollbackTo,
        ] {
            let actual = cancellation(&url, operation).await;
            assert!(actual.0, "{operation:?} reused an ambiguous connection");
            assert_eq!(
                actual.1,
                i64::from(matches!(operation, Operation::Commit)),
                "{operation:?}: actual committed data must survive loss of acknowledgment"
            );
            assert!(
                actual.2,
                "{operation:?}: ambiguous borrowed operation must poison its real parent"
            );
        }
    })
    .await
    .unwrap();
}
