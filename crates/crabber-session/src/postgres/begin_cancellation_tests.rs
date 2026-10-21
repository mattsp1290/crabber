//! Regression only: cancellation while the server has begun but `SQLx` has not acknowledged BEGIN.
mod finishing;
mod proxy;
mod store_commit;

use proxy::Proxy;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{str::FromStr, time::Duration};
use tokio::{sync::oneshot, time::timeout};

#[derive(Clone, Copy)]
enum Case {
    Complete,
    CancelEstablished,
    CancelBegin,
}

async fn begin_case(pool: &PgPool, proxy: &Proxy, case: Case) {
    if matches!(case, Case::Complete) {
        crate::postgres::transaction::begin(pool)
            .await
            .unwrap()
            .rollback()
            .await
            .unwrap();
        return;
    }
    let (established, ready) = oneshot::channel();
    if matches!(case, Case::CancelBegin) {
        proxy.arm();
    }
    let pool = pool.clone();
    let task = tokio::spawn(async move {
        let _transaction = crate::postgres::transaction::begin(&pool).await.unwrap();
        let _ = established.send(());
        std::future::pending::<()>().await;
    });
    if matches!(case, Case::CancelBegin) {
        timeout(Duration::from_secs(5), proxy.held()).await.unwrap();
    } else {
        timeout(Duration::from_secs(5), ready)
            .await
            .unwrap()
            .unwrap();
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    proxy.release();
}

async fn safe_reuse(url: &str, case: Case) -> (bool, Option<String>, bool) {
    let options = sqlx::postgres::PgConnectOptions::from_str(url).unwrap();
    let proxy = Proxy::start(&options).await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(proxy.options(options))
        .await
        .unwrap();
    let before: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    begin_case(&pool, &proxy, case).await;
    // A real ordinary Store query between cancellation and the next snapshot is legal.
    // It also distinguishes a leaked, previously queried transaction from an empty BEGIN.
    let after: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let mut transaction = crate::postgres::transaction::begin(&pool).await.unwrap();
    let result = sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await;
    let error_code = result
        .as_ref()
        .err()
        .and_then(sqlx::Error::as_database_error)
        .and_then(|error| error.code().map(std::borrow::Cow::into_owned));
    let safe = result.is_ok();
    // Observe all real cleanup before asserting the regression; failure must not leak sockets.
    transaction.rollback().await.unwrap();
    pool.close().await;
    proxy.join().await;
    (safe, error_code, before == after)
}

#[tokio::test]
async fn cancelled_begin_never_returns_an_open_transaction_to_the_pool() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let complete = safe_reuse(&url, Case::Complete).await;
        let established = safe_reuse(&url, Case::CancelEstablished).await;
        let cancelled = safe_reuse(&url, Case::CancelBegin).await;
        assert!(complete.0, "no-cancel control failed: {:?}", complete.1);
        assert!(
            complete.2,
            "ordinary completed transaction must reuse its connection"
        );
        assert!(
            established.0,
            "established-Tx cancellation failed: {:?}",
            established.1
        );
        assert!(
            established.2,
            "established transaction Drop must retain normal reuse"
        );
        assert!(
            cancelled.0,
            "cancelled BEGIN leaked its transaction; SQLSTATE {:?}, reused original backend {}",
            cancelled.1, cancelled.2
        );
        assert!(
            !cancelled.2,
            "cancelled opening owner must discard its original connection"
        );
    })
    .await
    .unwrap();
}
