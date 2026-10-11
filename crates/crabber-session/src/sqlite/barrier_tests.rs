//! G2: prove the two crash barriers before building the durability suite.
use super::{test_support::*, tests::*, *};
use crate::Store;
use crabber_core::SessionId;
use std::io::Write;

#[tokio::test]
async fn barrier_child() {
    let Ok(path) = std::env::var("CRABBER_SQLITE_CHILD_PATH") else {
        return;
    };
    let Ok(mode) = std::env::var("CRABBER_SQLITE_BARRIER_MODE") else {
        return;
    };
    let path = Path::new(&path);
    let store = if mode == "collation" {
        connect_with_test_options(
            path,
            TestOptions {
                barrier: Some(
                    std::env::var("CRABBER_SQLITE_BARRIER_MARKER")
                        .unwrap()
                        .into(),
                ),
                ..TestOptions::default()
            },
        )
        .await
    } else {
        SqliteStore::connect(path).await.unwrap()
    };
    println!("CRABBER_SQLITE_BARRIER_READY");
    std::io::stdout().flush().unwrap();
    store.admit_run(request(&SessionId::new())).await.unwrap();
    panic!("barrier must not complete before the parent kills the child");
}

async fn assert_rollback(store: &SqliteStore, connection: &mut SqliteConnection) {
    connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
    for table in ["sessions", "runs", "epochs", "messages", "parts"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&store.readers)
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "uncommitted {table} must not survive killing the process"
        );
    }
}

#[tokio::test]
async fn collation_crash_barrier_rolls_back() {
    let _guard = BARRIER_LOCK.lock().await;
    let (store, path, directory) = temp_store().await;
    let marker_path = directory.path().join("barrier.marker");
    sqlx::query("CREATE TRIGGER crash_barrier AFTER INSERT ON runs BEGIN SELECT 1 WHERE 'a' < 'b' COLLATE crabber_test_barrier; END")
        .execute(&store.writer).await.unwrap();
    let mut process = child(
        &path,
        "sqlite::barrier_tests::barrier_child",
        &[
            ("CRABBER_SQLITE_BARRIER_MODE", "collation".into()),
            (
                "CRABBER_SQLITE_BARRIER_MARKER",
                marker_path.to_str().unwrap().into(),
            ),
        ],
    );
    marker(&marker_path, &mut process).await;
    let mut connection = raw(&path).await;
    assert_busy(&mut connection).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs")
            .fetch_one(&store.readers)
            .await
            .unwrap(),
        0
    );
    process.kill();
    assert_rollback(&store, &mut connection).await;
    sqlx::query("DROP TRIGGER crash_barrier")
        .execute(&store.writer)
        .await
        .unwrap();
    store.admit_run(request(&SessionId::new())).await.unwrap();
}

#[tokio::test]
async fn public_cross_join_crash_barrier_rolls_back() {
    let _guard = BARRIER_LOCK.lock().await;
    let (store, path, _directory) = temp_store().await;
    sqlx::raw_sql("CREATE TABLE crabber_test_barrier(n INTEGER); WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<1000) INSERT INTO crabber_test_barrier SELECT n FROM numbers; CREATE TRIGGER crash_barrier AFTER INSERT ON runs BEGIN SELECT count(*) FROM crabber_test_barrier a, crabber_test_barrier b, crabber_test_barrier c, crabber_test_barrier d; END;")
        .execute(&store.writer).await.unwrap();
    let mut process = child(
        &path,
        "sqlite::barrier_tests::barrier_child",
        &[("CRABBER_SQLITE_BARRIER_MODE", "public".into())],
    );
    ready_line(&mut process).await;
    let mut connection = raw(&path).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match connection.begin_with("BEGIN IMMEDIATE").await {
                Ok(tx) => tx.rollback().await.unwrap(),
                Err(error) => {
                    assert_eq!(primary_code(&error), Some(5));
                    break;
                }
            }
            assert!(process.0.try_wait().unwrap().is_none());
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("child must acquire the lock after announcing its store call");
    // Public-API fixture cannot identify the exact statement: accepted G2 reduction.
    let until = tokio::time::Instant::now() + Duration::from_secs(1);
    while tokio::time::Instant::now() < until {
        assert!(process.0.try_wait().unwrap().is_none());
        assert_busy(&mut connection).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    process.kill();
    assert_rollback(&store, &mut connection).await;
    sqlx::query("DROP TRIGGER crash_barrier")
        .execute(&store.writer)
        .await
        .unwrap();
    store.admit_run(request(&SessionId::new())).await.unwrap();
}
