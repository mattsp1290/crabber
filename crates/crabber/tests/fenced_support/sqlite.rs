//! Public facade parity on independent temporary SQLite databases.
use super::*;
use crabber::session::SqliteStore;
use sqlx::{Connection, SqliteConnection, SqlitePool, sqlite::SqliteConnectOptions};
use std::path::{Path, PathBuf};
#[path = "sqlite/fixture.rs"]
mod fixture_store;
#[path = "sqlite/process.rs"]
mod process;
use fixture_store::{SqliteFixture, snapshot};
use process::*;

async fn database() -> (tempfile::TempDir, PathBuf, Arc<SqliteFixture>) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("facade.sqlite");
    SqliteStore::migrate(&path).await.unwrap();
    let store = Arc::new(SqliteFixture::connect(&path).await);
    (directory, path, store)
}
#[tokio::test]
async fn public_sqlite_pending_running_paused_zero_execution() {
    let (_directory, _path, mut store) = database().await;
    let clock = Arc::new(ManualClock::new(SystemClock.now()));
    let fixture = Arc::get_mut(&mut store).unwrap();
    fixture.store = fixture.store.clone().with_clock(clock.clone());
    facade_matrix(store, clock).await;
}
async fn fixture(store: &dyn Store) -> (crabber::session::AdmitOutcome, AbandonRequest) {
    let admitted = store
        .admit_run(AdmitRequest {
            input_envelope: None,
            additional_messages: Vec::new(),
            expected_revision: None,
            session_id: None,
            ..admit_request(&SessionId::new())
        })
        .await
        .unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    for running in [false, true] {
        let call = ToolCallRecord {
            id: ToolCallId::new(),
            run_id: admitted.run.id.clone(),
            name: "unsafe-unavailable".into(),
            arguments: serde_json::json!({}),
            status: ToolCallStatus::Pending,
            retry_safe: false,
            result: None,
        };
        execution
            .create_tool_call(
                call.clone(),
                event(&admitted.run, EventKind::ToolCallPending),
            )
            .await
            .unwrap();
        if running {
            execution
                .claim_tool_call(&call.id, event(&admitted.run, EventKind::ToolCallRunning))
                .await
                .unwrap();
        }
    }
    let request = AbandonRequest {
        expected: admitted.fence.clone(),
        expected_owner: admitted.run.owner.clone(),
        authority: AbandonAuthority::HostStoppedOwner,
    };
    (admitted, request)
}
async fn trigger(
    pool: &SqlitePool,
    run: &Run,
    table: &str,
    condition: &str,
    boundary: &str,
) -> String {
    let name = format!("abandon_{}", run.id.0.replace('-', ""));
    let operation = match boundary {
        "fence" | "run" | "tool" => "UPDATE",
        _ => "INSERT",
    };
    sqlx::query(&format!("CREATE TRIGGER {name} BEFORE {operation} ON {table} WHEN {condition} BEGIN SELECT RAISE(ABORT, 'injected abandon boundary'); END"))
        .execute(pool).await.unwrap();
    name
}
async fn drop_trigger(pool: &SqlitePool, name: &str) {
    sqlx::query(&format!("DROP TRIGGER {name}"))
        .execute(pool)
        .await
        .unwrap();
}
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn each_atomic_write_failure_rolls_back_private_commit_and_retries() {
    let (_directory, _path, store) = database().await;
    let pool = &store.raw;
    for boundary in [
        "fence",
        "tool",
        "message",
        "part",
        "tool_event",
        "run",
        "terminal_event",
        "private_commit",
    ] {
        let (admitted, request) = fixture(store.as_ref()).await;
        let run = &admitted.run;
        let (table, condition) = match boundary {
            "fence" => (
                "runs",
                format!(
                    "NEW.id='{}' AND json_extract(NEW.data, '$.claim_token') <> json_extract(OLD.data, '$.claim_token')",
                    run.id
                ),
            ),
            "tool" => (
                "tool_calls",
                format!("NEW.run_id='{}' AND NEW.status='interrupted'", run.id),
            ),
            "message" => (
                "messages",
                format!(
                    "NEW.run_id='{}' AND json_extract(NEW.data, '$.role')='tool'",
                    run.id
                ),
            ),
            "part" => (
                "parts",
                format!(
                    "EXISTS (SELECT 1 FROM messages WHERE id=NEW.message_id AND run_id='{}')",
                    run.id
                ),
            ),
            "tool_event" => (
                "events",
                format!(
                    "NEW.run_id='{}' AND json_extract(NEW.data, '$.kind')='tool_call_settled'",
                    run.id
                ),
            ),
            "run" => (
                "runs",
                format!("NEW.id='{}' AND NEW.status='interrupted'", run.id),
            ),
            "terminal_event" => (
                "events",
                format!(
                    "NEW.run_id='{}' AND json_extract(NEW.data, '$.kind')='run_settled'",
                    run.id
                ),
            ),
            _ => ("abandonment_commits", format!("NEW.run_id='{}'", run.id)),
        };
        let original = snapshot(pool, run).await;
        let name = trigger(pool, run, table, &condition, boundary).await;
        let count = Arc::new(AtomicUsize::new(0));
        assert!(
            matches!(
                agent(store.clone(), count.clone())
                    .abandon(request.clone())
                    .await,
                Err(AbandonError::Store(_))
            ),
            "{boundary}"
        );
        assert_eq!(snapshot(pool, run).await, original, "{boundary}");
        drop_trigger(pool, &name).await;
        let outcome = agent(store.clone(), count.clone())
            .abandon(request.clone())
            .await
            .unwrap();
        assert_eq!(outcome.run.status, RunStatus::Interrupted);
        assert_eq!(outcome.interrupted_tools.len(), 2);
        let committed = snapshot(pool, run).await;
        assert_eq!(
            agent(store.clone(), count.clone())
                .abandon(request)
                .await
                .unwrap(),
            outcome
        );
        assert_eq!(snapshot(pool, run).await, committed);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        println!("atomic boundary {boundary}: unchanged rollback, complete exact retry");
    }
    pool.close().await;
}

#[tokio::test]
async fn abandonment_process_helper() {
    let Ok(encoded) = std::env::var("CRABBER_SQLITE_FACADE_CHILD_REQUEST") else {
        return;
    };
    let request: AbandonRequest = serde_json::from_str(&encoded).unwrap();
    let store = Arc::new(
        SqliteFixture::connect(Path::new(
            &std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap(),
        ))
        .await,
    );
    let run = store
        .get_run(&request.expected.run_id)
        .await
        .unwrap()
        .unwrap();
    if let Ok(expected) = std::env::var("CRABBER_SQLITE_FACADE_CHILD_SNAPSHOT") {
        assert_eq!(
            snapshot(&store.raw, &run).await,
            serde_json::from_str::<serde_json::Value>(&expected).unwrap()
        );
    }
    let counters = Arc::new(AtomicUsize::new(0));
    let host = agent(store.clone(), counters.clone());
    println!("CRABBER_SQLITE_FACADE_READY");
    std::io::Write::flush(&mut std::io::stdout()).unwrap();
    let outcome = host.abandon(request.clone()).await.unwrap();
    let committed = snapshot(&store.raw, &outcome.run).await;
    assert_eq!(host.abandon(request).await.unwrap(), outcome);
    assert_eq!(snapshot(&store.raw, &outcome.run).await, committed);
    assert_eq!(counters.load(Ordering::SeqCst), 0);
    assert_eq!(outcome.interrupted_tools.len(), 2);
    println!(
        "fresh abandonment process: run={} event={:?} status=Interrupted effects=0",
        outcome.run.id, outcome.terminal_event.cursor
    );
}

#[tokio::test]
async fn physical_abandonment_writer_crash_then_fresh_process_retry() {
    let _guard = BARRIER_LOCK.lock().await;
    let (_directory, path, store) = database().await;
    let (admitted, request) = fixture(store.as_ref()).await;
    let original = snapshot(&store.raw, &admitted.run).await;
    sqlx::raw_sql("CREATE TABLE crabber_test_barrier(n INTEGER); WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<1000) INSERT INTO crabber_test_barrier SELECT n FROM numbers;")
        .execute(&store.raw).await.unwrap();
    let name = format!("crash_{}", admitted.run.id.0.replace('-', ""));
    sqlx::query(&format!("CREATE TRIGGER {name} AFTER INSERT ON abandonment_commits WHEN NEW.run_id='{}' BEGIN SELECT count(*) FROM crabber_test_barrier a, crabber_test_barrier b, crabber_test_barrier c, crabber_test_barrier d; END",admitted.run.id))
        .execute(&store.raw).await.unwrap();
    let mut writer = child(&path, &request, None);
    ready_line(&mut writer).await;
    let mut independent = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&path)
            .busy_timeout(Duration::ZERO),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match independent.begin_with("BEGIN IMMEDIATE").await {
                Ok(tx) => tx.rollback().await.unwrap(),
                Err(error) => {
                    assert_busy_error(&error);
                    break;
                }
            }
            assert!(writer.0.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("child must acquire the database write lock");
    // Public-API evidence cannot identify the exact statement: the accepted
    // facade reduction observes this call holding the lock while the child lives.
    let until = tokio::time::Instant::now() + Duration::from_secs(1);
    while tokio::time::Instant::now() < until {
        assert!(writer.0.try_wait().unwrap().is_none());
        assert_busy_error(
            &independent
                .begin_with("BEGIN IMMEDIATE")
                .await
                .expect_err("child must retain write lock"),
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    writer.kill();
    independent
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
    assert_eq!(snapshot(&store.raw, &admitted.run).await, original);
    drop_trigger(&store.raw, &name).await;
    child(&path, &request, Some(&original)).wait().await;
    let committed = snapshot(&store.raw, &admitted.run).await;
    let outcome = agent(store.clone(), Arc::new(AtomicUsize::new(0)))
        .abandon(request.clone())
        .await
        .unwrap();
    assert_eq!(outcome.run.status, RunStatus::Interrupted);
    assert_eq!(snapshot(&store.raw, &admitted.run).await, committed);
    child(&path, &request, Some(&committed)).wait().await;
    store.raw.close().await;
}

#[tokio::test]
async fn sqlite_stopped_process_and_error_semantics() {
    let (_directory, _path, store) = database().await;
    stopped_owner(store.clone()).await;
    error_semantics(store.clone()).await;
    store.raw.close().await;
}
#[tokio::test]
async fn facade_committed_response_loss_fresh_host_process_exact_replay() {
    let (_directory, path, store) = database().await;
    let (request, outcome) = response_loss::verify_loss(store.clone()).await;
    let original = snapshot(&store.raw, &outcome.run).await;
    child(&path, &request, Some(&original)).wait().await;
    assert_eq!(snapshot(&store.raw, &outcome.run).await, original);
    store.raw.close().await;
}
