use super::*;
use crabber::session::PostgresStore;
use sqlx::{PgPool, Row};
use std::process::{Command, Stdio};
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn url() -> Option<String> {
    match std::env::var("CRABBER_TEST_POSTGRES_URL") {
        Ok(value) => Some(value),
        Err(error) if std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref() == Ok("1") => {
            panic!("CRABBER_TEST_POSTGRES_URL is required: {error}")
        }
        Err(_) => None,
    }
}
async fn pool() -> Option<PgPool> {
    let url = url()?;
    Some(PgPool::connect(&url).await.unwrap())
}
#[async_trait]
impl abandonment_contract::FixtureStore for PostgresStore {
    async fn seed_run(&self, run: Run) {
        let pool = pool().await.unwrap();
        sqlx::query("UPDATE runs SET status=$2,data=$3,lease_until=$4 WHERE id=$1")
            .bind(&run.id.0)
            .bind(match run.status {
                RunStatus::Pending => "pending",
                RunStatus::Running => "running",
                RunStatus::Paused => "paused",
                _ => panic!(),
            })
            .bind(serde_json::to_value(&run).unwrap())
            .bind(i64::try_from(run.lease_until.unix_timestamp_nanos() / 1_000).unwrap())
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
    }
    async fn calls(&self, run: &RunId) -> Vec<ToolCallRecord> {
        let pool = pool().await.unwrap();
        let rows = sqlx::query("SELECT data FROM tool_calls WHERE run_id=$1 ORDER BY id")
            .bind(&run.0)
            .fetch_all(&pool)
            .await
            .unwrap();
        pool.close().await;
        rows.into_iter()
            .map(|r| serde_json::from_value(r.get("data")).unwrap())
            .collect()
    }
    async fn unconsumed_inbox(&self, session: &SessionId) -> usize {
        let pool = pool().await.unwrap();
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM inbox WHERE session_id=$1 AND consumed_by_run IS NULL",
        )
        .bind(&session.0)
        .fetch_one(&pool)
        .await
        .unwrap();
        pool.close().await;
        usize::try_from(count).unwrap()
    }
}
#[tokio::test]
async fn public_postgres_pending_running_paused_zero_execution() {
    let _guard = TEST_LOCK.lock().await;
    let Some(config) = url() else { return };
    PostgresStore::migrate(&config).await.unwrap();
    let Some(url) = url() else { return };
    PostgresStore::migrate(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(SystemClock.now()));
    let store = Arc::new(
        PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone()),
    );
    facade_matrix(store, clock).await;
}

async fn snapshot(pool: &PgPool, run: &Run) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    for (table, filter) in [
        ("runs", "id"),
        ("tool_calls", "run_id"),
        ("messages", "run_id"),
        ("events", "run_id"),
        ("abandonment_commits", "run_id"),
    ] {
        let query = format!(
            "SELECT to_jsonb(t) AS row FROM {table} t WHERE {filter}=$1 ORDER BY to_jsonb(t)::text"
        );
        let rows = sqlx::query(&query)
            .bind(&run.id.0)
            .fetch_all(pool)
            .await
            .unwrap();
        result.insert(
            table.into(),
            serde_json::Value::Array(rows.into_iter().map(|row| row.get("row")).collect()),
        );
    }
    let rows = sqlx::query("SELECT to_jsonb(p) AS row FROM parts p JOIN messages m ON m.id=p.message_id WHERE m.run_id=$1 ORDER BY p.id").bind(&run.id.0).fetch_all(pool).await.unwrap();
    result.insert(
        "parts".into(),
        serde_json::Value::Array(rows.into_iter().map(|r| r.get("row")).collect()),
    );
    serde_json::Value::Object(result)
}
async fn fixture(store: &PostgresStore) -> (crabber::session::AdmitOutcome, AbandonRequest) {
    let admitted = store
        .admit_run(AdmitRequest {
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
async fn trigger(pool: &PgPool, run: &Run, table: &str, condition: &str, crash: bool) -> String {
    let name = format!("abandon_{}", run.id.0.replace('-', ""));
    let body = if crash {
        "PERFORM pg_advisory_xact_lock(9123401);"
    } else {
        "RAISE EXCEPTION 'injected abandon boundary';"
    };
    let sql = format!(
        "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF {condition} THEN {body} END IF; RETURN NEW; END $$"
    );
    sqlx::query(&sql).execute(pool).await.unwrap();
    sqlx::query(&format!("CREATE TRIGGER {name} BEFORE INSERT OR UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION {name}()" )).execute(pool).await.unwrap();
    name
}
async fn drop_trigger(pool: &PgPool, table: &str, name: &str) {
    sqlx::query(&format!("DROP TRIGGER {name} ON {table}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(&format!("DROP FUNCTION {name}()"))
        .execute(pool)
        .await
        .unwrap();
}
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn each_atomic_write_failure_rolls_back_private_commit_and_retries() {
    let _guard = TEST_LOCK.lock().await;
    let Some(config) = url() else { return };
    PostgresStore::migrate(&config).await.unwrap();
    let Some(pool) = pool().await else { return };
    let store = Arc::new(PostgresStore::connect(&url().unwrap()).await.unwrap());
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
        let (admitted, request) = fixture(&store).await;
        let run = &admitted.run;
        let (table, condition) = match boundary {
            "fence" => (
                "runs",
                format!(
                    "NEW.id='{}' AND NEW.data->>'claim_token' <> OLD.data->>'claim_token'",
                    run.id
                ),
            ),
            "tool" => (
                "tool_calls",
                format!("NEW.run_id='{}' AND NEW.status='interrupted'", run.id),
            ),
            "message" => (
                "messages",
                format!("NEW.run_id='{}' AND NEW.data->>'role'='tool'", run.id),
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
                    "NEW.run_id='{}' AND NEW.data->>'kind'='tool_call_settled'",
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
                    "NEW.run_id='{}' AND NEW.data->>'kind'='run_settled'",
                    run.id
                ),
            ),
            _ => ("abandonment_commits", format!("NEW.run_id='{}'", run.id)),
        };
        let original = snapshot(&pool, run).await;
        let name = trigger(&pool, run, table, &condition, false).await;
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
        assert_eq!(snapshot(&pool, run).await, original, "{boundary}");
        drop_trigger(&pool, table, &name).await;
        let outcome = agent(store.clone(), count.clone())
            .abandon(request.clone())
            .await
            .unwrap();
        assert_eq!(outcome.run.status, RunStatus::Interrupted);
        assert_eq!(outcome.interrupted_tools.len(), 2);
        let committed = snapshot(&pool, run).await;
        assert_eq!(
            agent(store.clone(), count.clone())
                .abandon(request)
                .await
                .unwrap(),
            outcome
        );
        assert_eq!(snapshot(&pool, run).await, committed);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        println!("atomic boundary {boundary}: unchanged rollback, complete exact retry");
    }
    pool.close().await;
}

#[tokio::test]
async fn abandonment_process_helper() {
    let Ok(encoded) = std::env::var("CRABBER_FACADE_CHILD_REQUEST") else {
        return;
    };
    let request: AbandonRequest = serde_json::from_str(&encoded).unwrap();
    let pool = pool().await.unwrap();
    let store = Arc::new(PostgresStore::connect(&url().unwrap()).await.unwrap());
    if let Ok(expected) = std::env::var("CRABBER_FACADE_CHILD_SNAPSHOT") {
        let run = store
            .get_run(&request.expected.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            snapshot(&pool, &run).await,
            serde_json::from_str::<serde_json::Value>(&expected).unwrap()
        );
    }
    let counters = Arc::new(AtomicUsize::new(0));
    let host = agent(store, counters.clone());
    let outcome = host.abandon(request.clone()).await.unwrap();
    assert_eq!(host.abandon(request).await.unwrap(), outcome);
    assert_eq!(counters.load(Ordering::SeqCst), 0);
    assert_eq!(outcome.interrupted_tools.len(), 2);
    println!(
        "fresh abandonment process: run={} event={:?} status=Interrupted effects=0",
        outcome.run.id, outcome.terminal_event.cursor
    );
}
fn child(request: &AbandonRequest) -> Command {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([
        "--exact",
        "postgres::abandonment_process_helper",
        "--nocapture",
    ])
    .env(
        "CRABBER_FACADE_CHILD_REQUEST",
        serde_json::to_string(request).unwrap(),
    );
    cmd
}
#[tokio::test]
async fn physical_abandonment_writer_crash_then_fresh_process_retry() {
    let _guard = TEST_LOCK.lock().await;
    let Some(config) = url() else { return };
    PostgresStore::migrate(&config).await.unwrap();
    let Some(pool) = pool().await else { return };
    let store = PostgresStore::connect(&url().unwrap()).await.unwrap();
    let (admitted, request) = fixture(&store).await;
    let original = snapshot(&pool, &admitted.run).await;
    // Block at private evidence insertion, after rotation and all settlement writes.
    let mut lock = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(9123401)")
        .execute(&mut *lock)
        .await
        .unwrap();
    let name = trigger(
        &pool,
        &admitted.run,
        "abandonment_commits",
        &format!("NEW.run_id='{}'", admitted.run.id),
        true,
    )
    .await;
    let mut writer = child(&request).stdout(Stdio::null()).spawn().unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(10),async {
        loop {
            let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE 'INSERT INTO abandonment_commits%'").fetch_one(&pool).await.unwrap();
            if waiting > 0 {break;} tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await;
    // Always stop and reap our writer, including timeout paths.
    if writer.try_wait().unwrap().is_none() {
        writer.kill().unwrap();
    }
    let writer_exit = writer.wait().unwrap();
    assert!(!writer_exit.success());
    sqlx::query("SELECT pg_advisory_unlock(9123401)")
        .execute(&mut *lock)
        .await
        .unwrap();
    drop(lock);
    // Wait for PostgreSQL to observe the disconnected client and release row locks.
    tokio::time::timeout(Duration::from_secs(10),async {
        loop {
            let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE 'INSERT INTO abandonment_commits%'").fetch_one(&pool).await.unwrap();
            if waiting == 0 {break;} tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    drop_trigger(&pool, "abandonment_commits", &name).await;
    assert!(
        observed.is_ok(),
        "writer never reached the transaction boundary"
    );
    assert_eq!(snapshot(&pool, &admitted.run).await, original);
    let output = child(&request)
        .env("CRABBER_FACADE_CHILD_SNAPSHOT", original.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    print!("{}", String::from_utf8_lossy(&output.stdout));
    pool.close().await;
}

#[tokio::test]
async fn postgres_stopped_process_and_error_semantics() {
    let _guard = TEST_LOCK.lock().await;
    let Some(config) = url() else { return };
    PostgresStore::migrate(&config).await.unwrap();
    let Some(pool) = pool().await else { return };
    let store = Arc::new(PostgresStore::connect(&url().unwrap()).await.unwrap());
    stopped_owner(store.clone()).await;
    error_semantics(store).await;
    pool.close().await;
}
#[tokio::test]
async fn facade_committed_response_loss_fresh_host_process_exact_replay() {
    let _guard = TEST_LOCK.lock().await;
    let Some(config) = url() else { return };
    PostgresStore::migrate(&config).await.unwrap();
    let Some(pool) = pool().await else { return };
    let store = Arc::new(PostgresStore::connect(&url().unwrap()).await.unwrap());
    let (request, outcome) = response_loss::verify_loss(store).await;
    let original = snapshot(&pool, &outcome.run).await;
    let output = child(&request)
        .env("CRABBER_FACADE_CHILD_SNAPSHOT", original.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    print!("{}", String::from_utf8_lossy(&output.stdout));
    pool.close().await;
}
