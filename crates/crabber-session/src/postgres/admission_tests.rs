//! Live PostgreSQL evidence: each child opens its own pool in a fresh process.
use super::{
    tests::{TEST_LOCK, request, test_url},
    *,
};
use crabber_core::{AdmissionOptions, EventKind, InputFingerprint, ManualClock};
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

fn keyed(session: &SessionId, key: &str) -> KeyedAdmitRequest {
    let mut request = request(session);
    request.session_id = Some(session.clone());
    KeyedAdmitRequest {
        request,
        options: AdmissionOptions {
            key: AdmissionKey::new(key).unwrap(),
            fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
            behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
        },
    }
}
fn receipt(outcome: KeyedAdmitOutcome) -> AdmissionReceipt {
    match outcome {
        KeyedAdmitOutcome::Started { receipt, .. } | KeyedAdmitOutcome::Replayed(receipt) => {
            receipt
        }
    }
}
async fn finish(store: &PostgresStore, admitted: &AdmitOutcome) {
    store
        .execution(admitted.fence.clone())
        .await
        .unwrap()
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            EventRecord {
                cursor: None,
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
                turn_id: None,
                kind: EventKind::RunSettled,
                payload: serde_json::Value::Null,
                correlation: None,
                live_only: false,
                created_at: store.clock.now(),
            },
        )
        .await
        .unwrap();
}
async fn counts(store: &PostgresStore, session: &SessionId) -> (i64, i64, i64) {
    let row = sqlx::query("SELECT (SELECT count(*) FROM runs WHERE session_id=$1) AS runs, (SELECT count(*) FROM messages WHERE session_id=$1) AS messages, (SELECT count(*) FROM admission_receipts WHERE session_id=$1) AS receipts")
        .bind(&session.0).fetch_one(&store.pool).await.unwrap();
    (row.get("runs"), row.get("messages"), row.get("receipts"))
}

#[tokio::test]
async fn postgres_admission_contract() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    crate::admission_contract::run_contract(|clock| store.clone().with_clock(clock)).await;
    store.pool.close().await;
}

#[tokio::test]
async fn independent_pools_and_saturated_existing_session() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    let other = PostgresStore::connect(&url).await.unwrap();
    let session = SessionId::new();
    let (a, b) = tokio::join!(
        store.admit_keyed_run(keyed(&session, "first")),
        other.admit_keyed_run(keyed(&session, "first"))
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(receipt(a.clone()), receipt(b.clone()));
    let ((KeyedAdmitOutcome::Started { admitted, .. }, KeyedAdmitOutcome::Replayed(_))
    | (KeyedAdmitOutcome::Replayed(_), KeyedAdmitOutcome::Started { admitted, .. })) = (a, b)
    else {
        panic!("exactly one contender must start");
    };
    assert_eq!(counts(&store, &session).await, (1, 1, 1));
    finish(&store, &admitted).await;

    // Force all contenders to hold a pooled connection while waiting on the same
    // session lock. The winner must read history on its already-held connection.
    let mut held = other.admission_transaction(&session).await.unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let store = store.clone();
        let request = keyed(&session, "second");
        tasks.push(tokio::spawn(async move {
            store.admit_keyed_run(request).await.unwrap()
        }));
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
            )
            .bind(blocker)
            .fetch_one(&other.pool)
            .await
            .unwrap();
            if waiting == 10 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all pool connections must be waiting");
    let independent = SessionId::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        other.admit_keyed_run(keyed(&independent, "first")),
    )
    .await
    .unwrap()
    .unwrap();
    held.rollback().await.unwrap();
    let outputs = tokio::time::timeout(Duration::from_secs(10), async {
        let mut outputs = Vec::new();
        for task in tasks {
            outputs.push(task.await.unwrap());
        }
        outputs
    })
    .await
    .expect("admission must not need another pooled connection");
    let expected = receipt(outputs[0].clone());
    assert_eq!(
        outputs
            .iter()
            .filter(|o| matches!(o, KeyedAdmitOutcome::Started { .. }))
            .count(),
        1
    );
    for output in outputs {
        if let KeyedAdmitOutcome::Started { admitted, .. } = &output {
            assert_eq!(admitted.prior_history.len(), 1);
        }
        assert_eq!(receipt(output), expected);
    }
    assert_eq!(counts(&store, &session).await, (2, 2, 2));
    assert_eq!(
        other
            .admit_keyed_run(keyed(&session, "busy"))
            .await
            .unwrap_err(),
        StoreError::Busy
    );
    let mut changed = keyed(&session, "second");
    changed.request.config_hash = "changed".into();
    assert_eq!(
        other.admit_keyed_run(changed).await.unwrap_err(),
        StoreError::AdmissionConflict
    );
    store.pool.close().await;
    other.pool.close().await;
}

fn child(session: &SessionId, mode: &str, expected: Option<&AdmissionReceipt>) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "postgres::admission_tests::process_helper",
            "--nocapture",
        ])
        .env("CRABBER_RECEIPT_CHILD_SESSION", &session.0)
        .env("CRABBER_RECEIPT_CHILD_MODE", mode)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(expected) = expected {
        command.env(
            "CRABBER_RECEIPT_EXPECTED",
            serde_json::to_string(expected).unwrap(),
        );
    }
    command.spawn().unwrap()
}
async fn wait_child(child: &mut Child) {
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if result.is_err() {
        child.kill().unwrap();
        child.wait().unwrap();
    }
    assert!(
        result.expect("child timed out").success(),
        "fresh-process receipt assertion failed"
    );
}

#[tokio::test]
async fn process_helper() {
    let Ok(session) = std::env::var("CRABBER_RECEIPT_CHILD_SESSION") else {
        return;
    };
    let session = SessionId(session);
    let store = PostgresStore::connect(&test_url().expect("child database required"))
        .await
        .unwrap();
    let request = keyed(&session, "process");
    let mode = std::env::var("CRABBER_RECEIPT_CHILD_MODE").unwrap();
    if mode == "start" {
        let key = request.options.key.clone();
        let returned = receipt(store.admit_keyed_run(request).await.unwrap());
        assert_eq!(
            store.lookup_admission(&session, &key).await.unwrap(),
            Some(returned)
        );
    } else {
        let expected: AdmissionReceipt =
            serde_json::from_str(&std::env::var("CRABBER_RECEIPT_EXPECTED").unwrap()).unwrap();
        let before = store.get_run(&expected.run_id).await.unwrap().unwrap();
        assert_eq!(
            store
                .lookup_admission(&session, &request.options.key)
                .await
                .unwrap(),
            Some(expected.clone())
        );
        let KeyedAdmitOutcome::Replayed(actual) = store.admit_keyed_run(request).await.unwrap()
        else {
            panic!("replay granted execution");
        };
        assert_eq!(actual, expected);
        assert_eq!(
            store.get_run(&expected.run_id).await.unwrap().unwrap(),
            before
        );
        assert_eq!(counts(&store, &session).await, (1, 1, 1));
        assert!(
            store
                .lookup_admission(&session, &AdmissionKey::new("absent").unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }
    store.pool.close().await;
}

#[tokio::test]
async fn independent_processes_and_fresh_process_lifecycle() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let session = SessionId::new();
    let mut children: Vec<_> = (0..4).map(|_| child(&session, "start", None)).collect();
    for child in &mut children {
        wait_child(child).await;
    }
    let store = PostgresStore::connect(&url).await.unwrap();
    let expected = store
        .lookup_admission(&session, &AdmissionKey::new("process").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counts(&store, &session).await, (1, 1, 1));
    let original = store.get_run(&expected.run_id).await.unwrap().unwrap();
    store.pool.close().await;
    wait_child(&mut child(&session, "replay", Some(&expected))).await;
    // Persist a truly expired lease so the fresh child uses its ordinary system clock.
    let store = PostgresStore::connect(&url).await.unwrap();
    let mut tx = store.pool.begin().await.unwrap();
    let mut expired = original.clone();
    expired.lease_until = OffsetDateTime::now_utc() - time::Duration::seconds(1);
    save_run(&mut tx, &expired).await.unwrap();
    tx.commit().await.unwrap();
    store.pool.close().await;
    wait_child(&mut child(&session, "replay", Some(&expected))).await;
    let store = PostgresStore::connect(&url).await.unwrap();
    let fence = store
        .claim_expired_run(&expected.run_id, "reclaimer")
        .await
        .unwrap();
    assert_ne!(fence.claim_token, original.claim_token);
    assert!(matches!(
        store
            .execution(RunFence {
                run_id: original.id,
                claim_token: original.claim_token
            })
            .await,
        Err(StoreError::Conflict)
    ));
    store.pool.close().await;
    wait_child(&mut child(&session, "replay", Some(&expected))).await;
    let store = PostgresStore::connect(&url).await.unwrap();
    store
        .execution(fence)
        .await
        .unwrap()
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            EventRecord {
                cursor: None,
                session_id: session.clone(),
                run_id: expected.run_id.clone(),
                turn_id: None,
                kind: EventKind::RunSettled,
                payload: serde_json::Value::Null,
                correlation: None,
                live_only: false,
                created_at: OffsetDateTime::now_utc(),
            },
        )
        .await
        .unwrap();
    store.pool.close().await;
    wait_child(&mut child(&session, "replay", Some(&expected))).await;
}

#[tokio::test]
async fn forward_migration_preserves_v1_and_connect_is_read_only() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    legacy_migration(&url, 1).await;
    legacy_migration(&url, 2).await;
}

#[allow(clippy::too_many_lines)] // One isolated-schema legacy migration lifecycle.
async fn legacy_migration(url: &str, legacy_version: i32) {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .unwrap();
    let schema = format!("receipt_migration_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if url.contains('?') { '&' } else { '?' };
    let isolated_url = format!("{url}{separator}options=-csearch_path%3D{schema}");
    assert!(PostgresStore::connect(&isolated_url).await.is_err());
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    let tables: i64 =
        sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema=$1")
            .bind(&schema)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(tables, 0, "connect must not create a schema");
    for statement in include_str!("../../migrations/0001_initial.sql")
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }
    assert!(
        PostgresStore::connect(&isolated_url).await.is_err(),
        "v1 requires explicit migration"
    );
    let clock = Arc::new(ManualClock::new(OffsetDateTime::now_utc()));
    // Populate precisely the v1 schema using domain fixtures; current writers
    // intentionally require v3 accounting columns.
    let memory = crate::MemoryStore::with_clock(clock);
    let session = SessionId::new();
    let admitted = memory.admit_run(request(&session)).await.unwrap();
    let mut messages = memory.list_all_messages(&session).await.unwrap();
    let result = ToolResult {
        status: ToolResultStatus::Completed,
        content: vec![crabber_core::ContentBlock::Text {
            text: "legacy result".into(),
        }],
    };
    let mut result_message = tests::input(&session, "");
    result_message.run_id = Some(admitted.run.id.clone());
    result_message.role = crabber_core::Role::Tool;
    result_message.parts[0].content = crabber_core::ContentBlock::ToolResult {
        call_id: ToolCallId::from("z-first"),
        content: result.content.clone(),
        is_error: false,
    };
    messages.push(result_message);

    sqlx::query("INSERT INTO sessions(id,data) VALUES($1,$2)")
        .bind(&session.0)
        .bind(json(&admitted.session).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO runs(id,session_id,status,claim_token,lease_until,data) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(&admitted.run.id.0).bind(&session.0).bind(status(admitted.run.status)).bind(&admitted.run.claim_token).bind(micros(admitted.run.lease_until)).bind(json(&admitted.run).unwrap()).execute(&pool).await.unwrap();
    let epoch = ContextEpoch {
        id: admitted.epoch.clone(),
        session_id: session.clone(),
        run_id: admitted.run.id.clone(),
        parent: None,
        summarized_range: None,
        summary_message_id: None,
        tail_start_message_id: None,
        provider_id: String::new(),
        model_id: String::new(),
        reason: "initial".into(),
        next_policy: None,
    };
    sqlx::query("INSERT INTO epochs(id,session_id,run_id,data) VALUES($1,$2,$3,$4)")
        .bind(&epoch.id.0)
        .bind(&session.0)
        .bind(&admitted.run.id.0)
        .bind(json(&epoch).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    for message in &messages {
        sqlx::query("INSERT INTO messages(id,session_id,run_id,data) VALUES($1,$2,$3,$4)")
            .bind(&message.id.0)
            .bind(&session.0)
            .bind(&admitted.run.id.0)
            .bind(json(message).unwrap())
            .execute(&pool)
            .await
            .unwrap();
        for part in &message.parts {
            sqlx::query("INSERT INTO parts(id,message_id,ordinal,data) VALUES($1,$2,$3,$4)")
                .bind(&part.id.0)
                .bind(&message.id.0)
                .bind(i64::from(part.ordinal))
                .bind(json(part).unwrap())
                .execute(&pool)
                .await
                .unwrap();
        }
    }
    if legacy_version == 2 {
        sqlx::raw_sql(include_str!("../../migrations/0002_admission_receipts.sql"))
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            PostgresStore::connect(&isolated_url).await.is_err(),
            "v2 requires explicit migration"
        );
    }
    let mut legacy_calls = Vec::new();
    for id in ["z-first", "a-second", "0-untracked"] {
        let call = ToolCallRecord {
            id: id.into(),
            run_id: admitted.run.id.clone(),
            name: "legacy".into(),
            arguments: serde_json::Value::Null,
            status: ToolCallStatus::Completed,
            retry_safe: false,
            result: Some(result.clone()),
        };
        sqlx::query("INSERT INTO tool_calls(id,run_id,status,data) VALUES($1,$2,$3,$4)")
            .bind(id)
            .bind(&admitted.run.id.0)
            .bind(call_status(call.status))
            .bind(json(&call).unwrap())
            .execute(&pool)
            .await
            .unwrap();
        if id != "0-untracked" {
            let event = EventRecord {
                cursor: None,
                session_id: session.clone(),
                run_id: admitted.run.id.clone(),
                turn_id: None,
                kind: EventKind::ToolCallPending,
                payload: serde_json::json!({"call_id":id}),
                correlation: None,
                live_only: false,
                created_at: admitted.run.created_at,
            };
            sqlx::query("INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3)")
                .bind(&session.0)
                .bind(&admitted.run.id.0)
                .bind(json(&event).unwrap())
                .execute(&pool)
                .await
                .unwrap();
        }
        legacy_calls.push(call);
    }
    let legacy_inbox = tests::input(&session, "legacy pending message");
    sqlx::query("INSERT INTO inbox(session_id,kind,data) VALUES($1,$2,$3)")
        .bind(&session.0)
        .bind(inbox_kind(InboxKind::FollowUp))
        .bind(json(&legacy_inbox).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    PostgresStore::migrate(&isolated_url).await.unwrap();
    PostgresStore::migrate(&isolated_url).await.unwrap();
    let migrated = PostgresStore::connect(&isolated_url).await.unwrap();
    let versions: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM schema_version ORDER BY version")
            .fetch_all(&migrated.pool)
            .await
            .unwrap();
    assert_eq!(versions, vec![1, 2, 3]);
    assert_eq!(
        migrated.get_session(&session).await.unwrap(),
        Some(admitted.session.clone())
    );
    assert_eq!(
        migrated.get_run(&admitted.run.id).await.unwrap(),
        Some(admitted.run.clone())
    );
    assert_eq!(
        migrated.list_all_messages(&session).await.unwrap(),
        messages
    );
    assert!(
        migrated
            .lookup_admission(&session, &AdmissionKey::new("legacy").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    let query = crate::SnapshotRequest {
        session_id: session.clone(),
        continuation: None,
        limits: crate::SnapshotLimits {
            messages: 100,
            tool_calls: 100,
            parts: 100,
            text_bytes: 10_000,
            encoded_bytes: 100_000,
        },
    };
    let crate::SnapshotOutcome::Page(snapshot) = migrated.snapshot(query).await.unwrap() else {
        panic!("snapshot")
    };
    assert_eq!(snapshot.messages, messages);
    assert_eq!(snapshot.tool_calls, legacy_calls);
    assert!(snapshot.continuation.is_none());
    assert_eq!(
        migrated
            .execution(admitted.fence.clone())
            .await
            .unwrap()
            .claim_inbox(InboxKind::FollowUp)
            .await
            .unwrap(),
        vec![legacy_inbox]
    );
    finish(&migrated, &admitted).await;
    migrated
        .admit_keyed_run(keyed(&session, "new"))
        .await
        .unwrap();
    assert_eq!(counts(&migrated, &session).await, (2, 3, 1));
    // Setting the whole connection read-only proves connect and lookup use no DDL/DML.
    let read_only_url = format!(
        "{url}{separator}options=-csearch_path%3D{schema}%20-cdefault_transaction_read_only%3Don"
    );
    let read_only = PostgresStore::connect(&read_only_url).await.unwrap();
    assert!(
        read_only
            .lookup_admission(&session, &AdmissionKey::new("new").unwrap())
            .await
            .unwrap()
            .is_some()
    );
    read_only.pool.close().await;
    migrated.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn process_crash_before_commit_rolls_back_every_record() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    let session = SessionId::new();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let function = format!("receipt_barrier_{suffix}");
    // Test-only server trigger blocks after run/message writes and before receipt commit.
    // UUID session IDs and generated SQL identifiers contain no caller-controlled text.
    sqlx::query(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.session_id = '{}' THEN PERFORM pg_advisory_xact_lock(751302920); END IF; RETURN NEW; END $$", session.0)).execute(&store.pool).await.unwrap();
    sqlx::query(&format!("CREATE TRIGGER {function} BEFORE INSERT ON admission_receipts FOR EACH ROW EXECUTE FUNCTION {function}()" )).execute(&store.pool).await.unwrap();
    let mut held = store.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(751302920)")
        .execute(&mut *held)
        .await
        .unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let mut process = child(&session, "start", None);
    let waiter = tokio::time::timeout(Duration::from_secs(10),async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)) AND query LIKE 'INSERT INTO admission_receipts%'")
                .bind(blocker).fetch_optional(&store.pool).await.unwrap();
            if let Some(pid) = pid { break pid; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("child must reach precommit barrier");
    assert_eq!(counts(&store, &session).await, (0, 0, 0));
    assert!(
        store
            .lookup_admission(&session, &AdmissionKey::new("process").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    process.kill().unwrap();
    process.wait().unwrap();
    // Terminate the blocked backend too: PostgreSQL may not notice a disconnected
    // client until its blocked statement returns. This guarantees server rollback.
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(waiter)
        .execute(&store.pool)
        .await
        .unwrap();
    held.rollback().await.unwrap();
    sqlx::query(&format!("DROP TRIGGER {function} ON admission_receipts"))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query(&format!("DROP FUNCTION {function}()"))
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(store.get_session(&session).await.unwrap().is_none());
    assert_eq!(counts(&store, &session).await, (0, 0, 0));
    let epochs: i64 = sqlx::query_scalar("SELECT count(*) FROM epochs WHERE session_id=$1")
        .bind(&session.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(epochs, 0);
    store.pool.close().await;
    wait_child(&mut child(&session, "start", None)).await;
    let reopened = PostgresStore::connect(&url).await.unwrap();
    assert_eq!(counts(&reopened, &session).await, (1, 1, 1));
    reopened.pool.close().await;
}
