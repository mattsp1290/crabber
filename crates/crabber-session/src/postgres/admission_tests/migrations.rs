//! Isolated-schema admission receipt and execution-evidence migration proofs.
use super::*;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn forward_migration_preserves_v1_and_connect_is_read_only() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    legacy_migration(&url, 1).await;
    legacy_migration(&url, 2).await;
    legacy_migration(&url, 3).await;
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
    for statement in include_str!("../../../migrations/0001_initial.sql")
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
    if legacy_version >= 2 {
        sqlx::raw_sql(include_str!(
            "../../../migrations/0002_admission_receipts.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            PostgresStore::connect(&isolated_url).await.is_err(),
            "v2 requires explicit migration"
        );
    }
    if legacy_version == 3 {
        // The fenced-abandon branch used v3 before snapshots were integrated.
        let migration = include_str!("../../../migrations/0004_abandonment_commits.sql")
            .replace("VALUES (4)", "VALUES (3)");
        sqlx::raw_sql(&migration).execute(&pool).await.unwrap();
        assert!(PostgresStore::connect(&isolated_url).await.is_err());
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
    assert_eq!(versions, vec![1, 2, 3, 4, 5]);
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
#[allow(clippy::too_many_lines)]
async fn v2_migration_preserves_receipts_and_never_authenticates_old_markers() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let schema = format!("abandonment_migration_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if url.contains('?') { '&' } else { '?' };
    let isolated_url = format!("{url}{separator}options=-csearch_path%3D{schema}");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    for statement in concat!(
        include_str!("../../../migrations/0001_initial.sql"),
        "\n",
        include_str!("../../../migrations/0002_admission_receipts.sql")
    )
    .split(';')
    .map(str::trim)
    .filter(|part| !part.is_empty())
    {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }
    assert!(
        PostgresStore::connect(&isolated_url).await.is_err(),
        "v2 requires explicit forward migration"
    );
    let clock = Arc::new(ManualClock::new(OffsetDateTime::now_utc()));
    let legacy = crate::MemoryStore::with_clock(clock.clone());
    let session = SessionId::new();
    let original = keyed(&session, "legacy");
    let key = original.options.key.clone();
    let KeyedAdmitOutcome::Started { receipt, admitted } =
        legacy.admit_keyed_run(original).await.unwrap()
    else {
        panic!("new receipt")
    };
    let execution = legacy.execution(admitted.fence.clone()).await.unwrap();
    let call = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: admitted.run.id.clone(),
        name: "unavailable".into(),
        arguments: serde_json::json!({}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    let mut marker = crate::abandonment_contract::event(&admitted.run, EventKind::ToolCallPending);
    execution
        .create_tool_call(call.clone(), marker.clone())
        .await
        .unwrap();
    let assertion = crate::abandonment_contract::abandon_request(
        &admitted,
        crabber_core::AbandonAuthority::HostStoppedOwner,
    );
    let mut predicted = admitted.run.clone();
    predicted.status = RunStatus::Interrupted;
    predicted.updated_at = clock.now();
    marker.kind = EventKind::RunSettled;
    marker.payload = serde_json::json!({"abandonment_v1": {"request": assertion, "run": predicted, "interrupted_tools": []}});
    execution
        .settle_run(RunStatus::Interrupted, None, Usage::default(), marker)
        .await
        .unwrap();
    let messages = legacy.list_all_messages(&session).await.unwrap();
    let events = legacy.list_events(&session, None, 100).await.unwrap();
    sqlx::query("INSERT INTO sessions(id,data) VALUES($1,$2)")
        .bind(&session.0)
        .bind(json(&admitted.session).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO runs(id,session_id,status,claim_token,lease_until,data) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(&predicted.id.0).bind(&session.0).bind(status(predicted.status)).bind(&predicted.claim_token).bind(micros(predicted.lease_until)).bind(json(&predicted).unwrap()).execute(&pool).await.unwrap();
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
            .bind(&predicted.id.0)
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
    sqlx::query("INSERT INTO admission_receipts(session_id,admission_key,run_id,user_message_id,data) VALUES($1,$2,$3,$4,$5)")
        .bind(&session.0).bind(key.as_str()).bind(&admitted.run.id.0).bind(&receipt.user_message_id.0).bind(json(&receipt).unwrap()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO tool_calls(id,run_id,status,data) VALUES($1,$2,$3,$4)")
        .bind(&call.id.0)
        .bind(&predicted.id.0)
        .bind(call_status(call.status))
        .bind(json(&call).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    for event in &events {
        sqlx::query("INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3)")
            .bind(&session.0)
            .bind(&predicted.id.0)
            .bind(json(event).unwrap())
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;
    PostgresStore::migrate(&isolated_url).await.unwrap();
    PostgresStore::migrate(&isolated_url).await.unwrap();
    let migrated = PostgresStore::connect(&isolated_url).await.unwrap();
    assert_eq!(
        migrated.get_session(&session).await.unwrap(),
        Some(admitted.session.clone())
    );
    assert_eq!(
        migrated.get_run(&admitted.run.id).await.unwrap(),
        Some(predicted)
    );
    assert_eq!(
        migrated.lookup_admission(&session, &key).await.unwrap(),
        Some(receipt)
    );
    assert_eq!(
        migrated.list_all_messages(&session).await.unwrap(),
        messages
    );
    assert_eq!(
        migrated.list_events(&session, None, 100).await.unwrap(),
        events
    );
    assert_eq!(
        migrated
            .list_unfinished_tool_calls(&admitted.run.id)
            .await
            .unwrap(),
        vec![call]
    );
    let commits: i64 = sqlx::query_scalar("SELECT count(*) FROM abandonment_commits")
        .fetch_one(&migrated.pool)
        .await
        .unwrap();
    assert_eq!(
        commits, 0,
        "arbitrary historical markers must never be backfilled"
    );
    assert_eq!(
        migrated.abandon_run(assertion).await.unwrap_err(),
        crabber_core::AbandonError::AlreadyTerminal
    );
    assert_eq!(
        migrated.list_events(&session, None, 100).await.unwrap(),
        events
    );
    let next = migrated
        .admit_keyed_run(keyed(&session, "next"))
        .await
        .unwrap();
    let KeyedAdmitOutcome::Started { admitted: next, .. } = next else {
        panic!("next receipt")
    };
    let stopped = crate::abandonment_contract::abandon_request(
        &next,
        crabber_core::AbandonAuthority::HostStoppedOwner,
    );
    let outcome = migrated.abandon_run(stopped.clone()).await.unwrap();
    assert_eq!(migrated.abandon_run(stopped).await.unwrap(), outcome);
    let commits: i64 = sqlx::query_scalar("SELECT count(*) FROM abandonment_commits")
        .fetch_one(&migrated.pool)
        .await
        .unwrap();
    assert_eq!(commits, 1);
    migrated.pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Full retained-table comparison for both schema fixtures.
async fn v3_and_v4_upgrade_retains_records_without_manufacturing_unstarted_evidence() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    for baseline in [3, 4] {
        let schema = format!("execution_migration_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if url.contains('?') { '&' } else { '?' };
        let isolated_url = format!("{url}{separator}options=-csearch_path%3D{schema}");
        PostgresStore::migrate(&isolated_url).await.unwrap();
        let store = PostgresStore::connect(&isolated_url).await.unwrap();
        let session = SessionId::new();
        let original = keyed(&session, "legacy");
        let key = original.options.key.clone();
        let KeyedAdmitOutcome::Started { receipt, admitted } =
            store.admit_keyed_run(original.clone()).await.unwrap()
        else {
            panic!()
        };
        let execution = store.execution(admitted.fence.clone()).await.unwrap();
        execution
            .put_extension_state("legacy", vec![("key".into(), Some("value".into()))])
            .await
            .unwrap();
        store
            .enqueue_inbox(&session, InboxKind::Steer, request(&session).user_message)
            .await
            .unwrap();
        store
            .abandon_run(crate::abandonment_contract::abandon_request(
                &admitted,
                crabber_core::AbandonAuthority::HostStoppedOwner,
            ))
            .await
            .unwrap();
        // Build exact v3/v4 table fixtures from the legacy None admission path.
        sqlx::query("DROP TABLE admission_executions")
            .execute(&store.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM schema_version WHERE version>$1")
            .bind(baseline)
            .execute(&store.pool)
            .await
            .unwrap();
        if baseline == 3 {
            sqlx::query("DROP TABLE abandonment_commits")
                .execute(&store.pool)
                .await
                .unwrap();
        }
        let tables = if baseline == 4 {
            vec![
                "sessions",
                "runs",
                "messages",
                "parts",
                "events",
                "epochs",
                "inbox",
                "extension_state",
                "abandonment_commits",
            ]
        } else {
            vec![
                "sessions",
                "runs",
                "messages",
                "parts",
                "events",
                "epochs",
                "inbox",
                "extension_state",
            ]
        };
        let mut before = Vec::new();
        for table in &tables {
            let value: serde_json::Value = sqlx::query_scalar(&format!(
                "SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM {table} t"
            ))
            .fetch_one(&store.pool)
            .await
            .unwrap();
            before.push(value);
        }
        assert!(PostgresStore::connect(&isolated_url).await.is_err());
        PostgresStore::migrate(&isolated_url).await.unwrap();
        PostgresStore::migrate(&isolated_url).await.unwrap();
        let reopened = PostgresStore::connect(&isolated_url).await.unwrap();
        for (table, expected) in tables.iter().zip(before) {
            let value: serde_json::Value = sqlx::query_scalar(&format!(
                "SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM {table} t"
            ))
            .fetch_one(&reopened.pool)
            .await
            .unwrap();
            assert_eq!(value, expected, "v{baseline} retained {table}");
        }
        assert_eq!(
            reopened.lookup_admission(&session, &key).await.unwrap(),
            Some(receipt)
        );
        assert!(
            reopened
                .load_admission_execution(&session, &key)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            reopened.admit_keyed_run(original).await.unwrap(),
            KeyedAdmitOutcome::Replayed(_)
        ));
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM admission_executions")
            .fetch_one(&reopened.pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
        store.pool.close().await;
        reopened.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&admin)
            .await
            .unwrap();
    }
    admin.close().await;
}
