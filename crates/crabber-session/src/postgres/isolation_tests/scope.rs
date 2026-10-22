use super::*;

#[tokio::test]
#[allow(clippy::too_many_lines)] // One seeded denial matrix and its positive controls.
async fn raw_sql_cross_workspace_denial_per_table() {
    let _guard = tests::TEST_LOCK.lock().await;
    let Some((url, admin)) = admin().await else {
        return;
    };
    // Native journeys populate history, tools, queue publications, native
    // enrollment, owner publication evidence and deletion tombstones.
    crate::public_state_contract::run_contract(|clock| admin.clone().with_clock(clock)).await;
    crate::typed_admission_contract::run_contract(|clock| admin.clone().with_clock(clock)).await;
    crate::session_owner_contract::run_contract(|clock| admin.clone().with_clock(clock)).await;
    crate::deletion::coordinator_contract::run(&admin, async |request| {
        admin.delete_session(request).await
    })
    .await;
    let fixture = crate::fork_pending_contract::fixture(&admin).await;
    admin.fork_session(fixture.request).await.unwrap();
    let queue = crate::queue_contract::fixture(&admin).await;
    admin.mutate_queue(queue.enqueue.clone()).await.unwrap();
    crate::queue_contract::claim(&admin, &queue).await;
    foreign_seed::seed(&admin).await;
    native_seed::seed(&admin).await;
    let b = admit(&admin, "ws-b").await;
    let a_workspace = uuid::Uuid::new_v4().to_string();
    let worker = ProvisionedRole::worker(&url, &admin, &[&a_workspace]).await;
    for table in tables(&admin.pool).await {
        if [
            "schema_version",
            "snapshot_auth",
            "crabber_workspace_grants",
            "session_tombstones",
        ]
        .contains(&table.as_str())
        {
            continue;
        }
        let foreign_count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&admin.pool)
            .await
            .unwrap();
        assert!(foreign_count > 0, "missing foreign fixture: {table}");
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&worker.pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "foreign rows visible: {table}");
        let deleted = sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&worker.pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(deleted, 0, "foreign rows deleted: {table}");
        let column: String = sqlx::query_scalar("SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 AND is_generated='NEVER' AND is_identity='NO' ORDER BY ordinal_position LIMIT 1")
            .bind(&table).fetch_one(&admin.pool).await.unwrap();
        let updated = sqlx::query(&format!("UPDATE {table} SET {column}={column}"))
            .execute(&worker.pool)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(updated, 0, "foreign rows updated: {table}");
    }
    let tombstones: i64 = sqlx::query_scalar("SELECT count(*) FROM session_tombstones")
        .fetch_one(&worker.pool)
        .await
        .unwrap();
    assert!(tombstones > 0, "documented global tombstone read exception");
    denied(&worker.pool, &tombstone_insert(&b.session.id.0)).await;
    denied(
        &worker.pool,
        "UPDATE session_tombstones SET operation_id=operation_id",
    )
    .await;
    denied(&worker.pool, "DELETE FROM session_tombstones").await;
    // Positive controls: exactly the same role now creates its own complete
    // message/event/tool path; identity ownership is the inserting LOGIN role.
    let own = PostgresStore::connect(&worker.url).await.unwrap();
    let a = admit(&own, &a_workspace).await;
    let execution = own.execution(a.fence.clone()).await.unwrap();
    let call = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: a.run.id.clone(),
        name: "isolation".into(),
        arguments: serde_json::json!({}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    execution
        .create_tool_call(
            call.clone(),
            EventRecord {
                cursor: None,
                session_id: a.session.id.clone(),
                run_id: a.run.id.clone(),
                turn_id: None,
                kind: EventKind::ToolCallPending,
                payload: serde_json::Value::Null,
                correlation: None,
                live_only: false,
                created_at: a.run.created_at,
            },
        )
        .await
        .unwrap();
    execution
        .append_event(EventRecord {
            cursor: None,
            session_id: a.session.id.clone(),
            run_id: a.run.id.clone(),
            turn_id: None,
            kind: EventKind::RunStarted,
            payload: serde_json::Value::Null,
            correlation: None,
            live_only: false,
            created_at: a.run.created_at,
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO future_queue(session_id,queue_id,sequence,state,prepared,summary_json,data_json,admission_key) VALUES($1,$2,1,'Queued',false,'{}','{}',$2)")
        .bind(&a.session.id.0).bind(uuid::Uuid::new_v4().to_string()).execute(&worker.pool).await.unwrap();
    for table in [
        "sessions",
        "runs",
        "messages",
        "events",
        "tool_calls",
        "tool_call_identities",
        "future_queue",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&worker.pool)
            .await
            .unwrap();
        assert!(count > 0, "positive control: {table}");
    }
    let identity: String =
        sqlx::query_scalar("SELECT owner_role FROM tool_call_identities WHERE id=$1")
            .bind(&call.id.0)
            .fetch_one(&worker.pool)
            .await
            .unwrap();
    assert_eq!(identity, worker.name);
    own.pool.close().await;
    worker.cleanup().await;
}

#[tokio::test]
async fn with_check_blocks_key_reassignment() {
    let _guard = tests::TEST_LOCK.lock().await;
    let Some((url, admin)) = admin().await else {
        return;
    };
    let worker = ProvisionedRole::worker(&url, &admin, &["ws-a"]).await;
    let own = PostgresStore::connect(&worker.url).await.unwrap();
    let a = admit(&own, "ws-a").await;
    let b = admit(&admin, "ws-b").await;
    denied(
        &worker.pool,
        &format!(
            "UPDATE sessions SET data=jsonb_set(data,'{{workspace_id}}','\"ws-b\"') WHERE id='{}'",
            a.session.id.0
        ),
    )
    .await;
    // Existing native identity trigger may reject immutable reassignment before
    // WITH CHECK. A legacy-protocol row lets this test reach RLS itself.
    sqlx::query("INSERT INTO sessions(id,data) VALUES($1,'{\"workspace_id\":\"ws-a\"}');")
        .bind(format!("legacy-{}", a.session.id.0))
        .execute(&admin.pool)
        .await
        .unwrap();
    let run = format!("legacy-{}", a.run.id.0);
    sqlx::query("INSERT INTO runs(id,session_id,status,claim_token,lease_until,data) VALUES($1,$2,'completed','x',0,'{}')")
        .bind(&run).bind(format!("legacy-{}",a.session.id.0)).execute(&admin.pool).await.unwrap();
    let error = sqlx::query("UPDATE runs SET session_id=$1 WHERE id=$2")
        .bind(&b.session.id.0)
        .bind(&run)
        .execute(&worker.pool)
        .await
        .unwrap_err();
    assert!([Some("42501"), Some("23503"), Some("P0001")].contains(&code(&error).as_deref()));
    let unchanged: String = sqlx::query_scalar("SELECT session_id FROM runs WHERE id=$1")
        .bind(&run)
        .fetch_one(&admin.pool)
        .await
        .unwrap();
    assert_eq!(unchanged, format!("legacy-{}", a.session.id.0));
    denied(
        &worker.pool,
        &format!(
            "INSERT INTO events(session_id,run_id,data) VALUES('{}','{}','{{}}')",
            b.session.id.0, a.run.id.0
        ),
    )
    .await;
    denied(
        &worker.pool,
        &format!(
            "INSERT INTO sessions(id,data) VALUES('{}','{{\"workspace_id\":\"ws-b\"}}')",
            SessionId::new().0
        ),
    )
    .await;
    let mut foreign_request = tests::request(&b.session.id);
    foreign_request.workspace_id = "ws-a".into();
    assert_eq!(
        own.admit_run(foreign_request).await.unwrap_err(),
        StoreError::SessionIdentityMismatch
    );
    own.pool.close().await;
    worker.cleanup().await;
}

#[tokio::test]
async fn pre_birth_rows_are_scoped_and_squat_fails_closed() {
    let _guard = tests::TEST_LOCK.lock().await;
    let Some((url, admin)) = admin().await else {
        return;
    };
    let a = ProvisionedRole::worker(&url, &admin, &["ws-a"]).await;
    let b = ProvisionedRole::worker(&url, &admin, &["ws-b"]).await;
    let own_b = PostgresStore::connect(&b.url).await.unwrap();
    let session = SessionId::new();
    admin
        .reserve_session_owner(reservation(session.clone(), "ws-b"), None)
        .await
        .unwrap();
    deny_raw_birth_of_foreign_reservation(&admin, &a, &session).await;
    for table in [
        "session_owner_contexts",
        "session_owner_current",
        "session_owner_audits",
        "session_owner_publication_cohorts",
        "session_owner_publications",
        "session_owner_scope_origins",
    ] {
        let sql = format!("SELECT count(*) FROM {table} WHERE session_id=$1");
        let hidden: i64 = sqlx::query_scalar(&sql)
            .bind(&session.0)
            .fetch_one(&a.pool)
            .await
            .unwrap();
        let visible: i64 = sqlx::query_scalar(&sql)
            .bind(&session.0)
            .fetch_one(&b.pool)
            .await
            .unwrap();
        assert_eq!(hidden, 0, "{table}");
        assert!(visible > 0, "{table}");
    }
    let squatted = SessionId::new();
    sqlx::query("INSERT INTO session_owner_contexts(session_id,data_json) VALUES($1,'{\"workspace\":\"ws-a\"}')")
        .bind(&squatted.0).execute(&a.pool).await.unwrap();
    assert_eq!(
        own_b
            .reserve_session_owner(reservation(squatted.clone(), "ws-b"), None)
            .await
            .unwrap_err(),
        crate::session_owner::SessionOwnerError::SemanticConflict
    );
    // Host births the session in B; A loses pre-birth context visibility.
    sqlx::query("INSERT INTO sessions(id,data) VALUES($1,'{\"workspace_id\":\"ws-b\"}')")
        .bind(&squatted.0)
        .execute(&admin.pool)
        .await
        .unwrap();
    let visible: i64 =
        sqlx::query_scalar("SELECT count(*) FROM session_owner_contexts WHERE session_id=$1")
            .bind(&squatted.0)
            .fetch_one(&a.pool)
            .await
            .unwrap();
    assert_eq!(visible, 0);
    denied(&a.pool,&format!("INSERT INTO session_owner_current(session_id,owner_id,data_json) VALUES('{}','squat','{{}}')",squatted.0)).await;
    denied(&a.pool, &tombstone_insert(&squatted.0)).await;
    let other = SessionId::new();
    sqlx::query("INSERT INTO session_owner_contexts(session_id,data_json) VALUES($1,'{\"workspace\":\"ws-a\"}')")
        .bind(&other.0).execute(&a.pool).await.unwrap();
    // Fail-closed birth is covered at the store boundary; the facade's keyed
    // prompt denial is independently exercised by workspace_roles.
    let mut request = tests::request(&other);
    request.workspace_id = "ws-b".into();
    let result = own_b.admit_run(request).await;
    assert_eq!(result.unwrap_err(), StoreError::SessionIdentityMismatch);
    let born: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=$1)")
        .bind(&other.0)
        .fetch_one(&admin.pool)
        .await
        .unwrap();
    assert!(!born);
    own_b.pool.close().await;
    a.cleanup().await;
    b.cleanup().await;
}

async fn deny_raw_birth_of_foreign_reservation(
    admin: &PostgresStore,
    worker: &ProvisionedRole,
    session: &SessionId,
) {
    // B reserved first. Raw SQL must not let A birth that global ID in A and
    // reclassify B's pre-birth ledger as A-owned via the sessions policy arm.
    denied(
        &worker.pool,
        &format!(
            "INSERT INTO sessions(id,data) VALUES('{}','{{\"workspace_id\":\"ws-a\"}}')",
            session.0
        ),
    )
    .await;
    let born: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=$1)")
        .bind(&session.0)
        .fetch_one(&admin.pool)
        .await
        .unwrap();
    assert!(
        !born,
        "foreign pre-birth identity cannot be adopted through raw SQL"
    );
}
