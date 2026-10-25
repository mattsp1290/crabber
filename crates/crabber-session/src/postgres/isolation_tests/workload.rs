use super::*;
use crabber_core::ManualClock;

#[tokio::test]
async fn worker_role_workload_succeeds() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let admin = admin(&url).await;
        // The reusable store contract asserts its unfinished-run roster is empty.
        // Only the owner resets the fixture; the worker never receives TRUNCATE.
        sqlx::query("TRUNCATE sessions CASCADE")
            .execute(&admin.pool)
            .await
            .unwrap();
        let role = ProvisionedRole::worker(&url, &admin, CONTRACT_WORKSPACES).await;
        let store = PostgresStore::connect(&role.url).await.unwrap();
        exercise(&store, &admin.pool).await;
        store.pool.close().await;
        role.cleanup().await;
    })
    .await
    .unwrap();
}

/// Identical assertions for the ordinary worker proof and manual role timing.
async fn exercise(store: &PostgresStore, admin_pool: &sqlx::PgPool) {
    crate::storetest::run_contract(|clock| store.clone().with_clock(clock)).await;
    crate::public_state_contract::run_contract(|clock| store.clone().with_clock(clock)).await;
    crate::typed_admission_contract::run_contract(|clock| store.clone().with_clock(clock)).await;
    crate::session_owner_contract::run_contract(|clock| store.clone().with_clock(clock)).await;
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let timed = store.clone().with_clock(clock.clone());
    crate::control_contract::ordered(&timed, &clock).await;
    crate::queue_contract::run(&timed, clock).await;
    let fixture = crate::fork_pending_contract::fixture(store).await;
    store.fork_session(fixture.request).await.unwrap();
    management_and_delete(store).await;
    // Native trigger identity lifecycle under the LOGIN role, including release
    // after parent deletion (the parent no longer exists during that trigger).
    let admitted = admit(store, "ws-a").await;
    let call_id = ToolCallId::new();
    let record = "{}";
    sqlx::query("INSERT INTO tool_calls(id,run_id,status,data,snapshot_record,snapshot_parts,snapshot_text,snapshot_bytes) VALUES($1,$2,'pending','{}',$3,0,0,2)")
        .bind(&call_id.0).bind(&admitted.run.id.0).bind(record).execute(&store.pool).await.unwrap();
    sqlx::query("UPDATE tool_calls SET status='running' WHERE id=$1")
        .bind(&call_id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM tool_calls WHERE id=$1")
        .bind(&call_id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM tool_call_identities WHERE id=$1")
        .bind(&call_id.0)
        .fetch_one(admin_pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "manual paired owner/worker timing; run with --ignored --nocapture"]
async fn worker_role_workload_performance() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let admin = admin(&url).await;
        let role = ProvisionedRole::worker(&url, &admin, CONTRACT_WORKSPACES).await;
        let worker = PostgresStore::connect(&role.url).await.unwrap();
        for sample in 1..=3 {
            sqlx::query("TRUNCATE sessions CASCADE")
                .execute(&admin.pool)
                .await
                .unwrap();
            let start = std::time::Instant::now();
            exercise(&admin, &admin.pool).await;
            let owner_seconds = start.elapsed().as_secs_f64();
            sqlx::query("TRUNCATE sessions CASCADE")
                .execute(&admin.pool)
                .await
                .unwrap();
            let start = std::time::Instant::now();
            exercise(&worker, &admin.pool).await;
            let worker_seconds = start.elapsed().as_secs_f64();
            println!(
                "PERF sample={sample} owner_seconds={owner_seconds:.6} worker_seconds={worker_seconds:.6} ratio={:.6}",
                worker_seconds / owner_seconds
            );
        }
        worker.pool.close().await;
        role.cleanup().await;
    })
    .await
    .unwrap();
}

async fn management_and_delete(store: &PostgresStore) {
    use crabber_core::{ControlId, SessionRevision};
    let session = SessionId::new();
    let mut receipt = store
        .manage_session(crate::ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: crate::SessionChange::Create {
                workspace_id: "ws-a".into(),
                directory: "/tmp".into(),
                title: "worker".into(),
                defaults: crate::SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    for change in [
        crate::SessionChange::Rename {
            title: "renamed".into(),
        },
        crate::SessionChange::SetArchived { archived: true },
    ] {
        receipt = store
            .manage_session(crate::ManageSession {
                id: ControlId::new(),
                session_id: session.clone(),
                expected_revision: receipt.revision,
                change,
            })
            .await
            .unwrap();
    }
    let snapshot = store
        .public_session_snapshot_v2(crate::PublicSessionRequestV2 {
            session_id: session.clone(),
            continuation: None,
            limits: crate::PublicSessionLimitsV2 {
                header_bytes: 65536,
                proof_bytes: 32 << 20,
                approval_interactions: 64,
                tool_requirements: 64,
                queue_entries: 64,
                registered_owners: 1024,
                run_controls: 64,
                history: crate::SnapshotLimits {
                    messages: 1,
                    tool_calls: 1,
                    parts: 256,
                    text_bytes: 4096,
                    encoded_bytes: 4096,
                },
            },
        })
        .await
        .unwrap();
    assert!(matches!(snapshot, crate::PublicSessionOutcomeV2::Page(_)));
    let deleted = store
        .delete_session(crate::DeleteSession {
            id: ControlId::new(),
            session_id: session,
            expected_revision: receipt.revision,
        })
        .await
        .unwrap();
    assert!(matches!(deleted, crate::DeletionOutcome::Applied(_)));
}
