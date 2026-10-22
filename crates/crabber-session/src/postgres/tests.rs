//! Native backend contracts; logical test names remain `postgres::tests`.
use super::*;
use crabber_core::{ContentBlock, ManualClock, PartId, Role};
use std::time::Duration;
pub(super) static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) fn test_url() -> Option<String> {
    match std::env::var("CRABBER_TEST_POSTGRES_URL") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_))
            if std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref() == Ok("1") =>
        {
            panic!("CRABBER_TEST_POSTGRES_URL is required")
        }
        Err(_error) => {
            eprintln!("skipping PostgreSQL tests: CRABBER_TEST_POSTGRES_URL is unset");
            None
        }
    }
}
pub(super) fn input(session_id: &SessionId, text: &str) -> Message {
    let id = MessageId::new();
    Message {
        id: id.clone(),
        session_id: session_id.clone(),
        run_id: None,
        role: Role::User,
        parent_id: None,
        parts: vec![Part {
            id: PartId::new(),
            message_id: id,
            ordinal: 0,
            kind: PartKind::UserInputText,
            content: ContentBlock::Text { text: text.into() },
        }],
        created_at: OffsetDateTime::now_utc(),
    }
}
pub(super) fn request(session: &SessionId) -> AdmitRequest {
    AdmitRequest {
        input_envelope: None,
        additional_messages: Vec::new(),
        expected_revision: None,
        session_id: None,
        workspace_id: "test".into(),
        directory: "/tmp".into(),
        title: "test".into(),
        user_message: input(session, "hello"),
        config_hash: "config".into(),
        plan_fingerprint: "plan".into(),
        owner: "test".into(),
        lease: Duration::from_secs(30),
    }
}
#[tokio::test]
async fn postgres_public_state_contract() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    crate::public_state_contract::run_contract(|clock| store.with_clock(clock)).await;
}
#[tokio::test]
async fn postgres_typed_admission_contract() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    crate::typed_admission_contract::run_contract(|clock| store.with_clock(clock)).await;
}
#[tokio::test]
async fn postgres_revision_contract() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    crate::revision_contract::run_contract(|clock| store.with_clock(clock)).await;
}

#[tokio::test]
async fn postgres_contract() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    sqlx::query("TRUNCATE sessions CASCADE")
        .execute(&store.pool)
        .await
        .unwrap();
    crate::storetest::run_contract(|clock: Arc<ManualClock>| store.clone().with_clock(clock)).await;
}
#[tokio::test]
async fn postgres_storable_record_contract() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    sqlx::query("TRUNCATE sessions CASCADE")
        .execute(&store.pool)
        .await
        .unwrap();
    crate::storetest::run_storable_record_contract(|clock: Arc<ManualClock>| {
        store.clone().with_clock(clock)
    })
    .await;
}
#[tokio::test]
async fn postgres_workspace_discovery_contract() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    sqlx::query("TRUNCATE sessions CASCADE")
        .execute(&store.pool)
        .await
        .unwrap();
    crate::storetest::run_workspace_discovery_contract(|clock: Arc<ManualClock>| {
        store.clone().with_clock(clock)
    })
    .await;
}
#[tokio::test]
async fn postgres_workspace_identity_contract() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    sqlx::query("TRUNCATE sessions CASCADE")
        .execute(&store.pool)
        .await
        .unwrap();
    crate::storetest::run_workspace_identity_contract(|clock: Arc<ManualClock>| {
        store.clone().with_clock(clock)
    })
    .await;

    // A second pool reads the identity the first one persisted, and a
    // stored row that no longer decodes fails closed with a typed error.
    let session = SessionId::new();
    let admitted = store.admit_run(request(&session)).await.unwrap();
    let fresh = PostgresStore::connect(&url).await.unwrap();
    let stored = fresh.get_session(&session).await.unwrap().unwrap();
    assert_eq!(
        (stored.workspace_id.as_str(), stored.directory.as_str()),
        ("test", "/tmp")
    );
    assert_eq!(stored, admitted.session);
    sqlx::query("UPDATE sessions SET data = data - 'workspace_id' WHERE id=$1")
        .bind(&session.0)
        .execute(&store.pool)
        .await
        .unwrap();
    let invalid = StoreError::Validation("stored record is invalid".into());
    assert_eq!(fresh.get_session(&session).await.unwrap_err(), invalid);
    let mut again = request(&session);
    again.session_id = Some(session.clone());
    assert_eq!(fresh.admit_run(again).await.unwrap_err(), invalid);
    sqlx::query("TRUNCATE sessions CASCADE")
        .execute(&store.pool)
        .await
        .unwrap();
}
#[tokio::test]
async fn migrate_reopen_and_concurrent_sessions() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    let before: Vec<i32> =
        sqlx::query_scalar("SELECT version FROM schema_version ORDER BY version")
            .fetch_all(&store.pool)
            .await
            .unwrap();
    PostgresStore::migrate(&url).await.unwrap();
    let after: Vec<i32> = sqlx::query_scalar("SELECT version FROM schema_version ORDER BY version")
        .fetch_all(&store.pool)
        .await
        .unwrap();
    assert_eq!(before, after);
    let first = SessionId::new();
    let second = SessionId::new();
    let a = store.admit_run(request(&first)).await.unwrap();
    let first_execution = store.execution(a.fence.clone()).await.unwrap();

    // Hold this run's row lock until the other session has admitted and written.
    // A global store mutex would prevent the bounded operation below from finishing.
    let mut held = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("SELECT id FROM runs WHERE id=$1 FOR UPDATE")
        .bind(&a.run.id.0)
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let first_run = a.run.id.clone();
    let blocked_write = tokio::spawn(async move {
        let mut value = input(&first, "a");
        value.run_id = Some(first_run);
        started_tx.send(()).unwrap();
        first_execution.append_message(value).await
    });
    started_rx.await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *held)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock' AND query LIKE 'SELECT CASE WHEN octet_length(data::text)<=33554432 THEN data END AS data FROM runs WHERE id=% FOR UPDATE')"
            )
            .bind(blocker_pid).fetch_one(&store.pool).await.unwrap();
            if blocked { break; }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first writer must reach the held row lock");

    let b = tokio::time::timeout(Duration::from_secs(10), async {
        let b = store.admit_run(request(&second)).await?;
        let mut value = input(&second, "b");
        value.run_id = Some(b.run.id.clone());
        store
            .execution(b.fence.clone())
            .await?
            .append_message(value)
            .await?;
        Ok::<_, StoreError>(b)
    })
    .await
    .expect("independent session must finish before first lock releases")
    .unwrap();
    assert!(
        !blocked_write.is_finished(),
        "first write must wait for its row lock"
    );
    held.commit().await.unwrap();
    blocked_write.await.unwrap().unwrap();
    let mut busy_request = request(&a.session.id);
    busy_request.session_id = Some(a.session.id.clone());
    assert_eq!(
        store.admit_run(busy_request).await.unwrap_err(),
        StoreError::Busy
    );

    assert_eq!(
        store.list_all_messages(&b.session.id).await.unwrap().len(),
        2
    );
    let reopened = PostgresStore::connect(&url).await.unwrap();
    assert_eq!(
        store.list_messages(&a.session.id, None).await.unwrap(),
        reopened.list_messages(&a.session.id, None).await.unwrap()
    );
    assert_eq!(
        reopened
            .list_all_messages(&a.session.id)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admission_transaction_span_records_scope_and_outcome() {
    let Some(url) = test_url() else {
        return;
    };
    let _lock = TEST_LOCK.lock().await;
    let (captured, _guard) = crate::tracing_capture_tests::capture();
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    store.admit_run(request(&SessionId::new())).await.unwrap();
    let records = captured.0.lock().unwrap();
    assert!(records.iter().any(|r| {
        r.name == "crabber.store.transaction"
            && r.fields.get("db.system").is_some_and(|v| v == "postgresql")
            && r.fields
                .get("crabber.store.scope")
                .is_some_and(|v| v == "admission")
            && r.fields.get("outcome").is_some_and(|v| v == "committed")
    }));
    assert!(records.iter().any(|r| {
        r.name == "crabber.store.transaction"
            && r.fields
                .get("crabber.store.scope")
                .is_some_and(|v| v == "verify")
            && r.fields.get("outcome").is_some_and(|v| v == "rolled_back")
    }));
}
