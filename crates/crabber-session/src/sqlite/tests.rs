use super::*;
use crate::{AdmitRequest, Store};
use crabber_core::{
    ByteLimits, ContentBlock, EventKind, ManualClock, Message, MessageId, Part, PartId, PartKind,
    Role, SessionId,
};
use std::{path::PathBuf, time::Duration};
use time::OffsetDateTime;

pub(super) async fn temp_store() -> (SqliteStore, PathBuf, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    SqliteStore::migrate(&path).await.unwrap();
    (SqliteStore::connect(&path).await.unwrap(), path, directory)
}
pub(super) fn input(session: &SessionId, text: &str) -> Message {
    let id = MessageId::new();
    Message {
        id: id.clone(),
        session_id: session.clone(),
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
async fn sqlite_contract() {
    let (store, _, _directory) = temp_store().await;
    crate::storetest::run_contract(|clock: Arc<ManualClock>| store.clone().with_clock(clock)).await;
}
#[tokio::test]
async fn sqlite_storable_record_contract() {
    let (store, _, _directory) = temp_store().await;
    crate::storetest::run_storable_record_contract(|clock: Arc<ManualClock>| {
        store.clone().with_clock(clock)
    })
    .await;
}
#[tokio::test]
async fn sqlite_workspace_identity_contract() {
    let (store, path, _directory) = temp_store().await;
    crate::storetest::run_workspace_identity_contract(|clock: Arc<ManualClock>| {
        store.clone().with_clock(clock)
    })
    .await;
    let session = SessionId::new();
    let admitted = store.admit_run(request(&session)).await.unwrap();
    let fresh = SqliteStore::connect(&path).await.unwrap();
    let stored = fresh.get_session(&session).await.unwrap().unwrap();
    assert_eq!(
        (stored.workspace_id.as_str(), stored.directory.as_str()),
        ("test", "/tmp")
    );
    assert_eq!(stored, admitted.session);
    sqlx::query("UPDATE sessions SET data='{}' WHERE id=$1")
        .bind(&session.0)
        .execute(&store.writer)
        .await
        .unwrap();
    let invalid = StoreError::Validation("stored record is invalid".into());
    assert_eq!(fresh.get_session(&session).await.unwrap_err(), invalid);
    let mut again = request(&session);
    again.session_id = Some(session);
    assert_eq!(fresh.admit_run(again).await.unwrap_err(), invalid);
}
#[tokio::test]
async fn sqlite_admission_contract() {
    let (store, _, _directory) = temp_store().await;
    crate::admission_contract::run_contract(|clock| store.clone().with_clock(clock)).await;
}
#[tokio::test]
async fn sqlite_admission_execution_contract() {
    let (store, _, _directory) = temp_store().await;
    crate::admission_execution_contract::run_contract(|clock| store.clone().with_clock(clock))
        .await;
}
#[tokio::test]
async fn reader_pool_rejects_writes() {
    let (store, _, _directory) = temp_store().await;
    assert!(
        sqlx::query("INSERT INTO sessions(id,data) VALUES('reader-write','{}')")
            .execute(&store.readers)
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM sessions")
        .fetch_one(&store.writer)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
#[tokio::test]
async fn execution_does_not_take_the_write_lock() {
    let (store, path, _directory) = temp_store().await;
    let admitted = store.admit_run(request(&SessionId::new())).await.unwrap();
    let mut connection = SqliteConnection::connect_with(&options(&path))
        .await
        .unwrap();
    let held = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        let _execution = store.execution(admitted.fence.clone()).await.unwrap();
        assert_eq!(
            store.get_run(&admitted.run.id).await.unwrap(),
            Some(admitted.run.clone())
        );
        assert_eq!(
            store
                .list_events(&admitted.session.id, None, 10)
                .await
                .unwrap()
                .len(),
            0
        );
        let snapshot = store
            .snapshot(crate::SnapshotRequest {
                session_id: admitted.session.id,
                limits: crate::SnapshotLimits {
                    messages: 10,
                    tool_calls: 10,
                    parts: 100,
                    text_bytes: 10_000,
                    encoded_bytes: 100_000,
                },
                continuation: None,
            })
            .await
            .unwrap();
        assert!(matches!(snapshot, crate::SnapshotOutcome::Page(_)));
    })
    .await
    .expect("reader calls must finish while another connection holds the write lock");
    held.rollback().await.unwrap();
}
async fn assert_write_lock_free(path: &Path) {
    let mut connection =
        SqliteConnection::connect_with(&options(path).busy_timeout(Duration::ZERO))
            .await
            .unwrap();
    connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("failed call must release the write lock before returning")
        .rollback()
        .await
        .unwrap();
}
#[tokio::test]
async fn failed_write_releases_the_lock_before_returning() {
    let (store, path, _directory) = temp_store().await;
    let admitted = store.admit_run(request(&SessionId::new())).await.unwrap();
    let mut busy = request(&admitted.session.id);
    busy.session_id = Some(admitted.session.id.clone());
    assert_eq!(store.admit_run(busy).await.unwrap_err(), StoreError::Busy);
    assert_write_lock_free(&path).await;
    let mut stale = admitted.fence.clone();
    stale.claim_token = "stale".into();
    // Construct privately: public execution rejects a stale fence before writes.
    let execution = SqliteExecution {
        store: store.clone(),
        fence: stale,
    };
    let mut message = input(&admitted.session.id, "stale");
    message.run_id = Some(admitted.run.id.clone());
    assert_eq!(
        crate::ExecutionStore::append_message(&execution, message)
            .await
            .unwrap_err(),
        StoreError::Conflict
    );
    assert_write_lock_free(&path).await;
    assert_eq!(
        store
            .abandon_run(crabber_core::AbandonRequest {
                expected: admitted.fence.clone(),
                expected_owner: admitted.run.owner.clone(),
                authority: crabber_core::AbandonAuthority::ExpiredLease
            })
            .await
            .unwrap_err(),
        crabber_core::AbandonError::LiveLease
    );
    assert_write_lock_free(&path).await;
    let keyed =
        crate::admission_execution_contract::keyed(&SessionId::new(), OffsetDateTime::now_utc());
    let crate::KeyedAdmitOutcome::Started {
        admitted: unstarted,
        receipt,
    } = store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!()
    };
    let claim = crate::ClaimUnstartedAdmissionRequest {
        session_id: receipt.session_id,
        key: keyed.options.key,
        expected_fence: unstarted.fence,
        expected_owner: unstarted.run.owner,
        fingerprint: receipt.fingerprint,
        behavior_fingerprint: keyed.options.behavior_fingerprint,
        semantic_digest: receipt.semantic_digest,
        capsule_digest: keyed.execution.unwrap().digest().unwrap(),
        owner: "new owner".into(),
        lease: Duration::from_secs(30),
    };
    assert_eq!(
        store.claim_unstarted_admission(claim).await.unwrap_err(),
        crate::AdmissionExecutionError::LiveLease
    );
    assert_write_lock_free(&path).await;
    let limited = store.clone().with_limits(ByteLimits {
        max_state_entries: 0,
        ..ByteLimits::default()
    });
    let execution = limited.execution(admitted.fence).await.unwrap();
    assert_eq!(
        execution
            .put_extension_state("ext", vec![("key".into(), Some("value".into()))])
            .await
            .unwrap_err(),
        StoreError::Limit("extension state".into())
    );
    assert!(
        store
            .get_extension_state("ext", &admitted.session.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert_write_lock_free(&path).await;
}
#[tokio::test]
async fn event_cursors_follow_commit_order() {
    let (store, path, _directory) = temp_store().await;
    let other = SqliteStore::connect(&path).await.unwrap();
    let admitted = store.admit_run(request(&SessionId::new())).await.unwrap();
    let a = store.execution(admitted.fence.clone()).await.unwrap();
    let b = other.execution(admitted.fence).await.unwrap();
    let mut expected = Vec::new();
    for i in 0..6 {
        let mut event = crate::abandonment_contract::event(&admitted.run, EventKind::RunSettled);
        event.payload = serde_json::json!({"append":i});
        expected.push(event.clone());
        if i % 2 == 0 {
            a.append_event(event).await.unwrap();
        } else {
            b.append_event(event).await.unwrap();
        }
    }
    let events = store
        .list_events(&admitted.session.id, None, 20)
        .await
        .unwrap();
    assert_eq!(events.len(), expected.len());
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].cursor.unwrap() < pair[1].cursor.unwrap())
    );
    for (mut actual, wanted) in events.into_iter().zip(expected) {
        actual.cursor = None;
        assert_eq!(actual, wanted);
    }
}

#[tokio::test]
async fn failed_commit_rolls_back_before_returning() {
    let (store, path, _directory) = temp_store().await;
    let admitted = store.admit_run(request(&SessionId::new())).await.unwrap();
    // Deferred failure reaches COMMIT after the event write itself succeeds.
    sqlx::raw_sql("CREATE TABLE commit_failure(parent TEXT REFERENCES sessions(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fail_commit AFTER INSERT ON events BEGIN INSERT INTO commit_failure(parent) VALUES('missing'); END;")
        .execute(&store.writer).await.unwrap();
    let execution = store.execution(admitted.fence).await.unwrap();
    let event = crate::abandonment_contract::event(&admitted.run, EventKind::RunSettled);
    assert_eq!(
        execution.append_event(event).await.unwrap_err(),
        StoreError::NotFound
    );
    assert_write_lock_free(&path).await;
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 10)
            .await
            .unwrap()
            .len(),
        0
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM commit_failure")
        .fetch_one(&store.readers)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
