//! SQLite process evidence: each child opens its own pool in a fresh process.
use super::{
    test_support::{
        self, BARRIER_LOCK, ChildGuard, TestOptions, close, connect_with_test_options, marker, raw,
        wait_writers,
    },
    tests::{request, temp_store},
    *,
};
use crabber_core::{AdmissionOptions, EventKind, InputFingerprint};
use std::time::Duration;

use super::records::save_run;
use crate::*;
use crabber_core::*;
use sqlx::Row;
use time::OffsetDateTime;

fn keyed(session: &SessionId, key: &str) -> KeyedAdmitRequest {
    let mut request = request(session);
    request.session_id = Some(session.clone());
    KeyedAdmitRequest {
        execution: None,
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
async fn finish(store: &SqliteStore, admitted: &AdmitOutcome) {
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
async fn counts(store: &SqliteStore, session: &SessionId) -> (i64, i64, i64) {
    let row = sqlx::query("SELECT (SELECT count(*) FROM runs WHERE session_id=$1) AS runs, (SELECT count(*) FROM messages WHERE session_id=$1) AS messages, (SELECT count(*) FROM admission_receipts WHERE session_id=$1) AS receipts")
        .bind(&session.0).fetch_one(&store.readers).await.unwrap();
    (row.get("runs"), row.get("messages"), row.get("receipts"))
}

#[tokio::test]
async fn independent_pools_and_saturated_existing_session() {
    let (store, path, _directory) = temp_store().await;
    let other = SqliteStore::connect(&path).await.unwrap();
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

    let mut holder = raw(&path).await;
    let held = holder.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let store = store.clone();
        let request = keyed(&session, "second");
        tasks.push(tokio::spawn(async move {
            store.admit_keyed_run(request).await.unwrap()
        }));
    }
    wait_writers(&store, 10).await;
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
    close(&store).await;
    close(&other).await;
}

fn child(
    path: &Path,
    session: &SessionId,
    mode: &str,
    expected: Option<&AdmissionReceipt>,
) -> ChildGuard {
    let mut env = vec![
        ("CRABBER_SQLITE_RECEIPT_CHILD_SESSION", session.0.clone()),
        ("CRABBER_SQLITE_RECEIPT_CHILD_MODE", mode.to_string()),
    ];
    if let Some(expected) = expected {
        env.push((
            "CRABBER_SQLITE_RECEIPT_EXPECTED",
            serde_json::to_string(expected).unwrap(),
        ));
    }
    test_support::child(path, "sqlite::admission_tests::process_helper", &env)
}
async fn wait_child(child: &mut ChildGuard) {
    child.wait().await;
}

#[tokio::test]
async fn process_helper() {
    let Ok(session) = std::env::var("CRABBER_SQLITE_RECEIPT_CHILD_SESSION") else {
        return;
    };
    let session = SessionId(session);
    let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
    let store = if let Ok(marker) = std::env::var("CRABBER_SQLITE_BARRIER_MARKER") {
        connect_with_test_options(
            Path::new(&path),
            TestOptions {
                barrier: Some(marker.into()),
                ..TestOptions::default()
            },
        )
        .await
    } else {
        SqliteStore::connect(&path).await.unwrap()
    };
    let mut request = keyed(&session, "process");
    let mode = std::env::var("CRABBER_SQLITE_RECEIPT_CHILD_MODE").unwrap();
    if mode == "start-recoverable" {
        request = crate::admission_execution_contract::keyed(&session, store.clock.now());
        request.options.key = AdmissionKey::new("process").unwrap();
    }
    if mode == "start" || mode == "start-recoverable" {
        let key = request.options.key.clone();
        let returned = receipt(store.admit_keyed_run(request).await.unwrap());
        assert_eq!(
            store.lookup_admission(&session, &key).await.unwrap(),
            Some(returned)
        );
    } else {
        let expected: AdmissionReceipt =
            serde_json::from_str(&std::env::var("CRABBER_SQLITE_RECEIPT_EXPECTED").unwrap())
                .unwrap();
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
    close(&store).await;
}

#[tokio::test]
async fn independent_processes_and_fresh_process_lifecycle() {
    let (_initial, path, _directory) = temp_store().await;
    let session = SessionId::new();
    let mut children: Vec<_> = (0..4)
        .map(|_| child(&path, &session, "start", None))
        .collect();
    for child in &mut children {
        wait_child(child).await;
    }
    let store = SqliteStore::connect(&path).await.unwrap();
    let expected = store
        .lookup_admission(&session, &AdmissionKey::new("process").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counts(&store, &session).await, (1, 1, 1));
    let original = store.get_run(&expected.run_id).await.unwrap().unwrap();
    close(&store).await;
    wait_child(&mut child(&path, &session, "replay", Some(&expected))).await;
    // Persist a truly expired lease so the fresh child uses its ordinary system clock.
    let store = SqliteStore::connect(&path).await.unwrap();
    let mut tx = store.begin_write().await.unwrap();
    let mut expired = original.clone();
    expired.lease_until = OffsetDateTime::now_utc() - time::Duration::seconds(1);
    save_run(&mut tx, &expired).await.unwrap();
    tx.commit().await.unwrap();
    close(&store).await;
    wait_child(&mut child(&path, &session, "replay", Some(&expected))).await;
    let store = SqliteStore::connect(&path).await.unwrap();
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
    close(&store).await;
    wait_child(&mut child(&path, &session, "replay", Some(&expected))).await;
    let store = SqliteStore::connect(&path).await.unwrap();
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
    close(&store).await;
    wait_child(&mut child(&path, &session, "replay", Some(&expected))).await;
}

#[tokio::test]
async fn process_crash_before_commit_rolls_back_every_record() {
    let _guard = BARRIER_LOCK.lock().await;
    let (store, path, directory) = temp_store().await;
    let session = SessionId::new();
    let marker_path = directory.path().join("admission.marker");
    sqlx::query(&format!("CREATE TRIGGER receipt_barrier AFTER INSERT ON admission_executions WHEN NEW.session_id='{}' BEGIN SELECT 1 WHERE 'a' < 'b' COLLATE crabber_test_barrier; END",session.0)).execute(&store.writer).await.unwrap();
    let mut process = test_support::child(
        &path,
        "sqlite::admission_tests::process_helper",
        &[
            ("CRABBER_SQLITE_RECEIPT_CHILD_SESSION", session.0.clone()),
            (
                "CRABBER_SQLITE_RECEIPT_CHILD_MODE",
                "start-recoverable".into(),
            ),
            (
                "CRABBER_SQLITE_BARRIER_MARKER",
                marker_path.to_str().unwrap().into(),
            ),
        ],
    );
    marker(&marker_path, &mut process).await;
    let mut connection = raw(&path).await;
    test_support::assert_busy(&mut connection).await;
    assert_eq!(counts(&store, &session).await, (0, 0, 0));
    assert!(
        store
            .lookup_admission(&session, &AdmissionKey::new("process").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    process.kill();
    connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
    assert!(store.get_session(&session).await.unwrap().is_none());
    assert_eq!(counts(&store, &session).await, (0, 0, 0));
    for table in ["epochs", "admission_executions"] {
        let count: i64 =
            sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE session_id=$1"))
                .bind(&session.0)
                .fetch_one(&store.readers)
                .await
                .unwrap();
        assert_eq!(count, 0);
    }
    sqlx::query("DROP TRIGGER receipt_barrier")
        .execute(&store.writer)
        .await
        .unwrap();
    wait_child(&mut child(&path, &session, "start-recoverable", None)).await;
    let reopened = SqliteStore::connect(&path).await.unwrap();
    assert_eq!(counts(&reopened, &session).await, (1, 1, 1));
    assert!(
        reopened
            .load_admission_execution(&session, &AdmissionKey::new("process").unwrap())
            .await
            .unwrap()
            .is_some()
    );
}
