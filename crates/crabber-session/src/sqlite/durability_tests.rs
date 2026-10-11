//! SQLite-specific lock, damaged-page and backup evidence (T4.2–T4.5).
use super::{
    snapshot_tests::{assistant, call, event, page, query},
    test_support::*,
    tests::*,
    *,
};
use crate::*;
use crabber_core::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn migrate_reopen_and_serialized_writers() {
    let (store, path, _directory) = temp_store().await;
    let before: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM schema_version ORDER BY version")
            .fetch_all(&store.readers)
            .await
            .unwrap();
    SqliteStore::migrate(&path).await.unwrap();
    let after: Vec<i64> = sqlx::query_scalar("SELECT version FROM schema_version ORDER BY version")
        .fetch_all(&store.readers)
        .await
        .unwrap();
    assert_eq!(before, after);
    let other = SqliteStore::connect(&path).await.unwrap();
    let a = store.admit_run(request(&SessionId::new())).await.unwrap();
    let b = other.admit_run(request(&SessionId::new())).await.unwrap();
    let ea = store.execution(a.fence.clone()).await.unwrap();
    let eb = other.execution(b.fence.clone()).await.unwrap();
    let mut holder = raw(&path).await;
    let held = holder.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let ma = assistant(&a, "first");
    let mb = assistant(&b, "second");
    let wa = tokio::spawn(async move { ea.append_message(ma).await });
    let wb = tokio::spawn(async move { eb.append_message(mb).await });
    wait_writers(&store, 1).await;
    wait_writers(&other, 1).await;
    assert!(!wa.is_finished() && !wb.is_finished());
    tokio::time::timeout(Duration::from_secs(2), async {
        assert_eq!(
            store.list_all_messages(&a.session.id).await.unwrap().len(),
            1
        );
        assert_eq!(
            other.list_all_messages(&b.session.id).await.unwrap().len(),
            1
        );
    })
    .await
    .unwrap();
    held.commit().await.unwrap();
    wa.await.unwrap().unwrap();
    wb.await.unwrap().unwrap();
    let mut busy = request(&a.session.id);
    busy.session_id = Some(a.session.id.clone());
    assert_eq!(store.admit_run(busy).await.unwrap_err(), StoreError::Busy);
    let reopened = SqliteStore::connect(&path).await.unwrap();
    for admitted in [&a, &b] {
        let expected = store
            .list_messages(&admitted.session.id, None)
            .await
            .unwrap();
        assert_eq!(expected.len(), 2);
        assert_eq!(
            reopened
                .list_messages(&admitted.session.id, None)
                .await
                .unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn busy_timeout_surfaces_as_sanitized_unknown_outcome() {
    let (store, path, _directory) = temp_store().await;
    let short = connect_with_test_options(
        &path,
        TestOptions {
            busy_timeout: Duration::from_millis(200),
            ..TestOptions::default()
        },
    )
    .await;
    let mut holder = raw(&path).await;
    let held = holder.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let session = SessionId::new();
    let inputs = request(&session);
    // Hosts reconcile this unknown outcome and retry with identical inputs.
    assert_eq!(
        short.admit_run(inputs.clone()).await.unwrap_err(),
        StoreError::Validation("SQLite operation failed".into())
    );
    assert!(store.get_session(&session).await.unwrap().is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs")
            .fetch_one(&store.readers)
            .await
            .unwrap(),
        0
    );
    held.rollback().await.unwrap();
    short.admit_run(inputs).await.unwrap();
}

#[tokio::test]
async fn write_transactions_take_the_lock_at_begin() {
    let (store, path, _directory) = temp_store().await;
    let held = store.begin_write().await.unwrap();
    let mut independent = raw(&path).await;
    assert_busy(&mut independent).await;
    held.rollback().await.unwrap();
    independent
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
}

#[tokio::test]
async fn snapshot_reads_only_records_inside_the_page() {
    for tools in [false, true] {
        let (store, _, _directory) = temp_store().await;
        let a = store.admit_run(request(&SessionId::new())).await.unwrap();
        let execution = store.execution(a.fence.clone()).await.unwrap();
        for i in 0..5 {
            if tools {
                execution
                    .create_tool_call(
                        call(&a, &format!("call-{i}")),
                        event(&a, EventKind::ToolCallPending),
                    )
                    .await
                    .unwrap();
            } else if i > 0 {
                execution
                    .append_message(assistant(&a, &format!("message-{i}")))
                    .await
                    .unwrap();
            }
        }
        let table = if tools { "tool_calls" } else { "messages" };
        sqlx::query(&format!("UPDATE {table} SET data=X'FF',snapshot_bytes=1 WHERE seq>(SELECT min(seq) FROM {table})"))
            .execute(&store.writer).await.unwrap();
        let mut limited = query(&a.session.id);
        limited.limits.messages = if tools { 100 } else { 1 };
        limited.limits.tool_calls = usize::from(tools);
        let first = page(store.snapshot(limited.clone()).await.unwrap());
        assert_eq!(
            if tools {
                first.tool_calls.len()
            } else {
                first.messages.len()
            },
            1
        );
        assert!(first.continuation.is_some());
        limited.limits.messages = 5;
        limited.limits.tool_calls = 5;
        assert!(matches!(
            store.snapshot(limited).await,
            Err(StoreError::Validation(_))
        ));
    }
}

#[tokio::test]
async fn online_backup_during_writes_is_consistent() {
    let (store, path, directory) = temp_store().await;
    let a = store.admit_run(request(&SessionId::new())).await.unwrap();
    let session = a.session.id.clone();
    let execution = store.execution(a.fence.clone()).await.unwrap();
    let mut connection = SqliteConnection::connect_with(&options(&path))
        .await
        .unwrap();
    let copy = directory.path().join("backup.sqlite");
    let done = Arc::new(AtomicBool::new(false));
    let writer_done = done.clone();
    let (mid_tx, mid_rx) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut mid = Some(mid_tx);
            let text = "x".repeat(65_536);
            let mut count = 0;
            // The writer cannot finish naturally before backup starts or completes.
            while !writer_done.load(Ordering::SeqCst) {
                execution
                    .append_message(assistant(&a, &text))
                    .await
                    .unwrap();
                count += 1;
                if count == 11 {
                    mid.take().unwrap().send(()).unwrap();
                }
            }
        })
        .await
        .expect("backup must finish while the bounded writer is active");
    });
    mid_rx.await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        sqlx::query("VACUUM INTO $1")
            .bind(copy.to_str().unwrap())
            .execute(&mut connection),
    )
    .await
    .expect("online backup timed out")
    .unwrap();
    done.store(true, Ordering::SeqCst);
    writer.await.unwrap();
    let mut copied = raw(&copy).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_one(&mut copied)
            .await
            .unwrap(),
        "ok"
    );
    copied.close().await.unwrap();
    SqliteStore::migrate(&copy).await.unwrap();
    let backup = SqliteStore::connect(&copy).await.unwrap();
    let prefix = backup.list_all_messages(&session).await.unwrap();
    let original = store.list_all_messages(&session).await.unwrap();
    assert!(!prefix.is_empty() && prefix.len() <= original.len());
    assert_eq!(prefix, original[..prefix.len()]);
    let mut q = query(&session);
    q.limits.encoded_bytes = 2_000_000;
    assert_ne!(
        page(backup.snapshot(q).await.unwrap()).messages,
        [] as [Message; 0]
    );
    close(&backup).await;
    // Reproduce an overwrite target with stale side files from a different DB.
    let restored = directory.path().join("restored.sqlite");
    SqliteStore::migrate(&restored).await.unwrap();
    let stale = SqliteStore::connect(&restored).await.unwrap();
    stale.admit_run(request(&SessionId::new())).await.unwrap();
    let wal = restored.with_file_name("restored.sqlite-wal");
    let shm = restored.with_file_name("restored.sqlite-shm");
    let wal_bytes = std::fs::read(&wal).unwrap();
    let shm_bytes = std::fs::read(&shm).unwrap();
    close(&stale).await;
    std::fs::copy(&copy, &restored).unwrap();
    std::fs::write(&wal, wal_bytes).unwrap();
    std::fs::write(&shm, shm_bytes).unwrap();
    assert!(wal.exists() && shm.exists());
    // Restore procedure: stop users, overwrite main file, remove stale side files,
    // then migrate to re-enable WAL and connect. Side files are never reused.
    std::fs::remove_file(wal).unwrap();
    std::fs::remove_file(shm).unwrap();
    SqliteStore::migrate(&restored).await.unwrap();
    let restored = SqliteStore::connect(&restored).await.unwrap();
    assert_eq!(restored.list_all_messages(&session).await.unwrap(), prefix);
    assert!(matches!(
        restored.snapshot(query(&session)).await.unwrap(),
        SnapshotOutcome::Page(_)
    ));
}
