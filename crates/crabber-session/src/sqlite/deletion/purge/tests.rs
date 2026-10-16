//! Internal physical purge qualification; no fixture asserts lifecycle readiness.
use crate::sql_deletion::{
    VerifiedScope,
    contract::{self, Graph},
};
use crate::{
    DeleteSession, DeletionError, DeletionReceipt, SessionDisposition, SqliteStore, Store,
};
use crabber_core::{ControlId, RunFence, SessionId};
use sqlx::{Sqlite, Transaction};
fn clock() -> std::sync::Arc<crabber_core::ManualClock> {
    std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ))
}
async fn graph(store: &SqliteStore) -> (Graph, DeleteSession) {
    let created = store
        .manage_session(crate::sql_fork_contract::create())
        .await
        .unwrap();
    let graph = Graph::new(created.session_id.clone());
    sqlx::query(&graph.run_sql())
        .execute(&store.writer)
        .await
        .unwrap();
    let event: i64 = sqlx::query_scalar(&graph.event_sql())
        .fetch_one(&store.writer)
        .await
        .unwrap();
    let mut tx = store.begin_write().await.unwrap();
    for statement in graph.statements(event, false) {
        sqlx::query(&statement).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    let request = DeleteSession {
        id: ControlId::new(),
        session_id: created.session_id,
        expected_revision: created.revision,
    };
    (graph, request)
}
async fn tombstone(tx: &mut Transaction<'_, Sqlite>, request: &DeleteSession) -> DeletionReceipt {
    let receipt = DeletionReceipt {
        session_id: request.session_id.clone(),
        operation_id: request.id.clone(),
        semantic_digest: request.fingerprint().unwrap(),
        final_revision: request.expected_revision.next().unwrap(),
        deleted_at: time::OffsetDateTime::UNIX_EPOCH
            .replace_nanosecond(1)
            .unwrap(),
    };
    sqlx::query("INSERT INTO session_tombstones(session_id,operation_id,expected_revision,final_revision,semantic_digest,deleted_at_seconds,deleted_at_nanos,format_version) VALUES($1,$2,$3,$4,$5,0,1,1)")
        .bind(&request.session_id.0).bind(&request.id.0).bind(i64::try_from(request.expected_revision.get()).unwrap())
        .bind(i64::try_from(receipt.final_revision.get()).unwrap()).bind(receipt.semantic_digest.as_str()).execute(&mut **tx).await.unwrap();
    receipt
}
#[tokio::test]
async fn atomic_purge_preserves_survivor_cursors_and_registry() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("purge.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock());
    let (target, request) = graph(&store).await;
    let survivor = crate::fork_pending_contract::fixture(&store).await;
    let survivor_session = &survivor.request.source_session_id;
    cross_scope(&store, &target, survivor_session, &request).await;
    let before = store
        .list_events(survivor_session, None, 128)
        .await
        .unwrap();
    let continuation = contract::first_snapshot(&store, survivor_session).await;
    let tail: i64 = sqlx::query_scalar(&target.event_sql())
        .fetch_one(&store.writer)
        .await
        .unwrap();
    rollback_fault(&store, &request).await;
    let mut tx = store.begin_write().await.unwrap();
    let receipt = tombstone(&mut tx, &request).await;
    super::purge(
        &mut tx,
        &VerifiedScope::fixture(request.session_id.clone(), request.expected_revision),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    missing_scope(&store, &request).await;
    assert_eq!(
        store
            .list_events(survivor_session, None, 128)
            .await
            .unwrap(),
        before
    );
    contract::unchanged_snapshot(&store, continuation).await;
    assert_eq!(
        store
            .lookup_session_deletion(&request.session_id, &request.id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert!(matches!(
        store
            .session_disposition(&request.session_id)
            .await
            .unwrap(),
        SessionDisposition::Deleted { .. }
    ));
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM tool_call_identities WHERE id=$1 OR id=$2")
            .bind(&target.call)
            .bind(&target.imported)
            .fetch_one(&store.writer)
            .await
            .unwrap();
    assert_eq!(count, 0);
    let run = store.get_run(&survivor.run).await.unwrap().unwrap();
    let mut event = before.last().unwrap().clone();
    event.cursor = None;
    event.correlation = None;
    event.kind = crabber_core::EventKind::ExtensionNotice;
    event.payload = serde_json::json!({"survivor": true});
    store
        .execution(RunFence {
            run_id: run.id,
            claim_token: run.claim_token,
        })
        .await
        .unwrap()
        .append_event(event)
        .await
        .unwrap();
    let after = store
        .list_events(survivor_session, Some(contract::cursor(tail)), 8)
        .await
        .unwrap();
    assert_eq!(after.len(), 1);
    assert!(after[0].cursor.unwrap().0 > u64::try_from(tail).unwrap());
    let cold = SqliteStore::connect(&path).await.unwrap();
    assert_eq!(
        cold.lookup_session_deletion(&request.session_id, &request.id)
            .await
            .unwrap(),
        Some(receipt)
    );
}
async fn missing_scope(store: &SqliteStore, request: &DeleteSession) {
    let mut missing = store.begin_write().await.unwrap();
    assert_eq!(
        super::purge(
            &mut missing,
            &VerifiedScope::fixture(request.session_id.clone(), request.expected_revision)
        )
        .await
        .unwrap_err(),
        DeletionError::NotFound
    );
    missing.rollback().await.unwrap();
}
async fn cross_scope(
    store: &SqliteStore,
    target: &Graph,
    survivor: &SessionId,
    request: &DeleteSession,
) {
    let mut stale = store.begin_write().await.unwrap();
    assert_eq!(
        super::purge(
            &mut stale,
            &VerifiedScope::fixture(
                request.session_id.clone(),
                request.expected_revision.next().unwrap()
            )
        )
        .await
        .unwrap_err(),
        DeletionError::RevisionConflict
    );
    stale.rollback().await.unwrap();
    let id = format!("cross-{}", uuid::Uuid::new_v4());
    sqlx::query("INSERT INTO messages(id,session_id,run_id,data,snapshot_parts,snapshot_text,snapshot_bytes) VALUES($1,$2,$3,'{}',0,0,2)")
        .bind(&id).bind(&survivor.0).bind(&target.run).execute(&store.writer).await.unwrap();
    let mut tx = store.begin_write().await.unwrap();
    assert_eq!(
        super::purge(
            &mut tx,
            &VerifiedScope::fixture(request.session_id.clone(), request.expected_revision)
        )
        .await
        .unwrap_err(),
        DeletionError::UnknownStoreOutcome
    );
    tx.rollback().await.unwrap();
    assert!(
        store
            .get_session(&request.session_id)
            .await
            .unwrap()
            .is_some()
    );
    sqlx::query("DELETE FROM messages WHERE id=$1")
        .bind(&id)
        .execute(&store.writer)
        .await
        .unwrap();
}
async fn rollback_fault(store: &SqliteStore, request: &DeleteSession) {
    sqlx::raw_sql(&format!("CREATE TRIGGER purge_fault BEFORE DELETE ON sessions WHEN OLD.id='{}' BEGIN SELECT RAISE(ABORT,'purge rollback'); END;",request.session_id.0)).execute(&store.writer).await.unwrap();
    let mut tx = store.begin_write().await.unwrap();
    tombstone(&mut tx, request).await;
    assert_eq!(
        super::purge(
            &mut tx,
            &VerifiedScope::fixture(request.session_id.clone(), request.expected_revision)
        )
        .await
        .unwrap_err(),
        DeletionError::UnknownStoreOutcome
    );
    tx.rollback().await.unwrap();
    assert!(
        store
            .get_session(&request.session_id)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store
            .lookup_session_deletion(&request.session_id, &request.id)
            .await
            .unwrap(),
        None
    );
    sqlx::raw_sql("DROP TRIGGER purge_fault")
        .execute(&store.writer)
        .await
        .unwrap();
}
#[tokio::test]
async fn purge_process_rollback_and_cold_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("purge.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock());
    let (_, request) = graph(&store).await;
    contract::spawn(
        "sqlite::deletion::purge::tests::process_helper",
        &request,
        Some(&path),
        true,
    )
    .await;
    let cold = SqliteStore::connect(&path).await.unwrap();
    assert!(
        cold.get_session(&request.session_id)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        cold.lookup_session_deletion(&request.session_id, &request.id)
            .await
            .unwrap(),
        None
    );
    contract::spawn(
        "sqlite::deletion::purge::tests::process_helper",
        &request,
        Some(&path),
        false,
    )
    .await;
    let cold = SqliteStore::connect(&path).await.unwrap();
    assert!(
        cold.get_session(&request.session_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        cold.lookup_session_deletion(&request.session_id, &request.id)
            .await
            .unwrap()
            .is_some()
    );
}
#[tokio::test]
async fn process_helper() {
    let Ok(raw) = std::env::var("CRABBER_PURGE_CHILD") else {
        return;
    };
    let (request, crash): (DeleteSession, bool) = serde_json::from_str(&raw).unwrap();
    let store = SqliteStore::connect(std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap())
        .await
        .unwrap();
    let mut tx = store.begin_write().await.unwrap();
    tombstone(&mut tx, &request).await;
    super::purge(
        &mut tx,
        &VerifiedScope::fixture(request.session_id.clone(), request.expected_revision),
    )
    .await
    .unwrap();
    if crash {
        std::process::exit(83);
    }
    tx.commit().await.unwrap();
    // No acknowledgment receipt crosses the process boundary.
}
