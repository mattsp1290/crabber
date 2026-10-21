//! Internal physical purge qualification; no fixture asserts lifecycle readiness.
use crate::sql_deletion::{
    VerifiedScope,
    contract::{self, Graph},
};
use crate::transaction::Transaction;
use crate::{
    DeleteSession, DeletionError, DeletionReceipt, SessionDisposition, SqliteStore, Store,
};
use crabber_core::{ControlId, RunFence, SessionId};
use sqlx::Sqlite;
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
        crabber_test_support::ChildDatabase::Sqlite(&path),
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
        crabber_test_support::ChildDatabase::Sqlite(&path),
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

#[tokio::test]
async fn scoped_purge_removes_only_owned_admission_witness() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let store = store.with_clock(clock());

    let target =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let survivor =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let request = DeleteSession {
        id: ControlId::new(),
        session_id: target.session_id.clone(),
        expected_revision: store.session_revision(&target.session_id).await.unwrap(),
    };
    let before: String =
        sqlx::query_scalar("SELECT data_json FROM preparation_admission_witnesses WHERE run_id=$1")
            .bind(&survivor.run_id.0)
            .fetch_one(&store.writer)
            .await
            .unwrap();
    let mut tx = store.begin_write().await.unwrap();
    tombstone(&mut tx, &request).await;
    // This private fixture tests physical routing, not lifecycle readiness.
    super::purge(
        &mut tx,
        &VerifiedScope::fixture(request.session_id.clone(), request.expected_revision),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let remains: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM preparation_admission_witnesses WHERE run_id=$1 OR session_id=$2)")
        .bind(&target.run_id.0).bind(&target.session_id.0).fetch_one(&store.writer).await.unwrap();
    assert!(!remains);
    let after: String =
        sqlx::query_scalar("SELECT data_json FROM preparation_admission_witnesses WHERE run_id=$1")
            .bind(&survivor.run_id.0)
            .fetch_one(&store.writer)
            .await
            .unwrap();
    assert_eq!(before, after);
    assert!(
        store
            .get_session(&target.session_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.get_run(&survivor.run_id).await.unwrap().is_some());
}

#[tokio::test]
async fn protected_native_source_purge_requires_tombstone_even_for_missing_head_run() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("native-source-purge.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::connect(&path).await.unwrap();
    let session = SessionId::new();
    let keyed =
        crate::admission_execution_contract::keyed(&session, time::OffsetDateTime::now_utc());
    let crate::KeyedAdmitOutcome::Started { receipt, .. } =
        store.admit_keyed_run(keyed).await.unwrap()
    else {
        panic!()
    };
    let raw = crabber_core::RunId::new();
    sqlx::query("INSERT INTO runs(id,session_id,status,claim_token,lease_until,data,native_source_protocol) SELECT $1,session_id,'completed',claim_token,lease_until,data,1 FROM runs WHERE id=$2").bind(&raw.0).bind(&receipt.run_id.0).execute(&store.writer).await.unwrap();
    assert!(
        sqlx::query("DELETE FROM runs WHERE id=$1")
            .bind(&raw.0)
            .execute(&store.writer)
            .await
            .is_err()
    );
    let recursive: i64 = sqlx::query_scalar("PRAGMA recursive_triggers")
        .fetch_one(&store.writer)
        .await
        .unwrap();
    assert_eq!(recursive, 0);
    for algorithm in ["REPLACE", "IGNORE"] {
        let query = format!(
            "INSERT OR {algorithm} INTO runs(seq,id,session_id,status,claim_token,lease_until,data,native_source_protocol) SELECT seq,$2,session_id,status,claim_token,lease_until,data,1 FROM runs WHERE id=$1"
        );
        assert!(
            sqlx::query(&query)
                .bind(&raw.0)
                .bind(crabber_core::RunId::new().0)
                .execute(&store.writer)
                .await
                .is_err()
        );
    }
    let revision = store.session_revision(&session).await.unwrap();
    let request = DeleteSession {
        id: ControlId::new(),
        session_id: session.clone(),
        expected_revision: revision,
    };
    let scope = VerifiedScope::fixture(session.clone(), revision);
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    tombstone(&mut tx, &request).await;
    super::purge(&mut tx, &scope).await.unwrap();
    tx.rollback().await.unwrap();
    let sources: i64 =
        sqlx::query_scalar("SELECT count(*) FROM native_run_sources WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&store.writer)
            .await
            .unwrap();
    assert_eq!(sources, 1);
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    tombstone(&mut tx, &request).await;
    super::purge(&mut tx, &scope).await.unwrap();
    tx.commit().await.unwrap();
    for table in [
        "native_run_source_heads",
        "native_run_sources",
        "native_run_issuances",
        "runs",
        "sessions",
    ] {
        let key = if table == "sessions" {
            "id"
        } else {
            "session_id"
        };
        let query = format!("SELECT count(*) FROM {table} WHERE {key}=$1");
        let count: i64 = sqlx::query_scalar(&query)
            .bind(&session.0)
            .fetch_one(&store.writer)
            .await
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
}
