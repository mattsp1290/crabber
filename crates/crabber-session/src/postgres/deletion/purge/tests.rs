//! Internal physical purge qualification; no fixture asserts lifecycle readiness.
use crate::sql_deletion::{
    VerifiedScope,
    contract::{self, Graph},
};
use crate::{
    DeleteSession, DeletionError, DeletionReceipt, PostgresStore, SessionDisposition, Store,
};
use crabber_core::{ControlId, RunFence, SessionId};
use sqlx::{Postgres, Transaction};
fn clock() -> std::sync::Arc<crabber_core::ManualClock> {
    std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ))
}
async fn graph(store: &PostgresStore) -> (Graph, DeleteSession) {
    let created = store
        .manage_session(crate::sql_fork_contract::create())
        .await
        .unwrap();
    let graph = Graph::new(created.session_id.clone());
    sqlx::query(&graph.run_sql())
        .execute(&store.pool)
        .await
        .unwrap();
    let event: i64 = sqlx::query_scalar(&graph.event_sql())
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let mut tx = store.pool.begin().await.unwrap();
    for statement in graph.statements(event, true) {
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
async fn tombstone(tx: &mut Transaction<'_, Postgres>, request: &DeleteSession) -> DeletionReceipt {
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
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url)
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
        .fetch_one(&store.pool)
        .await
        .unwrap();
    rollback_fault(&store, &request).await;
    let mut tx = store.pool.begin().await.unwrap();
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
            .fetch_one(&store.pool)
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
    let cold = PostgresStore::connect(&url).await.unwrap();
    assert_eq!(
        cold.lookup_session_deletion(&request.session_id, &request.id)
            .await
            .unwrap(),
        Some(receipt)
    );
}
async fn missing_scope(store: &PostgresStore, request: &DeleteSession) {
    let mut missing = store.pool.begin().await.unwrap();
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
    store: &PostgresStore,
    target: &Graph,
    survivor: &SessionId,
    request: &DeleteSession,
) {
    let mut stale = store.pool.begin().await.unwrap();
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
    sqlx::query("INSERT INTO messages(id,session_id,run_id,data,snapshot_parts,snapshot_text,snapshot_bytes,snapshot_record) VALUES($1,$2,$3,'{}',0,0,2,'{}')")
        .bind(&id).bind(&survivor.0).bind(&target.run).execute(&store.pool).await.unwrap();
    let mut tx = store.pool.begin().await.unwrap();
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
        .execute(&store.pool)
        .await
        .unwrap();
}
async fn rollback_fault(store: &PostgresStore, request: &DeleteSession) {
    let name = format!("purge_fault_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'purge rollback'; END$$; CREATE TRIGGER {name} BEFORE DELETE ON sessions FOR EACH ROW WHEN(OLD.id='{}') EXECUTE FUNCTION {name}();",request.session_id.0)).execute(&store.pool).await.unwrap();
    let mut tx = store.pool.begin().await.unwrap();
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
    sqlx::raw_sql(&format!(
        "DROP TRIGGER {name} ON sessions; DROP FUNCTION {name}();"
    ))
    .execute(&store.pool)
    .await
    .unwrap();
}
#[tokio::test]
async fn purge_process_rollback_and_cold_commit() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock());
    let (_, request) = graph(&store).await;
    contract::spawn(
        "postgres::deletion::purge::tests::process_helper",
        &request,
        None,
        true,
    )
    .await;
    let cold = PostgresStore::connect(&url).await.unwrap();
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
        "postgres::deletion::purge::tests::process_helper",
        &request,
        None,
        false,
    )
    .await;
    let cold = PostgresStore::connect(&url).await.unwrap();
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
    let store = PostgresStore::connect(&crate::postgres::tests::test_url().unwrap())
        .await
        .unwrap();
    let mut tx = store.pool.begin().await.unwrap();
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
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock());

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
            .fetch_one(&store.pool)
            .await
            .unwrap();
    let mut tx = store.pool.begin().await.unwrap();
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
        .bind(&target.run_id.0).bind(&target.session_id.0).fetch_one(&store.pool).await.unwrap();
    assert!(!remains);
    let after: String =
        sqlx::query_scalar("SELECT data_json FROM preparation_admission_witnesses WHERE run_id=$1")
            .bind(&survivor.run_id.0)
            .fetch_one(&store.pool)
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
