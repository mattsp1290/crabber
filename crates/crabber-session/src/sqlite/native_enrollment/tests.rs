//! Actual native admission, immutable negative source, rollback and replay.
use super::*;
use crate::{KeyedAdmitOutcome, Store};
fn proof() -> Proof {
    Proof::bounded(32 << 20)
}

#[tokio::test]
async fn raw_birth_original_source_is_bounded_immutable_and_independent_of_current_run() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("execution-enrollment.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let session = SessionId::new();
    let request =
        crate::admission_execution_contract::keyed(&session, time::OffsetDateTime::now_utc());
    let KeyedAdmitOutcome::Started { admitted, .. } =
        store.admit_keyed_run(request.clone()).await.unwrap()
    else {
        panic!("fresh admission")
    };
    let run = &admitted.run.id;
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let mut measured = proof();
    assert_eq!(
        original(&mut tx, &session, run, &mut measured)
            .await
            .unwrap(),
        Status::Unclassified
    );
    assert_eq!(
        original(&mut tx, &session, run, &mut Proof::bounded(measured.used()))
            .await
            .unwrap(),
        Status::Unclassified
    );
    assert!(matches!(
        original(
            &mut tx,
            &session,
            run,
            &mut Proof::bounded(measured.used() - 1)
        )
        .await,
        Err(Failure::Limited(_))
    ));
    let before: String =
        sqlx::query_scalar("SELECT data FROM native_execution_unclassified WHERE run_id=$1")
            .bind(&run.0)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    // Current projections cannot replace or invalidate exact original negative metadata.
    sqlx::query("UPDATE runs SET claim_token=$2 WHERE id=$1")
        .bind(&run.0)
        .bind("changed-current-token")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        original(&mut tx, &session, run, &mut proof())
            .await
            .unwrap(),
        Status::Unclassified
    );
    let corrupt: bool = sqlx::query_scalar(crate::sql_deletion::CROSS_SCOPE)
        .bind(&session.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(!corrupt);
    tx.rollback().await.unwrap();
    immutable(&store, run).await;
    Box::pin(reserved_kind_is_not_supported(&store, &admitted.run)).await;
    assert!(matches!(
        store.admit_keyed_run(request).await.unwrap(),
        KeyedAdmitOutcome::Replayed(_)
    ));
    let after: String =
        sqlx::query_scalar("SELECT data FROM native_execution_unclassified WHERE run_id=$1")
            .bind(&run.0)
            .fetch_one(&store.writer)
            .await
            .unwrap();
    assert_eq!(before, after);
    assert_eq!(count(&store, &session).await, (1, 1, 1));
}

async fn immutable(store: &crate::SqliteStore, run: &RunId) {
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    for statement in [
        "UPDATE runs SET execution_enrollment_protocol=0,execution_enrollment_origin_id=NULL WHERE id=$1",
        "DELETE FROM runs WHERE id=$1",
        "UPDATE native_execution_origins SET birth_kind='pending',unclassified_run_id=NULL WHERE run_id=$1",
        "UPDATE native_execution_origins SET data='{}' WHERE run_id=$1",
        "DELETE FROM native_execution_origins WHERE run_id=$1",
        "UPDATE native_execution_unclassified SET data='{}' WHERE run_id=$1",
        "DELETE FROM native_execution_unclassified WHERE run_id=$1",
    ] {
        let mut save = tx.begin().await.unwrap();
        assert!(
            sqlx::query(statement)
                .bind(&run.0)
                .execute(&mut *save)
                .await
                .is_err(),
            "{statement}"
        );
        save.rollback().await.unwrap();
    }
    tx.rollback().await.unwrap();
}

async fn count(store: &crate::SqliteStore, session: &SessionId) -> (i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT COUNT(*) FROM runs WHERE session_id=$1),(SELECT COUNT(*) FROM native_execution_origins WHERE session_id=$1),(SELECT COUNT(*) FROM native_execution_unclassified WHERE session_id=$1)")
        .bind(&session.0).fetch_one(&store.writer).await.unwrap()
}

#[tokio::test]
async fn raw_birth_before_and_after_commit_faults_keep_exact_atomic_sources() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("execution-enrollment.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    for stage in [1, 2] {
        let session = SessionId::new();
        let request = crate::execution_binding_contract::fixture(
            &session,
            time::OffsetDateTime::now_utc(),
            "enrollment-fault",
            'b',
        );
        store
            .binding_fault
            .store(stage, std::sync::atomic::Ordering::SeqCst);
        assert!(store.admit_bound_keyed_run(request.clone()).await.is_err());
        assert_eq!(
            count(&store, &session).await,
            if stage == 1 { (0, 0, 0) } else { (1, 1, 1) }
        );
        let outcome = store.admit_bound_keyed_run(request).await.unwrap();
        let receipt = match outcome {
            KeyedAdmitOutcome::Started { receipt, .. } => {
                assert_eq!(stage, 1);
                receipt
            }
            KeyedAdmitOutcome::Replayed(receipt) => {
                assert_eq!(stage, 2);
                receipt
            }
        };
        assert_eq!(count(&store, &session).await, (1, 1, 1));
        let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
        assert_eq!(
            original(&mut tx, &session, &receipt.run_id, &mut proof())
                .await
                .unwrap(),
            Status::Unclassified
        );
        tx.rollback().await.unwrap();
    }
}

/// The current bootstrap rejects the old Pending-label-only insert. A separate
/// rollback-only counterfeit still exercises the original-reader negatives.
async fn reserved_kind_is_not_supported(store: &crate::SqliteStore, actual: &crabber_core::Run) {
    let mut run = actual.clone();
    run.id = RunId::new();
    run.status = crabber_core::RunStatus::Completed;
    let issuer = crate::native_parent::advance(
        None,
        crate::native_parent::RunState::from_run(&run).unwrap(),
        None,
    )
    .unwrap()
    .unwrap();
    let source = JournalSource::new(None, &issuer).unwrap();
    let (origin, poison) = Origin::raw_birth(&issuer, &source, &mut proof()).unwrap();
    let digest = origin.digest(&mut proof()).unwrap();
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    sqlx::query("INSERT INTO runs(id,session_id,status,claim_token,lease_until,data,native_source_protocol,execution_enrollment_protocol,execution_enrollment_origin_id) VALUES($1,$2,'completed',$3,0,$4,1,1,$1)")
        .bind(&run.id.0).bind(&run.session_id.0).bind(&run.claim_token).bind(serde_json::to_string(&run).unwrap()).execute(&mut *tx).await.unwrap();
    sqlx::query(
        "INSERT INTO native_run_issuances(session_id,run_id,sequence,data_json) VALUES($1,$2,1,$3)",
    )
    .bind(&run.session_id.0)
    .bind(&run.id.0)
    .bind(serde_json::to_string(&issuer).unwrap())
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("INSERT INTO native_run_sources(session_id,run_id,sequence,issuance_digest,chain_digest) VALUES($1,$2,1,$3,$4)")
        .bind(&run.session_id.0).bind(&run.id.0).bind(source.issuance_digest.as_str()).bind(source.chain_digest.as_str()).execute(&mut *tx).await.unwrap();
    // The exact old fixture is now correctly refused by the real bootstrap.
    let mut save = tx.begin().await.unwrap();
    let error = sqlx::query("INSERT INTO native_execution_origins(session_id,run_id,first_issuer_digest,origin_digest,birth_kind,unclassified_run_id,data) VALUES($1,$2,$3,$4,'pending',NULL,$5)")
        .bind(&run.session_id.0).bind(&run.id.0).bind(source.issuance_digest.as_str()).bind(digest.as_str()).bind(serde_json::to_string(&origin).unwrap()).execute(&mut *save).await.unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("1811")
    );
    save.rollback().await.unwrap();
    // Deliberately bypass ONLY the new immediate assignment guard to inspect
    // corruption. The outer rollback restores this actual production trigger;
    // the remaining CHECK and deferred FK guards are left intact.
    sqlx::query("DROP TRIGGER pending_origin_assignment")
        .execute(&mut *tx)
        .await
        .unwrap();
    // The real native19 source publication already retained the original head.
    sqlx::query("INSERT INTO native_execution_origins(session_id,run_id,first_issuer_digest,origin_digest,birth_kind,unclassified_run_id,pending_epoch,data) VALUES($1,$2,$3,$4,'pending',NULL,'counterfeit-epoch',$5)")
        .bind(&run.session_id.0).bind(&run.id.0).bind(source.issuance_digest.as_str()).bind(digest.as_str()).bind(serde_json::to_string(&origin).unwrap()).execute(&mut *tx).await.unwrap();
    // Keep the complete real Raw poison too: missing poison cannot explain refusal.
    sqlx::query("INSERT INTO native_execution_unclassified(session_id,run_id,origin_digest,data) VALUES($1,$2,$3,$4)")
        .bind(&run.session_id.0).bind(&run.id.0).bind(digest.as_str()).bind(serde_json::to_string(&poison).unwrap()).execute(&mut *tx).await.unwrap();
    assert!(
        original(&mut tx, &run.session_id, &run.id, &mut proof())
            .await
            .is_err()
    );
    assert!(
        super::super::native_parent::source_protocol::history(
            &mut tx,
            &run.session_id,
            &run.id,
            &mut proof()
        )
        .await
        .is_err()
    );
    let unsupported: bool = sqlx::query_scalar(crate::sql_deletion::CROSS_SCOPE)
        .bind(&run.session_id.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(unsupported);
    tx.rollback().await.unwrap();
}
