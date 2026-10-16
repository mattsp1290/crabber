//! Actual native journals exercise the private bounded readiness stages.
use super::*;
use crate::Store;
use crate::deletion::coverage::CoverageRead;
async fn coverage(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
) -> VerifiedDeletionCoverage {
    let CoverageRead::Verified(proof) =
        super::super::super::ownership_origin::coverage(tx, session)
            .await
            .unwrap()
    else {
        panic!("creation coverage")
    };
    proof
}
#[tokio::test]
async fn empty_managed_scope_is_ready_and_pending_input_blocks_without_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("readiness.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let created = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    let mut tx = store.begin_write().await.unwrap();
    let proof = coverage(&mut tx, &created.session_id).await;
    let Readiness::Ready(scope) = readiness(&mut tx, &created.session_id, &proof)
        .await
        .unwrap()
    else {
        panic!("ready empty scope")
    };
    assert_eq!(scope.session(), &created.session_id);
    assert_eq!(scope.revision(), created.revision);
    sqlx::query("UPDATE sessions SET public_revision=public_revision+1 WHERE id=$1")
        .bind(&created.session_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        readiness(&mut tx, &created.session_id, &proof).await,
        Err(DeletionError::RevisionConflict)
    ));
    tx.rollback().await.unwrap();
    sqlx::query("INSERT INTO inbox(session_id,kind,data) VALUES($1,'user',$2)")
        .bind(&created.session_id.0)
        .bind("{}")
        .execute(&store.writer)
        .await
        .unwrap();
    let mut tx = store.begin_write().await.unwrap();
    let proof = coverage(&mut tx, &created.session_id).await;
    assert!(matches!(
        readiness(&mut tx, &created.session_id, &proof)
            .await
            .unwrap(),
        Readiness::Blocked(DeletionBlocker::PendingInput)
    ));
    tx.rollback().await.unwrap();
    assert!(
        store
            .get_session(&created.session_id)
            .await
            .unwrap()
            .is_some()
    );
}
#[tokio::test]
async fn native_cleanup_authenticates_output_before_reporting_phase_blocker() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("readiness.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let store = store.with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let fixture = crate::fork_pending_contract::fixture(&store).await;
    let session = &fixture.request.source_session_id;
    let mut tx = store.begin_write().await.unwrap();
    let runtime = native::runtime(&mut tx, session, &mut Proof::bounded(MAX_PROOF_BYTES)).await;
    assert!(matches!(
        runtime,
        Err(Failure::Blocked(DeletionBlocker::Runtime))
    ));
    let attempts = native::attempts(&mut tx, session, &mut Proof::bounded(MAX_PROOF_BYTES)).await;
    assert!(matches!(
        attempts,
        Err(Failure::Blocked(DeletionBlocker::Attempt))
    ));
    let tiny = native::phases(&mut tx, session, &mut Proof::bounded(1)).await;
    assert!(matches!(
        tiny,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    let phase = native::phases(&mut tx, session, &mut Proof::bounded(MAX_PROOF_BYTES)).await;
    assert!(matches!(
        phase,
        Err(Failure::Blocked(DeletionBlocker::Phase))
    ));
    tx.rollback().await.unwrap();
    let mut tx = store.begin_write().await.unwrap();
    sqlx::query(
        "UPDATE tool_phases SET outcome_json=replace(hex(zeroblob(33554433)),'00','x') WHERE id=$1",
    )
    .bind(&fixture.target.id.0)
    .execute(&mut *tx)
    .await
    .unwrap();
    let oversized = native::phases(&mut tx, session, &mut Proof::bounded(MAX_PROOF_BYTES)).await;
    assert!(matches!(
        oversized,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    tx.rollback().await.unwrap();
    let (call, _, output) = crate::fork_pending_contract::forge(&store, &fixture).await;
    let mut tx = store.begin_write().await.unwrap();
    let mut phase =
        super::super::super::phase::load_host_record(&mut tx, &fixture.run, &fixture.target.id)
            .await
            .unwrap()
            .unwrap();
    phase.output = Some(output);
    super::super::super::phase::save_host_record(&mut tx, &phase, false)
        .await
        .unwrap();
    let accounting = crate::sql_snapshot::call_record(&call).unwrap();
    sqlx::query("UPDATE tool_calls SET data=$2,snapshot_parts=$3,snapshot_text=$4,snapshot_bytes=$5 WHERE id=$1").bind(&call.id.0).bind(accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes).execute(&mut *tx).await.unwrap();
    let phase = native::phases(&mut tx, session, &mut Proof::bounded(MAX_PROOF_BYTES)).await;
    assert!(matches!(
        phase,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn released_native_checkpoint_authenticates_exact_suspended_ancestry() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("released.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let store = store.with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let fixture = crate::tool_continuation_contract::lifecycle(&store).await;
    let session = &fixture.admitted.session.id;
    let mut tx = store.begin_write().await.unwrap();
    let mut proof = Proof::bounded(MAX_PROOF_BYTES);
    assert!(native::runtime(&mut tx, session, &mut proof).await.is_ok());
    assert!(matches!(
        native::attempts(&mut tx, session, &mut proof).await,
        Ok(true)
    ));
    assert!(matches!(
        native::phases(&mut tx, session, &mut proof).await,
        Ok(true)
    ));
    assert!(
        domains::continuations(&mut tx, session, &mut proof)
            .await
            .is_ok()
    );
    assert!(
        native::released_suspensions(&mut tx, session, &mut proof)
            .await
            .is_ok()
    );
    assert!(matches!(
        super::super::super::ownership_origin::coverage(&mut tx, session)
            .await
            .unwrap(),
        CoverageRead::Unknown
    ));
    sqlx::query("DELETE FROM tool_continuation_phases WHERE phase_id=$1")
        .bind(&fixture.suspension.phases[0].previous.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        native::released_suspensions(&mut tx, session, &mut Proof::bounded(MAX_PROOF_BYTES)).await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    sqlx::query("DELETE FROM tool_continuation_interactions WHERE phase_id=$1")
        .bind(&fixture.suspension.phases[0].previous.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM tool_phases WHERE id=$1")
        .bind(&fixture.suspension.phases[0].previous.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        native::phases(&mut tx, session, &mut Proof::bounded(MAX_PROOF_BYTES)).await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    sqlx::query("DELETE FROM execution_attempts WHERE id=$1")
        .bind(&fixture.suspension.attempt.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        native::attempts(&mut tx, session, &mut Proof::bounded(MAX_PROOF_BYTES)).await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    sqlx::query("UPDATE runs SET claim_token='forged-token',data=json_set(data,'$.claim_token','forged-token') WHERE id=$1").bind(&fixture.admitted.run.id.0).execute(&mut *tx).await.unwrap();
    assert!(matches!(
        native::runtime(&mut tx, session, &mut Proof::bounded(MAX_PROOF_BYTES)).await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    sqlx::query("UPDATE native_run_issuances SET data_json=replace(hex(zeroblob(33554433)),'00','x') WHERE run_id=$1 AND sequence=1").bind(&fixture.admitted.run.id.0).execute(&mut *tx).await.unwrap();
    assert!(matches!(
        native::runtime(&mut tx, session, &mut Proof::bounded(MAX_PROOF_BYTES)).await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn generic_joined_registry_does_not_upgrade_creation_coverage() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("readiness.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let created = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    crate::sql_deletion::readiness::contract::generic_joined(&store, &created.summary).await;
    let mut tx = store.begin_write().await.unwrap();
    let coverage = coverage(&mut tx, &created.session_id).await;
    assert!(matches!(
        readiness(&mut tx, &created.session_id, &coverage)
            .await
            .unwrap(),
        Readiness::Blocked(DeletionBlocker::OwnershipUnknown)
    ));
    tx.rollback().await.unwrap();
}
