//! Host Retire has its own terminal result and exact original public source.
use super::*;
#[tokio::test]
async fn host_retire_terminal_source_rejects_result_and_relink_forgery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retire.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = store.with_clock(clock);
    for outcome in [
        crate::AttemptRetirementOutcome::Failed,
        crate::AttemptRetirementOutcome::Interrupted,
    ] {
        let fixture = crate::attempt_control_contract::terminal::fixture(&store, outcome).await;
        checked(&store, &fixture).await;
    }
}
async fn checked(
    store: &crate::SqliteStore,
    fixture: &crate::attempt_control_contract::terminal::Fixture,
) {
    let status = if fixture.receipt.attempt.state == crate::AttemptState::Failed {
        RunStatus::Failed
    } else {
        RunStatus::Interrupted
    };
    let session = &fixture.admitted.session.id;
    let run = &fixture.admitted.run.id;
    let mut tx = store.begin_write().await.unwrap();
    assert!(matches!(
        verify(
            &mut tx,
            session,
            run,
            status,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Ok(true)
    ));
    assert!(matches!(
        verify(&mut tx, session, run, status, &mut Proof::bounded(1)).await,
        Err(Failure::Blocked(DeletionBlocker::ProofLimit))
    ));
    sqlx::query("UPDATE runs SET data=json_set(data,'$.error','forged error') WHERE id=$1")
        .bind(&run.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        verify(
            &mut tx,
            session,
            run,
            status,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    sqlx::query("UPDATE runs SET data=json_set(data,'$.error',$2,'$.usage.output_tokens',98765) WHERE id=$1").bind(&run.0).bind(crate::attempt_control::terminal::ERROR).execute(&mut *tx).await.unwrap();
    assert!(matches!(
        verify(
            &mut tx,
            session,
            run,
            status,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback().await.unwrap();
    reject_coherent_status(store, fixture).await;
    let mut tx = store.begin_write().await.unwrap();
    let mut audit = fixture.audit.clone();
    audit.payload["settlement"] = serde_json::to_value(&fixture.older).unwrap();
    sqlx::query("UPDATE events SET data=$1 WHERE seq=$2")
        .bind(serde_json::to_string(&audit).unwrap())
        .bind(i64::try_from(fixture.receipt.event_cursor.0).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        verify(
            &mut tx,
            session,
            run,
            status,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback().await.unwrap();
}

async fn reject_coherent_status(
    store: &crate::SqliteStore,
    fixture: &crate::attempt_control_contract::terminal::Fixture,
) {
    let status = if fixture.receipt.attempt.state == crate::AttemptState::Failed {
        RunStatus::Interrupted
    } else {
        RunStatus::Failed
    };
    let raw = if status == RunStatus::Failed {
        "failed"
    } else {
        "interrupted"
    };
    let mut tx = store.begin_write().await.unwrap();
    sqlx::query("UPDATE runs SET status=$2,data=json_set(data,'$.status',$2) WHERE id=$1")
        .bind(&fixture.admitted.run.id.0)
        .bind(raw)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE native_run_issuances SET data_json=json_set(data_json,'$.state.status',$2) WHERE run_id=$1 AND sequence=(SELECT max(sequence) FROM native_run_issuances WHERE run_id=$1)").bind(&fixture.admitted.run.id.0).bind(raw).execute(&mut *tx).await.unwrap();
    assert!(matches!(
        super::super::super::verify(
            &mut tx,
            &fixture.admitted.session.id,
            &fixture.admitted.run.id,
            status,
            &mut Proof::bounded(MAX_PROOF_BYTES)
        )
        .await,
        Err(Failure::Error(DeletionError::UnknownStoreOutcome))
    ));
    tx.rollback().await.unwrap();
}
