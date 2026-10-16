use super::Proof;

use crate::Store;
use crabber_core::SessionId;
#[tokio::test]
async fn execution_sources_are_atomic_immutable_and_reject_state_rollback() {
    let Some(url) = super::super::tests::test_url() else {
        return;
    };
    let _guard = super::super::tests::TEST_LOCK.lock().await;
    super::super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::super::PostgresStore::connect(&url).await.unwrap();
    let now = time::OffsetDateTime::now_utc();
    let keyed = crate::admission_execution_contract::keyed(&SessionId::new(), now);
    let crate::KeyedAdmitOutcome::Started { receipt, admitted } =
        store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!()
    };
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM admission_execution_sources WHERE run_id=$1")
            .bind(&receipt.run_id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    for query in [
        "UPDATE admission_execution_sources SET data_json='{}' WHERE run_id=$1",
        "DELETE FROM admission_execution_sources WHERE run_id=$1",
        "UPDATE admission_executions SET source_protocol=0 WHERE run_id=$1",
    ] {
        assert!(
            sqlx::query(query)
                .bind(&receipt.run_id.0)
                .execute(&store.pool)
                .await
                .is_err(),
            "{query}"
        );
    }
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    execution.begin_admission_execution().await.unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM admission_execution_sources WHERE run_id=$1")
            .bind(&receipt.run_id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(count, 2);
    assert_eq!(
        execution.begin_admission_execution().await.unwrap_err(),
        crate::AdmissionExecutionError::AlreadyStarted
    );
    sqlx::query("UPDATE admission_executions SET start_state='Unstarted',data=jsonb_set(data,'{state}','\"Unstarted\"'::jsonb) WHERE run_id=$1").bind(&receipt.run_id.0).execute(&store.pool).await.unwrap();
    assert!(
        store
            .load_admission_execution(&receipt.session_id, &keyed.options.key)
            .await
            .is_err()
    );
    assert!(execution.begin_admission_execution().await.is_err());
    assert!(
        store
            .abandon_run(crabber_core::AbandonRequest {
                expected: admitted.fence.clone(),
                expected_owner: admitted.run.owner,
                authority: crabber_core::AbandonAuthority::HostStoppedOwner,
            })
            .await
            .is_err()
    );
    store.pool.close().await;
}

#[cfg(test)]
#[tokio::test]
async fn legacy_execution_cohort_cannot_gain_original_no_effect_sources() {
    use crate::Store;
    let Some(url) = super::super::tests::test_url() else {
        return;
    };
    let _guard = super::super::tests::TEST_LOCK.lock().await;
    super::super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::super::PostgresStore::connect(&url).await.unwrap();
    let mut keyed = crate::admission_execution_contract::keyed(
        &SessionId::new(),
        time::OffsetDateTime::now_utc(),
    );
    let capsule = keyed.execution.take().unwrap();
    let crate::KeyedAdmitOutcome::Started { receipt, admitted } =
        store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!()
    };
    let record = crate::AdmissionExecutionRecord {
        receipt: receipt.clone(),
        key: keyed.options.key.clone(),
        capsule,
        state: crate::AdmissionExecutionState::Unstarted,
    };
    sqlx::query("INSERT INTO admission_executions(run_id,session_id,admission_key,capsule_version,start_state,data) VALUES($1,$2,$3,1,'Unstarted',$4)")
        .bind(&receipt.run_id.0).bind(&receipt.session_id.0).bind(keyed.options.key.as_str()).bind(super::super::json(&record).unwrap()).execute(&store.pool).await.unwrap();
    assert!(
        store
            .load_admission_execution(&receipt.session_id, &keyed.options.key)
            .await
            .unwrap()
            .is_some()
    );
    assert!(sqlx::query("INSERT INTO admission_execution_sources(session_id,run_id,kind,data_json) VALUES($1,$2,'init','{}')").bind(&receipt.session_id.0).bind(&receipt.run_id.0).execute(&store.pool).await.is_err());
    let outcome = store
        .abandon_run(crabber_core::AbandonRequest {
            expected: admitted.fence,
            expected_owner: admitted.run.owner,
            authority: crabber_core::AbandonAuthority::HostStoppedOwner,
        })
        .await
        .unwrap();
    assert!(
        outcome.terminal_event.payload["abandonment_v1"]
            .get("no_effect_v1")
            .is_none()
    );
    let mut tx = store.pool.begin().await.unwrap();
    assert!(matches!(
        super::super::abandon_history::verify(
            &mut tx,
            &receipt.session_id,
            &receipt.run_id,
            &mut Proof::bounded(32 * 1024 * 1024)
        )
        .await,
        Ok(false)
    ));
    tx.rollback().await.unwrap();
    store.pool.close().await;
}

#[tokio::test]
async fn oversized_execution_carrier_returns_typed_failure_before_decode() {
    let Some(url) = super::super::tests::test_url() else {
        return;
    };
    let _guard = super::super::tests::TEST_LOCK.lock().await;
    super::super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::super::PostgresStore::connect(&url).await.unwrap();
    let keyed = crate::admission_execution_contract::keyed(
        &SessionId::new(),
        time::OffsetDateTime::now_utc(),
    );
    let crate::KeyedAdmitOutcome::Started { receipt, .. } =
        store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!()
    };
    sqlx::query("UPDATE admission_executions SET data=jsonb_set(data,'{oversized_extension}',to_jsonb(repeat('x',17825792))) WHERE run_id=$1").bind(&receipt.run_id.0).execute(&store.pool).await.unwrap();
    assert!(
        store
            .load_admission_execution(&receipt.session_id, &keyed.options.key)
            .await
            .is_err()
    );
    let mut tx = store.pool.begin().await.unwrap();
    assert!(matches!(
        super::super::admission_execution::load_record_with_proof(
            &mut tx,
            &receipt.run_id,
            &mut Proof::bounded(32 * 1024 * 1024)
        )
        .await,
        Err(crate::public_session::v2::proof::ReadError::Domain(
            crate::StoreError::AdmissionConflict
        ))
    ));
    assert!(matches!(
        super::super::admission_execution::load_record_with_proof(
            &mut tx,
            &receipt.run_id,
            &mut Proof::bounded(1)
        )
        .await,
        Err(crate::public_session::v2::proof::ReadError::Budget(_))
    ));
    tx.rollback().await.unwrap();
    store.pool.close().await;
}
