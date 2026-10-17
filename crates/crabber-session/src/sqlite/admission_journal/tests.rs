use super::Proof;

use crate::Store;
use crabber_core::SessionId;
#[tokio::test]
async fn execution_sources_are_atomic_immutable_and_reject_state_rollback() {
    let (store, _path, _directory) = super::super::tests::temp_store().await;
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
            .fetch_one(&store.writer)
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
                .execute(&store.writer)
                .await
                .is_err(),
            "{query}"
        );
    }
    for algorithm in ["REPLACE", "IGNORE"] {
        let source = format!(
            "INSERT OR {algorithm} INTO admission_execution_sources SELECT * FROM admission_execution_sources WHERE run_id=$1"
        );
        assert!(
            sqlx::query(&source)
                .bind(&receipt.run_id.0)
                .execute(&store.writer)
                .await
                .is_err()
        );
        let cohort = format!(
            "INSERT OR {algorithm} INTO admission_executions(run_id,session_id,admission_key,capsule_version,start_state,data,source_protocol) SELECT run_id,session_id,admission_key,capsule_version,start_state,data,0 FROM admission_executions WHERE run_id=$1"
        );
        assert!(
            sqlx::query(&cohort)
                .bind(&receipt.run_id.0)
                .execute(&store.writer)
                .await
                .is_err()
        );
    }
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    execution.begin_admission_execution().await.unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM admission_execution_sources WHERE run_id=$1")
            .bind(&receipt.run_id.0)
            .fetch_one(&store.writer)
            .await
            .unwrap();
    assert_eq!(count, 2);
    assert_eq!(
        execution.begin_admission_execution().await.unwrap_err(),
        crate::AdmissionExecutionError::AlreadyStarted
    );
    sqlx::query("UPDATE admission_executions SET start_state='Unstarted',data=json_set(data,'$.state','Unstarted') WHERE run_id=$1").bind(&receipt.run_id.0).execute(&store.writer).await.unwrap();
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
    store.writer.close().await;
}

#[cfg(test)]
#[tokio::test]
async fn legacy_execution_cohort_cannot_gain_original_no_effect_sources() {
    use crate::Store;
    let (store, _path, _directory) = super::super::tests::temp_store().await;
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
        .bind(&receipt.run_id.0).bind(&receipt.session_id.0).bind(keyed.options.key.as_str()).bind(record.encode().unwrap()).execute(&store.writer).await.unwrap();
    assert!(
        store
            .load_admission_execution(&receipt.session_id, &keyed.options.key)
            .await
            .unwrap()
            .is_some()
    );
    assert!(sqlx::query("INSERT INTO admission_execution_sources(session_id,run_id,kind,data_json) VALUES($1,$2,'init','{}')").bind(&receipt.session_id.0).bind(&receipt.run_id.0).execute(&store.writer).await.is_err());
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
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
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
    store.writer.close().await;
}
