use crate::{PostgresStore, RunControlError, Store, run_control_contract};
use crabber_core::ManualClock;
use std::sync::{Arc, atomic::Ordering};
use time::OffsetDateTime;
#[tokio::test]
async fn durable_run_control_contract() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)));
    run_control_contract::run_contract(|clock| store.clone().with_clock(clock)).await;
    run_control_contract::run_fault_contract(&store, |point| {
        store.control_fault.store(point, Ordering::SeqCst);
    })
    .await;
    corrupted_carriers(&store).await;
    corrupted_termination(&store).await;
    run_control_contract::process::run(
        &store,
        "postgres::run_control::tests::durable_control_process_helper",
        None,
    )
    .await;
}
#[tokio::test]
async fn durable_control_process_helper() {
    let Some(command) = run_control_contract::process::command() else {
        return;
    };
    let url = crate::postgres::tests::test_url().expect("child URL");
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)));
    run_control_contract::process::execute(&store, command).await;
}

async fn corrupted_carriers(store: &PostgresStore) {
    let (request, original, execution) = run_control_contract::corruption_fixture(store).await;
    let session = &request.target.session_id.0;
    let operation = &request.id.0;
    let mut tx = store.pool.begin().await.unwrap();
    sqlx::query("UPDATE run_controls SET summary_json=$3 WHERE session_id=$1 AND operation_id=$2")
        .bind(session)
        .bind(operation)
        .bind(serde_json::to_string(&original).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "DELETE FROM run_control_progress WHERE session_id=$1 AND operation_id=$2 AND event_seq>$3",
    )
    .bind(session)
    .bind(operation)
    .bind(i64::try_from(original.event_cursor.0).unwrap())
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        store.run_controls(&request.target).await.unwrap_err(),
        RunControlError::UnknownStoreOutcome
    );
    assert_eq!(
        execution.observe_run_controls(None).await.unwrap_err(),
        RunControlError::UnknownStoreOutcome
    );
    assert!(
        store
            .lookup_run_control(&request.target.session_id, &request.id)
            .await
            .unwrap()
            .is_some()
    );
    let mut tx = store.pool.begin().await.unwrap();
    for table in ["run_control_progress", "run_controls", "operation_receipts"] {
        sqlx::query(&format!(
            "DELETE FROM {table} WHERE session_id=$1 AND operation_id=$2"
        ))
        .bind(session)
        .bind(operation)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(
        store.run_controls(&request.target).await.unwrap_err(),
        RunControlError::UnknownStoreOutcome
    );
}

async fn corrupted_termination(store: &PostgresStore) {
    for corruption in 0..4 {
        let (request, termination, continuation) =
            run_control_contract::termination_fixture(store).await;
        let mut tx = store.pool.begin().await.unwrap();
        match corruption {
            0 => {
                let data: String =
                    sqlx::query_scalar("SELECT data_json FROM tool_continuations WHERE id=$1")
                        .bind(&continuation.0)
                        .fetch_one(&mut *tx)
                        .await
                        .unwrap();
                let mut value: serde_json::Value = serde_json::from_str(&data).unwrap();
                value.as_object_mut().unwrap().remove("termination");
                sqlx::query("UPDATE tool_continuations SET data_json=$2 WHERE id=$1")
                    .bind(&continuation.0)
                    .bind(value.to_string())
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
            1 => {
                sqlx::query("UPDATE operation_receipts SET kind='attempt' WHERE session_id=$1 AND operation_id=$2").bind(&request.target.session_id.0).bind(&request.id.0).execute(&mut *tx).await.unwrap();
            }
            2 => {
                for table in ["run_control_progress", "run_controls", "operation_receipts"] {
                    sqlx::query(&format!(
                        "DELETE FROM {table} WHERE session_id=$1 AND operation_id=$2"
                    ))
                    .bind(&request.target.session_id.0)
                    .bind(&request.id.0)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                }
            }
            _ => {
                sqlx::query(
                    "DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2",
                )
                .bind(&request.target.session_id.0)
                .bind(&termination.0)
                .execute(&mut *tx)
                .await
                .unwrap();
            }
        }
        tx.commit().await.unwrap();
        assert_eq!(
            store
                .tool_continuation_snapshot(&request.target.session_id, &request.target.run_id)
                .await
                .unwrap_err(),
            crate::ToolContinuationError::UnknownStoreOutcome
        );
        let replay = store.control_run(request).await;
        if matches!(corruption, 0 | 3) {
            assert!(matches!(
                replay.unwrap(),
                crate::RunControlOutcome::Replayed(_)
            ));
        } else {
            assert!(replay.is_err());
        }
    }
}
