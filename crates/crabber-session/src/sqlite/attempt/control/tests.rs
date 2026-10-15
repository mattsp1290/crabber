#[tokio::test]
async fn attempt_control_contract() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    crate::attempt_control_contract::run_contract(|clock| store.with_clock(clock)).await;
}
#[tokio::test]
async fn attempt_control_faults() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    crate::attempt_control_contract::faults::run(&store, |point| {
        store
            .attempt_fault
            .store(point, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
}
use crate::attempt_control_contract::process;
#[tokio::test]
async fn process_helper() {
    let Some(f) = process::fixture() else {
        return;
    };
    let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
    if let Ok(marker) = std::env::var("CRABBER_PARENT_CONTROL_BARRIER") {
        let store = crate::sqlite::test_support::connect_with_test_options(
            std::path::Path::new(&path),
            crate::sqlite::test_support::TestOptions {
                barrier: Some(marker.into()),
                ..Default::default()
            },
        )
        .await;
        process::write(&store, &f).await.unwrap();
    } else {
        let store = super::super::super::SqliteStore::connect(&path)
            .await
            .unwrap();
        process::verify(&store, f).await;
    }
}
#[tokio::test]
async fn parent_controls_lost_ack_survives_fresh_process() {
    let (store, path, _directory) = crate::sqlite::tests::temp_store().await;
    for retire in [false, true] {
        let f = process::pending(&store, retire).await;
        store
            .attempt_fault
            .store(2, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            process::write(&store, &f).await.unwrap_err(),
            crate::AttemptError::UnknownStoreOutcome
        );
        store
            .attempt_fault
            .store(0, std::sync::atomic::Ordering::SeqCst);
        process::spawn(
            "sqlite::attempt::control::tests::process_helper",
            &f,
            Some(&path),
            &[],
        )
        .wait()
        .await;
    }
}
#[tokio::test]
async fn killed_parent_control_writer_rolls_back_every_boundary() {
    use crate::sqlite::test_support;
    let _guard = test_support::BARRIER_LOCK.lock().await;
    for retire in [false, true] {
        let (store, path, directory) = crate::sqlite::tests::temp_store().await;
        let f = process::pending(&store, retire).await;
        let marker = directory.path().join("parent-control.marker");
        sqlx::query("CREATE TRIGGER parent_control_barrier AFTER INSERT ON operation_receipts BEGIN SELECT 1 WHERE 'a' < 'b' COLLATE crabber_test_barrier; END").execute(&store.writer).await.unwrap();
        let mut child = test_support::child(
            &path,
            "sqlite::attempt::control::tests::process_helper",
            &[
                (
                    "CRABBER_PARENT_CONTROL_CHILD",
                    serde_json::to_string(&f).unwrap(),
                ),
                (
                    "CRABBER_PARENT_CONTROL_BARRIER",
                    marker.to_str().unwrap().into(),
                ),
            ],
        );
        test_support::marker(&marker, &mut child).await;
        process::unchanged(&store, &f).await;
        child.kill();
        sqlx::query("DROP TRIGGER parent_control_barrier")
            .execute(&store.writer)
            .await
            .unwrap();
        process::unchanged(&store, &f).await;
        process::write(&store, &f).await.unwrap();
        process::spawn(
            "sqlite::attempt::control::tests::process_helper",
            &f,
            Some(&path),
            &[],
        )
        .wait()
        .await;
    }
}

#[tokio::test]
async fn parent_retirement_rejects_forged_terminal_records() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    for forged in ["arguments", "result", "output"] {
        let (request, call, phase) =
            crate::attempt_control_contract::integrity::ready(&store).await;
        if forged == "output" {
            let text: String =
                sqlx::query_scalar("SELECT outcome_json FROM tool_phases WHERE id=$1")
                    .bind(&phase.0)
                    .fetch_one(&store.writer)
                    .await
                    .unwrap();
            let mut value: serde_json::Value = serde_json::from_str(&text).unwrap();
            value["output"] = serde_json::to_value(
                crabber_core::ToolOutput::from_json(&serde_json::json!("forged native output"))
                    .unwrap(),
            )
            .unwrap();
            sqlx::query("UPDATE tool_phases SET outcome_json=$2 WHERE id=$1")
                .bind(&phase.0)
                .bind(value.to_string())
                .execute(&store.writer)
                .await
                .unwrap();
        } else {
            let text: String = sqlx::query_scalar("SELECT data FROM tool_calls WHERE id=$1")
                .bind(&call.0)
                .fetch_one(&store.writer)
                .await
                .unwrap();
            let mut value: serde_json::Value = serde_json::from_str(&text).unwrap();
            if forged == "arguments" {
                value["arguments"] = serde_json::json!({"forged":true});
            } else {
                value["result"]["content"] =
                    serde_json::json!([{"type":"text","text":"forged logical output"}]);
            }
            let decoded: crabber_core::ToolCallRecord =
                serde_json::from_value(value.clone()).unwrap();
            let accounting = crate::sql_snapshot::call_record(&decoded).unwrap();
            sqlx::query("UPDATE tool_calls SET data=$2,snapshot_parts=$3,snapshot_text=$4,snapshot_bytes=$5 WHERE id=$1").bind(&call.0).bind(&accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes).execute(&store.writer).await.unwrap();
        }
        crate::attempt_control_contract::integrity::refused(&store, request).await;
    }
}

#[tokio::test]
async fn parent_retirement_requires_original_claimed_approval_provenance() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let store = store.with_clock(clock.clone());
    for case in 0..6 {
        let (request, id) =
            crate::attempt_control_contract::integrity::claimed(&store, &clock).await;
        let text: String = sqlx::query_scalar("SELECT data_json FROM continuations WHERE id=$1")
            .bind(&id.0)
            .fetch_one(&store.writer)
            .await
            .unwrap();
        let mut record: crate::approval::ApprovalRecord = serde_json::from_str(&text).unwrap();
        crate::attempt_control_contract::integrity::forge_approval(&mut record, case);
        sqlx::query("UPDATE continuations SET data_json=$2 WHERE id=$1")
            .bind(&id.0)
            .bind(serde_json::to_string(&record).unwrap())
            .execute(&store.writer)
            .await
            .unwrap();
        crate::attempt_control_contract::integrity::refused(&store, request).await;
    }
}
