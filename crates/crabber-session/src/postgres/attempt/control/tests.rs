#[tokio::test]
async fn attempt_control_contract() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::super::PostgresStore::migrate(&url)
            .await
            .unwrap();
        let store = super::super::super::PostgresStore::connect(&url)
            .await
            .unwrap();
        crate::attempt_control_contract::run_contract(|clock| store.with_clock(clock)).await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn attempt_control_faults() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::super::PostgresStore::migrate(&url)
            .await
            .unwrap();
        let store = super::super::super::PostgresStore::connect(&url)
            .await
            .unwrap();
        crate::attempt_control_contract::faults::run(&store, |point| {
            store
                .attempt_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
    })
    .await
    .unwrap();
}
use crate::attempt_control_contract::process;
#[tokio::test]
async fn process_helper() {
    let Some(f) = process::fixture() else {
        return;
    };
    let store = super::super::super::PostgresStore::connect(
        &crabber_test_support::postgres::TestDatabase::attached_url()
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    if std::env::var_os("CRABBER_PARENT_CONTROL_WRITE").is_some() {
        process::write(&store, &f).await.unwrap();
    } else {
        process::verify(&store, f).await;
    }
}
#[tokio::test]
async fn parent_controls_lost_ack_survives_fresh_process() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::super::PostgresStore::migrate(&url)
            .await
            .unwrap();
        let store = super::super::super::PostgresStore::connect(&url)
            .await
            .unwrap();
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
                "postgres::attempt::control::tests::process_helper",
                &f,
                crabber_test_support::ChildDatabase::Postgres(&url),
                &[],
            )
            .wait()
            .await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn killed_parent_control_writer_rolls_back_every_boundary() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

    super::super::super::PostgresStore::migrate(&url)
        .await
        .unwrap();
    let store = super::super::super::PostgresStore::connect(&url)
        .await
        .unwrap();
    for retire in [false, true] {
        let f = process::pending(&store, retire).await;
        let name = format!("parent_control_barrier_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.session_id='{}' THEN PERFORM pg_advisory_xact_lock(751302925); END IF; RETURN NEW; END $$",f.control.scope().0.0)).execute(&store.pool).await.unwrap();
        sqlx::query(&format!("CREATE TRIGGER {name} AFTER INSERT ON operation_receipts FOR EACH ROW EXECUTE FUNCTION {name}()")).execute(&store.pool).await.unwrap();
        let mut held = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(751302925)")
            .execute(&mut *held)
            .await
            .unwrap();
        let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *held)
            .await
            .unwrap();
        let mut child = process::spawn(
            "postgres::attempt::control::tests::process_helper",
            &f,
            crabber_test_support::ChildDatabase::Postgres(&url),
            &[("CRABBER_PARENT_CONTROL_WRITE", "1".into())],
        );
        let waiter=tokio::time::timeout(std::time::Duration::from_secs(10),async{loop{let pid:Option<i32>=sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'INSERT INTO operation_receipts%'").bind(blocker).fetch_optional(&store.pool).await.unwrap();if let Some(pid)=pid{break pid;}tokio::time::sleep(std::time::Duration::from_millis(10)).await;}}).await.expect("parent writer must reach receipt barrier");
        process::unchanged(&store, &f).await;
        child.kill();
        sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=$1 AND datname=current_database()")
            .bind(waiter)
            .execute(&store.pool)
            .await
            .unwrap();
        held.rollback().await.unwrap();
        sqlx::query(&format!("DROP TRIGGER {name} ON operation_receipts"))
            .execute(&store.pool)
            .await
            .unwrap();
        sqlx::query(&format!("DROP FUNCTION {name}()"))
            .execute(&store.pool)
            .await
            .unwrap();
        process::unchanged(&store, &f).await;
        process::write(&store, &f).await.unwrap();
        process::spawn(
            "postgres::attempt::control::tests::process_helper",
            &f,
            crabber_test_support::ChildDatabase::Postgres(&url),
            &[],
        )
        .wait()
        .await;
    }

    }).await.unwrap();
}

#[tokio::test]
async fn parent_retirement_rejects_forged_terminal_records() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

    super::super::super::PostgresStore::migrate(&url)
        .await
        .unwrap();
    let store = super::super::super::PostgresStore::connect(&url)
        .await
        .unwrap();
    for forged in ["arguments", "result", "output"] {
        let (request, call, phase) =
            crate::attempt_control_contract::integrity::ready(&store).await;
        if forged == "output" {
            let text: String =
                sqlx::query_scalar("SELECT outcome_json FROM tool_phases WHERE id=$1")
                    .bind(&phase.0)
                    .fetch_one(&store.pool)
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
                .execute(&store.pool)
                .await
                .unwrap();
        } else {
            let text: String =
                sqlx::query_scalar("SELECT snapshot_record FROM tool_calls WHERE id=$1")
                    .bind(&call.0)
                    .fetch_one(&store.pool)
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
            sqlx::query("UPDATE tool_calls SET snapshot_record=$2,data=$3,snapshot_parts=$4,snapshot_text=$5,snapshot_bytes=$6 WHERE id=$1").bind(&call.0).bind(&accounting.record) .bind(sqlx::types::Json(value.clone())).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes).execute(&store.pool).await.unwrap();
        }
        crate::attempt_control_contract::integrity::refused(&store, request).await;
    }

    }).await.unwrap();
}

#[tokio::test]
async fn parent_retirement_requires_original_claimed_approval_provenance() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::super::PostgresStore::migrate(&url)
            .await
            .unwrap();
        let store = super::super::super::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        for case in 0..6 {
            let (request, id) =
                crate::attempt_control_contract::integrity::claimed(&store, &clock).await;
            let text: String =
                sqlx::query_scalar("SELECT data_json FROM continuations WHERE id=$1")
                    .bind(&id.0)
                    .fetch_one(&store.pool)
                    .await
                    .unwrap();
            let mut record: crate::approval::ApprovalRecord = serde_json::from_str(&text).unwrap();
            crate::attempt_control_contract::integrity::forge_approval(&mut record, case);
            sqlx::query("UPDATE continuations SET data_json=$2 WHERE id=$1")
                .bind(&id.0)
                .bind(serde_json::to_string(&record).unwrap())
                .execute(&store.pool)
                .await
                .unwrap();
            crate::attempt_control_contract::integrity::refused(&store, request).await;
        }
    })
    .await
    .unwrap();
}
