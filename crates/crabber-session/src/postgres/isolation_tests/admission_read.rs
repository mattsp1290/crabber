//! Host admission reads stay coherent while a worker commits Unstarted-to-Started Begin.
use super::*;
use crate::{AdmissionExecutionState, KeyedAdmitOutcome};
use std::time::Duration;

const READ_BARRIER: i64 = 751_302_930;

#[tokio::test]
async fn host_admission_reads_use_one_snapshot_across_worker_begin() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let admin = admin(&url).await;
        let role = ProvisionedRole::host(&url, &admin).await;
        let host = PostgresStore::connect(&role.url).await.unwrap();
        let function = format!("admission_read_barrier_{}", role.name);
        // Test-only restrictive policy: it grants nothing, and pauses only the
        // provisioned host after its statement snapshot exists.
        sqlx::query(&format!("CREATE FUNCTION {function}() RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$ BEGIN PERFORM pg_advisory_xact_lock({READ_BARRIER}); RETURN true; END $$"))
            .execute(&admin.pool).await.unwrap();
        sqlx::query(&format!("CREATE POLICY {function} ON admission_executions AS RESTRICTIVE FOR SELECT TO {} USING ({function}())", role.name))
            .execute(&admin.pool).await.unwrap();
        for lookup in ["state", "record"] {
            let session = SessionId::new();
            let request = crate::admission_execution_contract::keyed(&session, time::OffsetDateTime::now_utc());
            let key = request.options.key.clone();
            let KeyedAdmitOutcome::Started { receipt, admitted } =
                admin.admit_keyed_run(request).await.unwrap()
            else {
                panic!("fresh keyed admission must start")
            };
            let mut held = crate::postgres::transaction::begin(&admin.pool).await.unwrap();
            sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(READ_BARRIER).execute(&mut *held).await.unwrap();
            let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *held).await.unwrap();
            let reader = async {
                match lookup {
                    "state" => host.admission_execution_state(&receipt.run_id).await.map_err(|e| format!("{e:?}")),
                    _ => host
                        .load_admission_execution(&session, &key)
                        .await
                        .map(|record| record.map(|record| record.state))
                        .map_err(|e| format!("{e:?}")),
                }
            };
            let control = async {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let paused: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND usename=$1 AND $2=ANY(pg_blocking_pids(pid)) AND query LIKE '%FROM admission_executions%')")
                            .bind(&role.name).bind(blocker).fetch_one(&admin.pool).await.unwrap();
                        if paused { break; }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }).await.expect("host read must reach the snapshot barrier");
                admin.execution(admitted.fence.clone()).await.unwrap().begin_admission_execution().await.unwrap();
                assert_eq!(
                    admin.admission_execution_state(&receipt.run_id).await.unwrap(),
                    Some(AdmissionExecutionState::Started),
                    "{lookup}: Begin must commit before the paused read resumes"
                );
                held.rollback().await.unwrap();
            };
            let (read, ()) = tokio::join!(reader, control);
            assert_eq!(read, Ok(Some(AdmissionExecutionState::Unstarted)), "{lookup}");
            assert_eq!(
                host.admission_execution_state(&receipt.run_id).await.unwrap(),
                Some(AdmissionExecutionState::Started),
                "{lookup}"
            );
        }
        sqlx::query(&format!("DROP POLICY {function} ON admission_executions")).execute(&admin.pool).await.unwrap();
        sqlx::query(&format!("DROP FUNCTION {function}()")).execute(&admin.pool).await.unwrap();
        host.pool.close().await;
        role.cleanup().await;
    })
    .await
    .unwrap();
}
