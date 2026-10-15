//! Real opposing connection paths: every writer waits at the session gate before
//! locking a run, while independent sessions continue to make progress.
use super::{
    PostgresStore, load_run,
    tests::{TEST_LOCK, input, test_url},
};
use crate::{ClaimUnstartedAdmissionRequest, KeyedAdmitOutcome, Store};
use crabber_core::{
    AbandonAuthority, AbandonRequest, Clock, EventKind, ManualClock, RunStatus, SessionId,
};
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

#[tokio::test]
async fn session_gate_precedes_run_rows_for_existing_mutation_paths() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(OffsetDateTime::now_utc()));
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let other = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let before: i64 = sqlx::query_scalar(
        "SELECT deadlocks FROM pg_stat_database WHERE datname=current_database()",
    )
    .fetch_one(&store.pool)
    .await
    .unwrap();
    for mode in 0..9 {
        let now = clock.now();
        let keyed = crate::admission_execution_contract::keyed(&SessionId::new(), now);
        let KeyedAdmitOutcome::Started { receipt, admitted } =
            store.admit_keyed_run(keyed.clone()).await.unwrap()
        else {
            panic!("fresh admission must start")
        };
        let execution = other.execution(admitted.fence.clone()).await.unwrap();
        if mode < 6 || mode == 8 {
            execution.begin_admission_execution().await.unwrap();
        }
        if matches!(mode, 4 | 5 | 7) {
            clock.set(admitted.run.lease_until);
        }
        let mut held = store
            .session_transaction(&admitted.session.id)
            .await
            .unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *held)
            .await
            .unwrap();
        let writer = tokio::spawn(run_mutation(
            mode,
            other.clone(),
            execution,
            keyed.clone(),
            receipt,
            admitted.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(!writer.is_finished(), "mode {mode} bypassed the held session gate");
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1 = ANY(pg_blocking_pids(pid)) AND wait_event='advisory')").bind(pid).fetch_one(&store.pool).await.unwrap();
                if blocked { break; }
                tokio::task::yield_now().await;
            }
        }).await.expect("writer must reach the session gate");
        // This is the opposing path: the gate owner can still take the run row.
        // A run-first writer waiting on the gate would deadlock this acquisition.
        tokio::time::timeout(
            Duration::from_secs(2),
            load_run(&mut held, &admitted.run.id, true),
        )
        .await
        .expect("gate owner must acquire run row without a lock inversion")
        .unwrap();
        held.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), writer)
            .await
            .unwrap()
            .unwrap();
        let retained = store.get_run(&admitted.run.id).await.unwrap().unwrap();
        if mode == 5 {
            assert_eq!(retained.status, RunStatus::Interrupted);
        }
        if matches!(mode, 4 | 7) {
            assert_ne!(retained.claim_token, admitted.fence.claim_token);
        }
    }
    let after: i64 = sqlx::query_scalar(
        "SELECT deadlocks FROM pg_stat_database WHERE datname=current_database()",
    )
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert_eq!(
        after, before,
        "opposing write paths must not produce PostgreSQL 40P01 deadlocks"
    );
    store.pool.close().await;
    other.pool.close().await;
}

async fn run_mutation(
    mode: u8,
    writer_store: PostgresStore,
    execution: Box<dyn crate::ExecutionStore>,
    original: crate::KeyedAdmitRequest,
    receipt: crabber_core::AdmissionReceipt,
    admitted: Box<crate::AdmitOutcome>,
) {
    let run = admitted.run.clone();
    let fence = admitted.fence.clone();

    match mode {
        0 => execution
            .renew_lease(run.lease_until + time::Duration::seconds(60))
            .await
            .unwrap(),
        1 => execution
            .append_event(crate::abandonment_contract::event(
                &run,
                EventKind::RunStarted,
            ))
            .await
            .unwrap(),
        2 => {
            let mut message = input(&run.session_id, "new message");
            message.run_id = Some(run.id.clone());
            execution.append_message(message).await.unwrap();
        }
        3 => writer_store
            .enqueue_inbox(
                &run.session_id,
                crate::InboxKind::Steer,
                input(&run.session_id, "steer"),
            )
            .await
            .unwrap(),
        4 => {
            writer_store
                .claim_expired_run(&run.id, "replacement")
                .await
                .unwrap();
        }
        5 => {
            writer_store
                .abandon_run(AbandonRequest {
                    expected: fence.clone(),
                    expected_owner: run.owner.clone(),
                    authority: AbandonAuthority::ExpiredLease,
                })
                .await
                .unwrap();
        }
        6 => execution.begin_admission_execution().await.unwrap(),
        7 => {
            writer_store
                .claim_unstarted_admission(ClaimUnstartedAdmissionRequest {
                    session_id: run.session_id.clone(),
                    key: original.options.key.clone(),
                    expected_fence: fence.clone(),
                    expected_owner: run.owner.clone(),
                    fingerprint: receipt.fingerprint.clone(),
                    behavior_fingerprint: original.options.behavior_fingerprint.clone(),
                    semantic_digest: receipt.semantic_digest.clone(),
                    capsule_digest: original.execution.as_ref().unwrap().digest().unwrap(),
                    owner: "replacement".into(),
                    lease: Duration::from_secs(30),
                })
                .await
                .unwrap();
        }
        _ => {
            assert!(matches!(
                writer_store.admit_keyed_run(original).await.unwrap(),
                KeyedAdmitOutcome::Replayed(_)
            ));
        }
    }
}
