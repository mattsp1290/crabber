//! Real opposing connection paths: every writer waits at the session gate before
//! locking a run, while independent sessions continue to make progress.
use super::{PostgresStore, load_run, tests::input};
use crate::{ClaimUnstartedAdmissionRequest, KeyedAdmitOutcome, Store};
use crabber_core::{
    AbandonAuthority, AbandonRequest, Clock, EventKind, ManualClock, RunStatus, SessionId,
};
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

#[tokio::test]
#[allow(clippy::too_many_lines)] // Preserve the causal history inside its explicit fixture scope.
async fn session_gate_precedes_run_rows_for_existing_mutation_paths() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

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
    for mode in 0..13 {
        let now = clock.now();
        let session = SessionId::new();
        let bound = (mode >= 11).then(|| {
            crate::execution_binding_contract::fixture(&session, now, "opposing-backend", 'b')
        });
        let keyed = bound.as_ref().map_or_else(
            || crate::admission_execution_contract::keyed(&session, now),
            |bound| bound.keyed.clone(),
        );
        let outcome = if let Some(bound) = bound {
            store.admit_bound_keyed_run(bound).await
        } else {
            store.admit_keyed_run(keyed.clone()).await
        };
        let KeyedAdmitOutcome::Started { receipt, admitted } = outcome.unwrap() else {
            panic!("fresh admission must start")
        };
        let execution = other.execution(admitted.fence.clone()).await.unwrap();
        if mode < 6 || matches!(mode, 8..=12) {
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

    }).await.unwrap();
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
        8..=12 => {
            run_native_mutation(mode, &writer_store, execution.as_ref(), original, &run).await;
        }
        _ => unreachable!("fixed mutation mode"),
    }
}

async fn run_native_mutation(
    mode: u8,
    writer_store: &PostgresStore,
    execution: &dyn crate::ExecutionStore,
    original: crate::KeyedAdmitRequest,
    run: &crabber_core::Run,
) {
    match mode {
        8 => {
            assert!(matches!(
                writer_store.admit_keyed_run(original).await.unwrap(),
                KeyedAdmitOutcome::Replayed(_)
            ));
        }
        9 => {
            execution
                .mutate_public_state(crate::PublicStateMutation {
                    id: crabber_core::ControlId::new(),
                    expected_revision: writer_store
                        .session_revision(&run.session_id)
                        .await
                        .unwrap(),
                    contents: crate::public_state_contract::contents(),
                })
                .await
                .unwrap();
        }
        10 => {
            let request = crate::attempt_contract::reserve(
                writer_store
                    .session_revision(&run.session_id)
                    .await
                    .unwrap(),
            );
            execution.mutate_attempt(request, None).await.unwrap();
        }
        11 => {
            let bound = crate::execution_binding_contract::fixture(
                &run.session_id,
                run.created_at,
                "opposing-backend",
                'b',
            );
            assert!(matches!(
                writer_store.admit_bound_keyed_run(bound).await.unwrap(),
                KeyedAdmitOutcome::Replayed(_)
            ));
        }
        12 => {
            let bound = crate::execution_binding_contract::fixture(
                &run.session_id,
                run.created_at,
                "opposing-backend",
                'b',
            );
            assert_eq!(
                writer_store
                    .admit_bound_run(crate::BoundAdmitRequest {
                        request: bound.keyed.request,
                        binding: bound.binding
                    })
                    .await
                    .unwrap_err(),
                crate::StoreError::Busy
            );
        }
        _ => unreachable!("fixed native mutation mode"),
    }
}

#[tokio::test]
async fn foreign_run_records_are_rejected_before_taking_their_row_locks() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    use super::tests::request;
    use crabber_core::{
        ContentBlock, Part, PartId, PartKind, ToolCallId, ToolCallRecord, ToolCallStatus,
        ToolResult, ToolResultStatus,
    };
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        PostgresStore::migrate(&url).await.unwrap();
        let store = PostgresStore::connect(&url).await.unwrap();
        let a = store.admit_run(request(&SessionId::new())).await.unwrap();
        let request_b = request(&SessionId::new());
        let message_b = request_b.user_message.id.clone();
        let b = store.admit_run(request_b).await.unwrap();
        let execution_a = store.execution(a.fence.clone()).await.unwrap();
        let execution_b = store.execution(b.fence.clone()).await.unwrap();
        let call_b = ToolCallId::new();
        execution_b
            .create_tool_call(
                ToolCallRecord {
                    id: call_b.clone(),
                    run_id: b.run.id.clone(),
                    name: "fixture".into(),
                    arguments: serde_json::json!({}),
                    status: ToolCallStatus::Pending,
                    retry_safe: false,
                    result: None,
                },
                crate::abandonment_contract::event(&b.run, EventKind::ToolCallPending),
            )
            .await
            .unwrap();
        let mut held = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        for (table, id) in [
            ("messages", &message_b.0),
            ("tool_calls", &call_b.0),
            ("epochs", &b.run.epoch_id.0),
        ] {
            sqlx::query(&format!("SELECT id FROM {table} WHERE id=$1 FOR UPDATE"))
                .bind(id)
                .fetch_one(&mut *held)
                .await
                .unwrap();
        }
        denied(execution_a.append_part(Part {
            id: PartId::new(),
            message_id: message_b.clone(),
            ordinal: 99,
            kind: PartKind::UserInputText,
            content: ContentBlock::Text {
                text: "foreign".into(),
            },
        }))
        .await;
        denied(execution_a.claim_tool_call(
            &call_b,
            crate::abandonment_contract::event(&a.run, EventKind::ToolCallRunning),
        ))
        .await;
        let mut result_message = input(&a.run.session_id, "result");
        result_message.run_id = Some(a.run.id.clone());
        denied(execution_a.settle_tool_call(
            &call_b,
            ToolResult {
                status: ToolResultStatus::Completed,
                content: vec![],
            },
            result_message,
            crate::abandonment_contract::event(&a.run, EventKind::ToolCallSettled),
        ))
        .await;
        let mut summary = input(&a.run.session_id, "summary");
        summary.run_id = Some(a.run.id.clone());
        summary.parts[0].kind = PartKind::CompactionSummary;
        denied(execution_a.finish_epoch(&b.run.epoch_id, summary)).await;
        held.rollback().await.unwrap();
        assert_eq!(
            store.list_unfinished_tool_calls(&b.run.id).await.unwrap()[0].status,
            ToolCallStatus::Pending
        );
        assert_eq!(
            store
                .list_all_messages(&b.run.session_id)
                .await
                .unwrap()
                .len(),
            1
        );
        store.pool.close().await;
    })
    .await
    .unwrap();
}

async fn denied(future: impl std::future::Future<Output = Result<(), crate::StoreError>>) {
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), future)
            .await
            .expect("foreign record denial must not wait on its row lock")
            .unwrap_err(),
        crate::StoreError::NotFound
    );
}
