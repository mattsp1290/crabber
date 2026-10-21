use crate::{ApprovalError as Error, Store};
use crabber_core::{Clock, ManualClock};

#[tokio::test]
async fn approval_metadata_is_scoped_bounded_and_checks_question_identity() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::super::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        let f = crate::approval_contract::fixture::staged(&store, &clock).await;
        check_scope(&store, &f).await;
        let record = seed_record(&store, &f, &clock).await;
        let original = serde_json::to_string(&record).unwrap();
        let expected = record.snapshot(store.session_revision(&f.run.session_id).await.unwrap());
        assert_eq!(
            store
                .approval_snapshot(&f.run.session_id, &f.run.id)
                .await
                .unwrap(),
            Some(expected.clone())
        );
        for data in [
            "x".repeat(4 * 1024 * 1024 + 1),
            "{}".into(),
            original.replace("\"version\":1", "\"version\":9"),
        ] {
            sqlx::query("UPDATE continuations SET data_json=$2 WHERE id=$1")
                .bind(&record.batch.continuation.id.0)
                .bind(data)
                .execute(&store.pool)
                .await
                .unwrap();
            assert_eq!(
                store
                    .approval_snapshot(&f.run.session_id, &f.run.id)
                    .await
                    .unwrap_err(),
                Error::UnknownStoreOutcome
            );
        }
        sqlx::query("UPDATE continuations SET data_json=$2 WHERE id=$1")
            .bind(&record.batch.continuation.id.0)
            .bind(&original)
            .execute(&store.pool)
            .await
            .unwrap();
        let id = &record.questions[0].view.id;
        sqlx::query("UPDATE continuation_interactions SET call_id=$2 WHERE id=$1")
            .bind(&id.0)
            .bind(&record.batch.calls[2].target.call_id.0)
            .execute(&store.pool)
            .await
            .unwrap();
        assert_eq!(
            store
                .approval_snapshot(&f.run.session_id, &f.run.id)
                .await
                .unwrap_err(),
            Error::UnknownStoreOutcome
        );
        sqlx::query("UPDATE continuation_interactions SET call_id=$2 WHERE id=$1")
            .bind(&id.0)
            .bind(&record.batch.calls[0].target.call_id.0)
            .execute(&store.pool)
            .await
            .unwrap();
        assert_eq!(
            store
                .approval_snapshot(&f.run.session_id, &f.run.id)
                .await
                .unwrap(),
            Some(expected)
        );
        check_answer(&store, &f).await;
    })
    .await
    .unwrap();
}

async fn seed_record(
    store: &super::super::PostgresStore,
    f: &crate::approval_contract::fixture::Fixture,
    clock: &ManualClock,
) -> crate::approval::ApprovalRecord {
    let record = crate::approval::ApprovalRecord::new(
        f.batch.clone(),
        f.run.session_id.clone(),
        f.run.id.clone(),
        store
            .get_session(&f.run.session_id)
            .await
            .unwrap()
            .unwrap()
            .workspace_id,
        clock.now(),
    )
    .unwrap();
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    record
}

async fn check_scope(
    store: &super::super::PostgresStore,
    f: &crate::approval_contract::fixture::Fixture,
) {
    assert_eq!(
        store
            .approval_snapshot(&f.run.session_id, &f.run.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .approval_snapshot(&crabber_core::SessionId::new(), &f.run.id)
            .await
            .unwrap_err(),
        Error::NotFound
    );
    let event = store
        .list_events(&f.run.session_id, None, 1000)
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.kind == crabber_core::EventKind::ExecutionAttemptChanged)
        .unwrap();
    let native_key = crabber_core::ControlId(event.correlation.unwrap());
    assert_eq!(
        store
            .lookup_approval(&f.run.session_id, &native_key)
            .await
            .unwrap_err(),
        Error::SemanticConflict
    );
    assert_eq!(
        store
            .lookup_approval(&crabber_core::SessionId::new(), &native_key)
            .await
            .unwrap(),
        None
    );
}

async fn check_answer(
    store: &super::super::PostgresStore,
    f: &crate::approval_contract::fixture::Fixture,
) {
    let answer =
        crate::approval_contract::fixture::answer(store, f, 0, crate::ApprovalReply::Grant).await;
    let revision = store.session_revision(&f.run.session_id).await.unwrap();
    let before = store
        .list_events(&f.run.session_id, None, 1000)
        .await
        .unwrap();
    store
        .approval_fault
        .store(1, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        store.answer_approval(answer.clone()).await.unwrap_err(),
        Error::UnknownStoreOutcome
    );
    assert_eq!(
        store
            .lookup_approval(&f.run.session_id, &answer.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store.session_revision(&f.run.session_id).await.unwrap(),
        revision
    );
    assert_eq!(
        store
            .list_events(&f.run.session_id, None, 1000)
            .await
            .unwrap(),
        before
    );
    store
        .approval_fault
        .store(2, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        store.answer_approval(answer.clone()).await.unwrap_err(),
        Error::UnknownStoreOutcome
    );
    let receipt = store
        .lookup_approval(&f.run.session_id, &answer.id)
        .await
        .unwrap()
        .unwrap();
    // Even while the lost-ACK fault remains enabled, replay must only read metadata.

    assert_eq!(
        store
            .lookup_approval(&f.run.session_id, &answer.id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        store.answer_approval(answer.clone()).await.unwrap(),
        receipt
    );
    let mut changed = answer;
    changed.reply = crate::ApprovalReply::Deny;
    assert_eq!(
        store.answer_approval(changed).await.unwrap_err(),
        Error::SemanticConflict
    );
    assert_eq!(
        store
            .approval_snapshot(&f.run.session_id, &f.run.id)
            .await
            .unwrap()
            .unwrap()
            .interactions[0]
            .state,
        crabber_core::InteractionState::Answered
    );
    store
        .approval_fault
        .store(0, std::sync::atomic::Ordering::SeqCst);
    let reserve = crate::AttemptMutation {
        id: crabber_core::ControlId::new(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        command: crate::AttemptCommand::Reserve {
            target: f.batch.next_attempt.clone(),
        },
    };
    let fence = store
        .claim_expired_run(&f.run.id, "raw-bypass")
        .await
        .unwrap();
    let execution = store.execution(fence).await.unwrap();
    assert_eq!(
        execution.mutate_attempt(reserve, None).await.unwrap_err(),
        crate::AttemptError::Busy
    );
}

#[tokio::test]
async fn recovery_rejects_coherently_changed_consumed_answer() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::super::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        let p = crate::approval_contract::recovery::prepare(&store, &clock).await;
        clock.set(time::OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(31));
        let data: String = sqlx::query_scalar("SELECT data_json FROM continuations WHERE id=$1")
            .bind(&p.original.continuation.id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
        for change in 0..2 {
            let mut record: crate::approval::ApprovalRecord = serde_json::from_str(&data).unwrap();
            if change == 0 {
                record.questions[0].answer.as_mut().unwrap().reply = crate::ApprovalReply::Deny;
                record.questions[0].view.state = crabber_core::InteractionState::Denied;
            } else {
                record.batch.calls[2].decision = crate::CallApproval::Deny;
            }
            record
                .validate(&p.original.session_id, &p.original.run_id)
                .unwrap();
            sqlx::query("UPDATE continuations SET data_json=$2 WHERE id=$1")
                .bind(&p.original.continuation.id.0)
                .bind(serde_json::to_string(&record).unwrap())
                .execute(&store.pool)
                .await
                .unwrap();
            let before = crate::approval_contract::origin::evidence(&store, &p).await;
            assert_eq!(
                store
                    .reclaim_approval_continuation(p.request.clone())
                    .await
                    .unwrap_err(),
                Error::UnknownStoreOutcome
            );
            assert_eq!(
                crate::approval_contract::origin::evidence(&store, &p).await,
                before
            );
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn approval_origin_rejects_coherent_deadline_and_source_changes() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::super::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        crate::approval_contract::origin::run(&store, &clock, |id, case| {
            let pool = store.pool.clone();
            async move {
                let data: String =
                    sqlx::query_scalar("SELECT data_json FROM continuations WHERE id=$1")
                        .bind(&id.0)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                let mut record: crate::approval::ApprovalRecord =
                    serde_json::from_str(&data).unwrap();
                let fingerprint = record
                    .batch
                    .fingerprint(&record.session_id, &record.run_id)
                    .unwrap();
                crate::approval_contract::origin::corrupt(&mut record, case);
                record.validate(&record.session_id, &record.run_id).unwrap();
                assert_eq!(
                    record
                        .batch
                        .fingerprint(&record.session_id, &record.run_id)
                        .unwrap(),
                    fingerprint
                );
                sqlx::query("UPDATE continuations SET data_json=$2 WHERE id=$1")
                    .bind(&record.batch.continuation.id.0)
                    .bind(serde_json::to_string(&record).unwrap())
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        })
        .await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn original_approval_terminal_source_binds_request_and_actual_settlement() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        crate::approval::terminal::contract::run(&store, &clock).await;
    })
    .await
    .unwrap();
}
