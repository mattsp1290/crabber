//! Original immutable source audits remain unchanged during coherent corruption.
use super::*;
use crate::approval_contract::current::{self, Case, Target};
struct Saved {
    record: approval::ApprovalRecord,
    run: Run,
    barrier: bool,
    claim: Option<ApprovalReceipt>,
}
#[tokio::test]
async fn immutable_current_approval_sources_reject_coherent_private_changes() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let Some(url) = super::super::tests::test_url() else {
        return;
    };
    let _guard = super::super::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    for case in current::CASES {
        let saved = current::run(&store, &clock, case, |target| corrupt(&store, target)).await;
        let mut tx = store.pool.begin().await.unwrap();
        save_record(&mut tx, &saved.record).await;
        save_run(&mut tx, &saved.run).await;
        if saved.barrier {
            sqlx::query("INSERT INTO continuation_barriers(session_id,run_id,continuation_id) VALUES($1,$2,$3)").bind(&saved.record.session_id.0).bind(&saved.record.run_id.0).bind(&saved.record.batch.continuation.id.0).execute(&mut *tx).await.unwrap();
        }
        if let Some(claim) = saved.claim {
            save_receipt(&mut tx, &claim).await;
        }
        tx.commit().await.unwrap();
    }
}
async fn corrupt(store: &crate::PostgresStore, target: Target) -> Saved {
    let mut tx = store.pool.begin().await.unwrap();
    let data: String = sqlx::query_scalar("SELECT data_json FROM continuations WHERE id=$1")
        .bind(&target.id.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let record: approval::ApprovalRecord = serde_json::from_str(&data).unwrap();
    let data: String = sqlx::query_scalar("SELECT CAST(data AS TEXT) FROM runs WHERE id=$1")
        .bind(&record.run_id.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let run: Run = serde_json::from_str(&data).unwrap();
    let barrier: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE continuation_id=$1)",
    )
    .bind(&target.id.0)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let claim = if target.case == Case::ChangedClaimReceipt {
        let data: String = sqlx::query_scalar(
            "SELECT data_json FROM operation_receipts WHERE session_id=$1 AND operation_id=$2",
        )
        .bind(&record.session_id.0)
        .bind(&target.claim_id.unwrap().0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        let saved: ApprovalReceipt = serde_json::from_str(&data).unwrap();
        let mut changed = saved.clone();
        changed.semantic_digest = crabber_core::InputFingerprint::new("c".repeat(64)).unwrap();
        save_receipt(&mut tx, &changed).await;
        Some(saved)
    } else {
        None
    };
    let saved = Saved {
        record: record.clone(),
        run: run.clone(),
        barrier: false,
        claim,
    };
    let mut changed = record;
    current::corrupt(&mut changed, target.case);
    save_record(&mut tx, &changed).await;
    let erase = matches!(
        target.case,
        Case::ReleasedNoBarrier | Case::FakeTerminalCleanup
    );
    if erase {
        sqlx::query("DELETE FROM continuation_barriers WHERE continuation_id=$1")
            .bind(&target.id.0)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    if target.case == Case::FakeTerminalCleanup {
        let mut changed = run;
        changed.status = crabber_core::RunStatus::Interrupted;
        save_run(&mut tx, &changed).await;
    }
    tx.commit().await.unwrap();
    Saved {
        barrier: barrier && erase,
        ..saved
    }
}
async fn save_record(tx: &mut Transaction<'_, Postgres>, record: &approval::ApprovalRecord) {
    sqlx::query("UPDATE continuations SET state=$2,data_json=$3 WHERE id=$1")
        .bind(&record.batch.continuation.id.0)
        .bind(sql::enum_text(record.state).unwrap())
        .bind(serde_json::to_string(record).unwrap())
        .execute(&mut **tx)
        .await
        .unwrap();
}
async fn save_run(tx: &mut Transaction<'_, Postgres>, run: &Run) {
    sqlx::query("UPDATE runs SET status=$2,data=$3 WHERE id=$1")
        .bind(&run.id.0)
        .bind(sql::enum_text(run.status).unwrap())
        .bind(sqlx::types::Json(run))
        .execute(&mut **tx)
        .await
        .unwrap();
}
async fn save_receipt(tx: &mut Transaction<'_, Postgres>, receipt: &ApprovalReceipt) {
    sqlx::query("UPDATE operation_receipts SET semantic_digest=$3,data_json=$4 WHERE session_id=$1 AND operation_id=$2").bind(&receipt.session_id.0).bind(&receipt.operation_id.0).bind(receipt.semantic_digest.as_str()).bind(serde_json::to_string(receipt).unwrap()).execute(&mut **tx).await.unwrap();
}

#[tokio::test]
async fn current_approval_accepts_authentic_successor_and_parent_retirement() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let Some(url) = super::super::tests::test_url() else {
        return;
    };
    let _guard = super::super::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());

    crate::public_session::rounds::checked(&store, &clock, |prior, run| {
        let store = &store;
        async move {
            assert_eq!(run.status, crabber_core::RunStatus::Paused);
            let mut tx = store.pool.begin().await.unwrap();
            let (latest, _) = super::load(&mut tx, &run.session_id, &run.id)
                .await
                .unwrap()
                .unwrap();
            let previous = super::current::exact(&mut tx, &latest, &prior)
                .await
                .unwrap();
            assert_eq!(previous.state, crate::ApprovalContinuationState::Released);
            super::origin::verify(&mut tx, &previous).await.unwrap();
            super::current::verify(&mut tx, &run, &previous, false)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
    })
    .await;
    crate::approval_contract::current::retired(&store, &clock).await;
}

#[tokio::test]
async fn unrelated_nul_payload_does_not_break_immutable_source_selection() {
    use crate::Store;
    let Some(url) = super::super::tests::test_url() else {
        return;
    };
    let _guard = super::super::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let mut f = crate::approval_contract::fixture::staged(&store, &clock).await;
    f.execution
        .mutate_public_state(crate::PublicStateMutation {
            id: crabber_core::ControlId::new(),
            expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
            contents: crabber_core::PublicStateContents::new(
                serde_json::json!({"private":"nul\u{0}","nested":{"kind":"continuation_changed"}}),
                vec![],
            )
            .unwrap(),
        })
        .await
        .unwrap();
    f.batch.expected_revision = store.session_revision(&f.run.session_id).await.unwrap();
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner)
        .await
        .unwrap();
    assert!(
        store
            .approval_snapshot(&f.run.session_id, &f.run.id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        store
            .public_session_snapshot(crate::public_session::tests::request(f.run.session_id))
            .await
            .unwrap(),
        crate::PublicSessionOutcome::Page(_)
    ));
}
