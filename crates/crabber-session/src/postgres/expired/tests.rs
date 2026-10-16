use crate::session_owner::preparation::{
    ExpiredRunOrigin, ExpiredRunOutcome, PreparationCompletion,
};
use crate::{PostgresStore, Store};
use crabber_core::ManualClock;
use std::sync::{Arc, atomic::Ordering};
#[tokio::test]
async fn expired_preparation_faults_and_sources() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    crate::run_control_contract::expired::faults(&store, &clock, |point| {
        store.binding_fault.store(point, Ordering::SeqCst);
    })
    .await;
    crate::run_control_contract::expired::unkeyed::faults(&store, &clock, |point| {
        store.binding_fault.store(point, Ordering::SeqCst);
    })
    .await;
    crate::run_control_contract::expired::unkeyed::lost_admission_ack(&store, &clock, |point| {
        store.binding_fault.store(point, Ordering::SeqCst);
    })
    .await;
    verify_begin_guard(&store, &clock).await;
    let pending = crate::run_control_contract::expired::pending(&store, &clock).await;
    let source = &pending.source;
    let PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Claimed { receipt, .. }) =
        Box::pin(store.complete_preparation(pending.joined))
            .await
            .unwrap()
    else {
        panic!("fresh claim")
    };
    let session = &source.request().session_id;
    let run = &source.request().expected_fence.run_id;
    let original = store.get_run(run).await.unwrap().unwrap();
    let mut oversized = original.clone();
    oversized.checkpoint =
        Some(serde_json::json!({"unused":"x".repeat(crate::MAX_RUN_BOUNDARY_BYTES+32768)}));
    sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
        .bind(&run.0)
        .bind(sqlx::types::Json(serde_json::to_value(&oversized).unwrap()))
        .execute(&store.pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .lookup_expired_run_preparation(session, run, source.expected_issuance())
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    let mut request = source.request().clone();
    request
        .expected_fence
        .claim_token
        .clone_from(&original.claim_token);
    clock.set(original.lease_until);
    assert!(
        store
            .inspect_expired_run_preparation(request)
            .await
            .is_err()
    );
    sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
        .bind(&run.0)
        .bind(sqlx::types::Json(serde_json::to_value(&original).unwrap()))
        .execute(&store.pool)
        .await
        .unwrap();
    verify_original_sources(&store, source, receipt, &url).await;
}
async fn verify_original_sources(
    store: &PostgresStore,
    source: &crate::session_owner::preparation::ExpiredRunPreparation,
    receipt: crate::session_owner::preparation::ExpiredRunReceipt,
    url: &str,
) {
    let session = &source.request().session_id;
    let run = &source.request().expected_fence.run_id;
    let ExpiredRunOrigin::Suspended(id) = &source.target().origin else {
        panic!("suspended source")
    };
    let mut tx = store.session_transaction(session).await.unwrap();
    let saved = crate::postgres::attempt::records::receipt(&mut tx, session, id)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2")
        .bind(&session.0)
        .bind(&id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        store
            .lookup_expired_run_preparation(session, run, source.expected_issuance())
            .await
            .is_err()
    );
    sqlx::query("INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'attempt',$3,$4,$5)").bind(&session.0).bind(&id.0).bind(saved.semantic_digest.as_str()).bind(i64::try_from(saved.event_cursor.0).unwrap()).bind(serde_json::to_string(&saved).unwrap()).execute(&store.pool).await.unwrap();
    let sequence = i64::try_from(source.expected_issuance() + 1).unwrap();
    let original_issuer:String = sqlx::query_scalar("SELECT data_json FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND sequence=$3").bind(&session.0).bind(&run.0).bind(sequence).fetch_one(&store.pool).await.unwrap();
    let mut changed: crate::native_parent::RunIssuance =
        serde_json::from_str(&original_issuer).unwrap();
    changed
        .completion_source
        .as_mut()
        .unwrap()
        .native_semantic_digest = crabber_core::InputFingerprint::new("e".repeat(64)).unwrap();
    write_issuer(
        store,
        session,
        run,
        sequence,
        &serde_json::to_string(&changed).unwrap(),
    )
    .await;
    assert!(
        store
            .lookup_expired_run_preparation(session, run, source.expected_issuance())
            .await
            .is_err()
    );
    write_issuer(
        store,
        session,
        run,
        sequence,
        &format!("{original_issuer}{}", " ".repeat(2048)),
    )
    .await;
    assert!(
        store
            .lookup_expired_run_preparation(session, run, source.expected_issuance())
            .await
            .is_err()
    );
    write_issuer(store, session, run, sequence, &original_issuer).await;
    // A reopened controller holds metadata only, no physical join/grant proof.
    let cold = PostgresStore::connect(url).await.unwrap();
    assert_eq!(
        cold.lookup_expired_run_preparation(session, run, source.expected_issuance())
            .await
            .unwrap(),
        Some(receipt)
    );
}
async fn write_issuer(
    store: &PostgresStore,
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    sequence: i64,
    data: &str,
) {
    sqlx::query("UPDATE native_run_issuances SET data_json=$4 WHERE session_id=$1 AND run_id=$2 AND sequence=$3").bind(&session.0).bind(&run.0).bind(sequence).bind(data).execute(&store.pool).await.unwrap();
}

#[tokio::test]
async fn expired_preparation_process_boundaries() {
    use crate::session_owner::preparation::expired::process;
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    if std::env::var_os("CRABBER_EXPIRED_CHILD").is_some() {
        process::child(&store, &clock).await;
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    for stage in ["before", "after"] {
        let key = process::kill(
            "postgres::expired::tests::expired_preparation_process_boundaries",
            stage,
            None,
            directory.path(),
        );
        let cold = PostgresStore::connect(&url).await.unwrap();
        process::verify(&cold, key, stage == "after").await;
        let unkeyed = tempfile::tempdir().unwrap();
        let key = process::kill_unkeyed(
            "postgres::expired::tests::expired_preparation_process_boundaries",
            stage,
            None,
            unkeyed.path(),
        );
        process::verify(&cold, key, stage == "after").await;
    }
}

async fn verify_begin_guard(store: &PostgresStore, clock: &ManualClock) {
    let (source, reservation, authority) =
        crate::run_control_contract::expired::reserved(store, clock).await;
    let request = crate::run_control_contract::expired::begin_request(&reservation, &authority);
    let run_id = &source.request().expected_fence.run_id;
    let original = store.get_run(run_id).await.unwrap().unwrap();
    let mut changed = original.clone();
    changed.checkpoint =
        Some(serde_json::json!({"unused":"x".repeat(crate::MAX_RUN_BOUNDARY_BYTES+32768)}));
    sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
        .bind(&run_id.0)
        .bind(sqlx::types::Json(serde_json::to_value(&changed).unwrap()))
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(
        store
            .mutate_session_owner(request.clone(), Some(authority.clone()))
            .await
            .is_err()
    );
    sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
        .bind(&run_id.0)
        .bind(sqlx::types::Json(serde_json::to_value(&original).unwrap()))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
        .bind(&request.session_id.0)
        .bind(&reservation.owner.id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(
        store
            .mutate_session_owner(request, Some(authority))
            .await
            .is_err()
    );
    assert!(store.reserve_preparation(reservation).await.is_err());
}
