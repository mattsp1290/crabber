use super::{test_support::close, tests::temp_store, *};
use crate::AdmissionExecutionError as Error;
use crate::*;
use crabber_core::*;
use time::OffsetDateTime;
async fn admission(
    store: &SqliteStore,
    now: OffsetDateTime,
) -> (
    crate::KeyedAdmitRequest,
    crate::AdmitOutcome,
    ClaimUnstartedAdmissionRequest,
) {
    let keyed = crate::admission_execution_contract::keyed(&SessionId::new(), now);
    let crate::KeyedAdmitOutcome::Started { receipt, admitted } =
        store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!()
    };
    let claim = ClaimUnstartedAdmissionRequest {
        session_id: receipt.session_id,
        key: keyed.options.key.clone(),
        expected_fence: admitted.fence.clone(),
        expected_owner: admitted.run.owner.clone(),
        fingerprint: receipt.fingerprint,
        behavior_fingerprint: keyed.options.behavior_fingerprint.clone(),
        semantic_digest: receipt.semantic_digest,
        capsule_digest: keyed.execution.as_ref().unwrap().digest().unwrap(),
        owner: "new owner".into(),
        lease: Duration::from_secs(30),
    };
    (keyed, *admitted, claim)
}

#[tokio::test]
async fn renew_claim_begin_and_abandon_serialize_without_duplicate_authority() {
    let (_initial, path, _directory) = temp_store().await;
    let now = OffsetDateTime::now_utc();
    let clock = Arc::new(ManualClock::new(now));
    let store = SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let (_, admitted, request) = admission(&store, now).await;
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let (renewed, denied) = tokio::join!(
        execution.renew_lease(now + time::Duration::seconds(60)),
        store.claim_unstarted_admission(request.clone())
    );
    renewed.unwrap();
    assert_eq!(denied.unwrap_err(), Error::LiveLease);
    clock.set(now + time::Duration::seconds(60));
    let abandonment = AbandonRequest {
        expected: admitted.fence,
        expected_owner: admitted.run.owner,
        authority: AbandonAuthority::ExpiredLease,
    };
    let (claim_result, abandon_result) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            store.claim_unstarted_admission(request),
            store.abandon_run(abandonment)
        )
    })
    .await
    .unwrap();
    match (claim_result, abandon_result) {
        (Ok(claimed), Err(crabber_core::AbandonError::StaleOwner)) => {
            store
                .execution(claimed.fence)
                .await
                .unwrap()
                .begin_admission_execution()
                .await
                .unwrap();
        }
        (Err(Error::StaleOwner | Error::AlreadyTerminal), Ok(_)) => (),
        other => panic!("unexpected race {other:?}"),
    }
    // Live administrative abandonment competes with one-shot begin. If begin
    // wins, abandonment still revokes it; if abandonment wins, begin is denied.
    clock.set(now);
    let (_, admitted, _) = admission(&store, now).await;
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let abandonment = AbandonRequest {
        expected: admitted.fence,
        expected_owner: admitted.run.owner,
        authority: AbandonAuthority::HostStoppedOwner,
    };
    let (begin, abandoned) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            execution.begin_admission_execution(),
            store.abandon_run(abandonment)
        )
    })
    .await
    .unwrap();
    abandoned.unwrap();
    assert!(matches!(
        begin,
        Ok(()) | Err(Error::StaleOwner | Error::AlreadyTerminal)
    ));
    assert!(
        store
            .get_run(&admitted.run.id)
            .await
            .unwrap()
            .unwrap()
            .status
            .is_terminal()
    );
    assert!(execution.begin_admission_execution().await.is_err());
    close(&store).await;
}
#[cfg(test)]
#[tokio::test]
async fn failed_begin_update_rolls_back_started_and_can_be_retried() {
    use crate::Store;
    use time::OffsetDateTime;
    let (_initial, path, _directory) = temp_store().await;
    let now = OffsetDateTime::now_utc();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(now));
    let store = SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let keyed = crate::admission_execution_contract::keyed(&crabber_core::SessionId::new(), now);
    let crate::KeyedAdmitOutcome::Started { admitted, receipt } =
        store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!()
    };
    sqlx::query(&format!("CREATE TRIGGER begin_rollback AFTER UPDATE ON admission_executions WHEN NEW.run_id='{}' BEGIN SELECT RAISE(ABORT,'injected'); END",receipt.run_id.0)).execute(&store.writer).await.unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    assert_eq!(
        execution.begin_admission_execution().await.unwrap_err(),
        Error::UnknownStoreFailure
    );
    assert_eq!(
        store
            .load_admission_execution(&receipt.session_id, &keyed.options.key)
            .await
            .unwrap()
            .unwrap()
            .state,
        AdmissionExecutionState::Unstarted
    );
    sqlx::query("DROP TRIGGER begin_rollback")
        .execute(&store.writer)
        .await
        .unwrap();
    // The failed caller never retries begin for authority. After expiry a fresh
    // claim can start the retained Unstarted turn with a different fence.
    clock.set(now + time::Duration::seconds(30));
    let claimed = store
        .claim_unstarted_admission(ClaimUnstartedAdmissionRequest {
            session_id: receipt.session_id.clone(),
            key: keyed.options.key,
            expected_fence: admitted.fence,
            expected_owner: admitted.run.owner,
            fingerprint: receipt.fingerprint,
            behavior_fingerprint: keyed.options.behavior_fingerprint,
            semantic_digest: receipt.semantic_digest,
            capsule_digest: keyed.execution.unwrap().digest().unwrap(),
            owner: "after rollback".into(),
            lease: std::time::Duration::from_secs(30),
        })
        .await
        .unwrap();
    let execution = store.execution(claimed.fence).await.unwrap();
    execution.begin_admission_execution().await.unwrap();
    assert_eq!(
        execution.begin_admission_execution().await.unwrap_err(),
        Error::AlreadyStarted
    );
    close(&store).await;
}
