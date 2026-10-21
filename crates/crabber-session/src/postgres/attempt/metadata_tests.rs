use crate::{AttemptError, AttemptMutationOutcome, Store};
#[tokio::test]
async fn metadata_replay_is_independent_of_private_record_and_rejects_tampering() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

    super::super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::super::PostgresStore::connect(&url).await.unwrap();
    let admitted =
        crate::public_state_contract::admit(&store, time::OffsetDateTime::now_utc()).await;
    let execution = store.execution(admitted.fence).await.unwrap();
    let request = crate::attempt_contract::reserve(
        store.session_revision(&admitted.session.id).await.unwrap(),
    );
    let AttemptMutationOutcome::Applied { receipt, .. } = execution
        .mutate_attempt(request.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh reserve");
    };
    let token: String =
        sqlx::query_scalar("SELECT claim_token FROM execution_attempts WHERE id=$1")
            .bind(&receipt.attempt.target.id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    sqlx::query("UPDATE execution_attempts SET claim_token=$2 WHERE id=$1")
        .bind(&receipt.attempt.target.id.0)
        .bind("x".repeat(513))
        .execute(&store.pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .execution_attempt(&admitted.run.id, &receipt.attempt.target.id)
            .await
            .unwrap_err(),
        AttemptError::UnknownStoreOutcome
    );
    assert_eq!(
        store
            .lookup_attempt_mutation(&admitted.session.id, &request.id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert!(
        matches!(execution.mutate_attempt(request.clone(),None).await.unwrap(),AttemptMutationOutcome::Replayed(r) if r == receipt)
    );
    sqlx::query("UPDATE execution_attempts SET claim_token=$2 WHERE id=$1")
        .bind(&receipt.attempt.target.id.0)
        .bind(token)
        .execute(&store.pool)
        .await
        .unwrap();
    let mut corrupt = receipt.clone();
    corrupt.attempt.state = crate::AttemptState::Started;
    sqlx::query(
        "UPDATE operation_receipts SET data_json=$3 WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&admitted.session.id.0)
    .bind(&request.id.0)
    .bind(serde_json::to_string(&corrupt).unwrap())
    .execute(&store.pool)
    .await
    .unwrap();
    assert_eq!(
        store
            .lookup_attempt_mutation(&admitted.session.id, &request.id)
            .await
            .unwrap_err(),
        AttemptError::UnknownStoreOutcome
    );
    assert_eq!(
        execution
            .mutate_attempt(request.clone(), None)
            .await
            .unwrap_err(),
        AttemptError::UnknownStoreOutcome
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        receipt.revision
    );
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 100)
            .await
            .unwrap()
            .len(),
        1
    );

    }).await.unwrap();
}
