#[tokio::test]
async fn sqlite_phase_contract() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    crate::phase_contract::run_contract(|clock| store.with_clock(clock)).await;
}
#[tokio::test]
async fn phase_fault_contract() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let store = store.with_clock(clock);
    crate::phase_contract::run_fault_contract(
        &store,
        |point: crate::phase_contract::FaultPoint| {
            store
                .phase_fault
                .store(point as u8, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .await;
}

#[tokio::test]
async fn typed_output_and_receipts_are_independent_of_private_authority() {
    use crate::{Store, phase_contract::storage};
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let store = store.with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let fixture = storage::completed(&store).await;
    let outcome: String = sqlx::query_scalar("SELECT outcome_json FROM tool_phases WHERE id=$1")
        .bind(&fixture.receipt.phase.target.id.0)
        .fetch_one(&store.writer)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&outcome).unwrap();
    assert_eq!(
        serde_json::from_value::<crabber_core::ToolOutput>(value["output"].clone()).unwrap(),
        fixture.output
    );
    sqlx::query("UPDATE tool_phases SET claim_token=$2 WHERE id=$1")
        .bind(&fixture.receipt.phase.target.id.0)
        .bind("x".repeat(513))
        .execute(&store.writer)
        .await
        .unwrap();
    assert_eq!(
        store
            .tool_phase(&fixture.admitted.run.id, &fixture.receipt.phase.target.id)
            .await
            .unwrap()
            .phase,
        fixture.receipt.phase
    );
    storage::isolated_receipt(&store, &fixture).await;
    let mut corrupt = fixture.receipt.clone();
    corrupt.phase.state = crate::PhaseState::Failed;
    sqlx::query(
        "UPDATE operation_receipts SET data_json=$3 WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&fixture.admitted.session.id.0)
    .bind(&fixture.request.id.0)
    .bind(serde_json::to_string(&corrupt).unwrap())
    .execute(&store.writer)
    .await
    .unwrap();
    storage::reject_corrupt_receipt(&store, &fixture).await;
}

#[tokio::test]
async fn phase_page_rejects_corrupt_sentinel_instead_of_omitting_it() {
    use crate::Store;
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let store = store.with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let (mut request, sentinel) = crate::phase_contract::controls::pagination(&store).await;
    request.limit = 1;
    let original = store.list_tool_phases(request.clone()).await.unwrap();
    assert_eq!(original.phases.len(), 1);
    assert!(original.next_after.is_some());
    sqlx::query("UPDATE tool_phases SET outcome_json=$2 WHERE id=$1")
        .bind(&sentinel.0)
        .bind("{}")
        .execute(&store.writer)
        .await
        .unwrap();
    assert_eq!(
        store.list_tool_phases(request).await.unwrap_err(),
        crate::PhaseError::UnknownStoreOutcome
    );
}
