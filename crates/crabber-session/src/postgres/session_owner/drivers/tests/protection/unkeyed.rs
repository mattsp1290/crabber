//! Unkeyed original binding and revision fences survive actual storage corruption.
use super::*;
pub(super) async fn run(store: &PostgresStore) {
    for activate in [false, true] {
        coherent_binding(store, activate).await;
        erased_actual_attempt(store, activate).await;
    }
    metadata_without_current_binding(store).await;
}
async fn coherent_binding(store: &PostgresStore, activate: bool) {
    let request = crate::driver_contract::request_unkeyed(store).await;
    let grant = grant(store, &request, activate).await;
    let raw: String =
        sqlx::query_scalar("SELECT data_json FROM run_execution_bindings WHERE run_id=$1")
            .bind(&request.fence.run_id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    let mut binding: crate::RetainedExecutionBinding = serde_json::from_str(&raw).unwrap();
    let original = binding.capsule.plan_fingerprint.clone();
    binding.capsule.plan_fingerprint = crabber_core::InputFingerprint::new("e".repeat(64)).unwrap();
    binding.capsule.input_fingerprint = binding.capsule.computed_input_fingerprint().unwrap();
    sqlx::query("UPDATE run_execution_bindings SET data_json=$2 WHERE run_id=$1")
        .bind(&request.fence.run_id.0)
        .bind(serde_json::to_string(&binding).unwrap())
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET data=jsonb_set(data,'{plan_fingerprint}',to_jsonb(CAST($2 AS TEXT))) WHERE id=$1").bind(&request.fence.run_id.0).bind(binding.capsule.plan_fingerprint.as_str())
        .execute(&store.pool).await.unwrap();
    // The mutated current binding is coherent; the original Join rejects it.
    assert!(
        store
            .execution_binding(request.context.session_id(), &request.fence.run_id)
            .await
            .unwrap()
            .is_some()
    );
    refuse(store, request.clone(), grant, false).await;
    sqlx::query("UPDATE run_execution_bindings SET data_json=$2 WHERE run_id=$1")
        .bind(&request.fence.run_id.0)
        .bind(raw)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET data=jsonb_set(data,'{plan_fingerprint}',to_jsonb(CAST($2 AS TEXT))) WHERE id=$1").bind(&request.fence.run_id.0).bind(original.as_str()).execute(&store.pool).await.unwrap();
}
async fn erased_actual_attempt(store: &PostgresStore, activate: bool) {
    let request = crate::driver_contract::request_unkeyed(store).await;
    let grant = grant(store, &request, activate).await;
    let revision = store
        .session_revision(request.context.session_id())
        .await
        .unwrap();
    let execution = store.execution(request.fence.clone()).await.unwrap();
    let mut reserve = crate::attempt_contract::reserve(revision);
    reserve.command = crate::AttemptCommand::ReserveBound {
        target: reserve.command.target().clone(),
    };
    let crate::AttemptMutationOutcome::Applied { receipt, authority } = execution
        .mutate_attempt(reserve.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh actual attempt");
    };
    let begin = crate::AttemptMutation {
        id: crabber_core::ControlId::new(),
        expected_revision: receipt.revision,
        command: crate::AttemptCommand::Begin {
            target: receipt.attempt.target.clone(),
        },
    };
    execution
        .mutate_attempt(begin.clone(), authority)
        .await
        .unwrap();
    sqlx::query("DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id IN ($2,$3)")
        .bind(&request.context.session_id().0)
        .bind(&reserve.id.0)
        .bind(&begin.id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM execution_attempts WHERE run_id=$1")
        .bind(&request.fence.run_id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(
        sqlx::query("DELETE FROM events WHERE session_id=$1 AND run_id=$2")
            .bind(&request.context.session_id().0)
            .bind(&request.fence.run_id.0)
            .execute(&store.pool)
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM native_parent_witnesses WHERE session_id=$1 AND run_id=$2")
        .bind(&request.context.session_id().0)
        .bind(&request.fence.run_id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM events WHERE session_id=$1 AND run_id=$2")
        .bind(&request.context.session_id().0)
        .bind(&request.fence.run_id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    refuse(store, request, grant, true).await;
}
async fn metadata_without_current_binding(store: &PostgresStore) {
    let request = crate::driver_contract::request_unkeyed(store).await;
    let grant = grant(store, &request, true).await;
    let keyed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM admission_executions WHERE run_id=$1")
            .bind(&request.fence.run_id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(keyed, 0);
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM admission_receipts WHERE run_id=$1")
            .bind(&request.fence.run_id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(receipts, 0);
    sqlx::query("DELETE FROM run_execution_bindings WHERE run_id=$1")
        .bind(&request.fence.run_id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(matches!(
        store
            .register_execution_driver(request.clone())
            .await
            .unwrap(),
        ExecutionDriverOutcome::Replayed(_)
    ));
    if let Some(grant) = grant {
        let mut task = OwnedTaskRegistration::register(grant.scope_source().unwrap(), || async {
            panic!("missing binding dispatched");
        })
        .unwrap();
        assert!(
            store
                .activate_owned_scope(
                    task.activation().clone(),
                    grant.authority().clone(),
                    Some(OwnedScopeParentAuthority::ExecutionDriver(grant))
                )
                .await
                .is_err()
        );
        assert_eq!(
            task.join().await.unwrap().disposition(),
            OwnedMainDisposition::NoDispatch
        );
    }
}
