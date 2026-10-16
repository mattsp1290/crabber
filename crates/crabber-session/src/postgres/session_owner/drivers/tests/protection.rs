//! Actual source loss, restored cancellation, and bounded selected carriers.
use super::super::*;
use crate::session_owner::owned_scope::{
    OwnedMainDisposition, OwnedScopeParentAuthority, OwnedTaskRegistration,
};
use crate::{
    Store,
    session_owner::{RequestSessionOwnerCancel, SessionOwnerResolver},
};
use sqlx::Row;
pub(super) async fn run(store: &PostgresStore) {
    for activate in [false, true] {
        erased_cancel(store, activate).await;
        oversized(store, activate).await;
    }
    erased_source(store).await;
    mutated_current_run(store).await;
    missing_first_issuer(store).await;
    altered_admission_receipt(store).await;
}
async fn witness(
    store: &PostgresStore,
    request: &RegisterExecutionDriver,
) -> crate::session_owner::preparation::admission::Witness {
    let raw: String =
        sqlx::query_scalar("SELECT data_json FROM preparation_admission_witnesses WHERE run_id=$1")
            .bind(&request.fence.run_id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    serde_json::from_str(&raw).unwrap()
}
async fn grant(
    store: &PostgresStore,
    request: &RegisterExecutionDriver,
    activate: bool,
) -> Option<Box<ExecutionDriverGrant>> {
    if !activate {
        return None;
    }
    let ExecutionDriverOutcome::Applied { grant, .. } = store
        .register_execution_driver(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh");
    };
    Some(grant)
}
async fn refuse(
    store: &PostgresStore,
    request: RegisterExecutionDriver,
    grant: Option<Box<ExecutionDriverGrant>>,
    stale: bool,
) {
    if let Some(grant) = grant {
        let mut task = OwnedTaskRegistration::register(grant.scope_source().unwrap(), || async {
            panic!("refused driver polled");
        })
        .unwrap();
        let result = store
            .activate_owned_scope(
                task.activation().clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::ExecutionDriver(grant)),
            )
            .await;
        assert!(
            matches!(result, Err(Error::UnknownStoreOutcome))
                || stale && matches!(result, Err(Error::StaleOwner)),
            "unexpected refusal: {:?}",
            result.as_ref().err()
        );
        assert_eq!(
            task.join().await.unwrap().disposition(),
            OwnedMainDisposition::NoDispatch
        );
    } else {
        let result = store.register_execution_driver(request).await;
        assert!(
            matches!(result, Err(Error::UnknownStoreOutcome))
                || stale && matches!(result, Err(Error::StaleOwner)),
            "unexpected refusal: {:?}",
            result.as_ref().err()
        );
    }
}
async fn erased_cancel(store: &PostgresStore, activate: bool) {
    let request = crate::driver_contract::request(store).await;
    let grant = grant(store, &request, activate).await;
    let witness = witness(store, &request).await;
    let begin = witness.audit_ids().unwrap()[4].clone();
    let mut tx = store.pool.begin().await.unwrap();
    let audit = reads::audit(&mut tx, request.context.session_id(), &begin)
        .await
        .unwrap()
        .unwrap();
    tx.rollback().await.unwrap();
    let child = audit.receipt.owner.target;
    let current: String = sqlx::query_scalar(
        "SELECT data_json FROM session_owner_current WHERE session_id=$1 AND owner_id=$2",
    )
    .bind(&request.context.session_id().0)
    .bind(&child.id.0)
    .fetch_one(&store.pool)
    .await
    .unwrap();
    let cancel = crabber_core::SessionOwnerOperationId::new();
    store
        .request_session_owner_cancel(RequestSessionOwnerCancel {
            id: cancel.clone(),
            session_id: request.context.session_id().clone(),
            target: child.clone(),
            expected_revision: Some(
                store
                    .session_revision(request.context.session_id())
                    .await
                    .unwrap(),
            ),
            claim: SessionOwnerResolver {
                resolver_id: "test-host".into(),
                authority_claim: "stop actual child".into(),
            },
        })
        .await
        .unwrap();
    // Keep actual public R after cancellation. Restore only mutable current and
    // remove its last immutable audit to test the strict independent R fence.
    let mut tx = store.pool.begin().await.unwrap();
    sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
        .bind(&request.context.session_id().0)
        .bind(&cancel.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
    )
    .bind(&request.context.session_id().0)
    .bind(&child.id.0)
    .bind(current)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    refuse(store, request, grant, true).await;
}
async fn oversized(store: &PostgresStore, activate: bool) {
    let request = crate::driver_contract::request(store).await;
    let grant = grant(store, &request, activate).await;
    let witness = witness(store, &request).await;
    let begin = witness.audit_ids().unwrap()[4].clone();
    let mut tx = store.pool.begin().await.unwrap();
    let audit = reads::audit(&mut tx, request.context.session_id(), &begin)
        .await
        .unwrap()
        .unwrap();
    tx.rollback().await.unwrap();
    let id = audit.receipt.owner.target.id;
    let current: String = sqlx::query_scalar(
        "SELECT data_json FROM session_owner_current WHERE session_id=$1 AND owner_id=$2",
    )
    .bind(&request.context.session_id().0)
    .bind(&id.0)
    .fetch_one(&store.pool)
    .await
    .unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&current).unwrap();
    value["owner"] =
        serde_json::Value::String("x".repeat(crate::deletion::coverage::MAX_PROOF_BYTES + 1));
    sqlx::query(
        "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
    )
    .bind(&request.context.session_id().0)
    .bind(&id.0)
    .bind(value.to_string())
    .execute(&store.pool)
    .await
    .unwrap();
    refuse(store, request.clone(), grant, false).await;
    sqlx::query(
        "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
    )
    .bind(&request.context.session_id().0)
    .bind(&id.0)
    .bind(current)
    .execute(&store.pool)
    .await
    .unwrap();
}
async fn erased_source(store: &PostgresStore) {
    let request = crate::driver_contract::request(store).await;
    let grant = grant(store, &request, true).await;
    let witness = witness(store, &request).await;
    let begin = witness.audit_ids().unwrap()[1].clone();
    let row=sqlx::query("SELECT ordinal,owner_id,sequence,operation,state,data_json FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2").bind(&request.context.session_id().0).bind(&begin.0).fetch_one(&store.pool).await.unwrap();
    sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
        .bind(&request.context.session_id().0)
        .bind(&begin.0)
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(matches!(
        store.register_execution_driver(request.clone()).await,
        Err(Error::UnknownStoreOutcome)
    ));
    refuse(store, request.clone(), grant, false).await;
    sqlx::query("INSERT INTO session_owner_audits(ordinal,session_id,operation_id,owner_id,sequence,operation,state,data_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(row.get::<i64,_>("ordinal")).bind(&request.context.session_id().0).bind(&begin.0).bind(row.get::<String,_>("owner_id")).bind(row.get::<i64,_>("sequence")).bind(row.get::<String,_>("operation")).bind(row.get::<String,_>("state")).bind(row.get::<String,_>("data_json")).execute(&store.pool).await.unwrap();
}

async fn mutated_current_run(store: &PostgresStore) {
    let request = crate::driver_contract::request(store).await;
    let grant = grant(store, &request, true).await;
    sqlx::query("UPDATE runs SET claim_token=$2,data=jsonb_set(data,'{claim_token}',to_jsonb(CAST($2 AS TEXT))) WHERE id=$1").bind(&request.fence.run_id.0).bind("mutated-current-fence").execute(&store.pool).await.unwrap();
    refuse(store, request.clone(), grant, false).await;
    sqlx::query("UPDATE runs SET claim_token=$2,data=jsonb_set(data,'{claim_token}',to_jsonb(CAST($2 AS TEXT))) WHERE id=$1").bind(&request.fence.run_id.0).bind(&request.fence.claim_token).execute(&store.pool).await.unwrap();
}
async fn missing_first_issuer(store: &PostgresStore) {
    let request = crate::driver_contract::request(store).await;
    let grant = grant(store, &request, true).await;
    let raw:String=sqlx::query_scalar("SELECT data_json FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND sequence=1").bind(&request.context.session_id().0).bind(&request.fence.run_id.0).fetch_one(&store.pool).await.unwrap();
    sqlx::query(
        "DELETE FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND sequence=1",
    )
    .bind(&request.context.session_id().0)
    .bind(&request.fence.run_id.0)
    .execute(&store.pool)
    .await
    .unwrap();
    assert!(matches!(
        store.register_execution_driver(request.clone()).await,
        Err(Error::UnknownStoreOutcome)
    ));
    refuse(store, request.clone(), grant, false).await;
    sqlx::query(
        "INSERT INTO native_run_issuances(session_id,run_id,sequence,data_json) VALUES($1,$2,1,$3)",
    )
    .bind(&request.context.session_id().0)
    .bind(&request.fence.run_id.0)
    .bind(raw)
    .execute(&store.pool)
    .await
    .unwrap();
}
async fn altered_admission_receipt(store: &PostgresStore) {
    let request = crate::driver_contract::request(store).await;
    let grant = grant(store, &request, true).await;
    let raw: String = sqlx::query_scalar(
        "SELECT data::text FROM admission_receipts WHERE session_id=$1 AND run_id=$2",
    )
    .bind(&request.context.session_id().0)
    .bind(&request.fence.run_id.0)
    .fetch_one(&store.pool)
    .await
    .unwrap();
    // FK protection prevents erasing this original receipt while execution exists.
    assert!(
        sqlx::query("DELETE FROM admission_receipts WHERE session_id=$1 AND run_id=$2")
            .bind(&request.context.session_id().0)
            .bind(&request.fence.run_id.0)
            .execute(&store.pool)
            .await
            .is_err()
    );
    let mut altered: serde_json::Value = serde_json::from_str(&raw).unwrap();
    altered["semantic_digest"] = serde_json::Value::String("b".repeat(64));
    sqlx::query("UPDATE admission_receipts SET data=$3::jsonb WHERE session_id=$1 AND run_id=$2")
        .bind(&request.context.session_id().0)
        .bind(&request.fence.run_id.0)
        .bind(altered.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(matches!(
        store.register_execution_driver(request.clone()).await,
        Err(Error::UnknownStoreOutcome)
    ));
    refuse(store, request.clone(), grant, false).await;
    sqlx::query("UPDATE admission_receipts SET data=$3::jsonb WHERE session_id=$1 AND run_id=$2")
        .bind(&request.context.session_id().0)
        .bind(&request.fence.run_id.0)
        .bind(raw)
        .execute(&store.pool)
        .await
        .unwrap();
}

#[path = "protection/unkeyed.rs"]
mod unkeyed;
pub(super) async fn unkeyed_contract(store: &PostgresStore) {
    unkeyed::run(store).await;
}
