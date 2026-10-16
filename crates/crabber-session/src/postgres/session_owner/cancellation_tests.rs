//! Private current state cannot erase or replace immutable cancellation evidence.
use super::*;
use crate::public_session::v2::proof::Failure;
use crate::{Store, session_owner::cancellation_contract as contract};
pub(super) async fn corruption(store: &PostgresStore) {
    for case in 0..9 {
        let (owner, authority, request, observation) = contract::source_fixture(store).await;
        let session = &owner.binding.session_id;
        corrupt(store, &owner, &request, &observation, case).await;
        let mut tx = store.pool.begin().await.unwrap();
        let mut safe = crate::public_session::budget::Budget::new(
            65536,
            crate::PublicSessionLimit::HeaderBytes,
        );
        let mut proof = crate::public_session::v2::proof::Proof::bounded(65536);
        let result = super::public::registered(&mut tx, session, 1, &mut safe, &mut proof).await;
        if matches!(case, 5 | 6 | 8) {
            assert!(
                matches!(
                    result,
                    Err(Failure::Limited(crate::PublicSessionLimitV2::ProofBytes))
                ),
                "case{case}"
            );
        } else {
            assert!(
                matches!(
                    result,
                    Err(Failure::Error(
                        crate::PublicSessionError::UnknownStoreOutcome
                    ))
                ),
                "case{case}"
            );
        }
        tx.rollback().await.unwrap();
        assert_eq!(
            store
                .get_session_owner(session, &owner.target.id)
                .await
                .unwrap_err(),
            Error::UnknownStoreOutcome,
            "case {case}"
        );
        assert_eq!(
            store
                .pending_session_owner_cancel(&authority)
                .await
                .unwrap_err(),
            Error::UnknownStoreOutcome,
            "case {case}"
        );
        let mut fresh = request.clone();
        fresh.id = SessionOwnerOperationId::new();
        assert_eq!(
            store
                .request_session_owner_cancel(fresh.clone())
                .await
                .unwrap_err(),
            Error::UnknownStoreOutcome,
            "case {case}"
        );
        assert!(
            store
                .lookup_session_owner(session, &fresh.id)
                .await
                .unwrap()
                .is_none()
        );
        if !matches!(case, 3 | 6 | 7) {
            assert!(matches!(
                store.request_session_owner_cancel(request).await.unwrap(),
                SessionOwnerOutcome::Replayed(_)
            ));
        }
    }
}

async fn corrupt(
    store: &PostgresStore,
    owner: &SessionOwnerSnapshot,
    request: &crate::session_owner::RequestSessionOwnerCancel,
    observation: &crate::session_owner::ObserveSessionOwnerCancel,
    case: u8,
) {
    let session = &owner.binding.session_id;
    let text: String = sqlx::query_scalar(
        "SELECT data_json FROM session_owner_current WHERE session_id=$1 AND owner_id=$2",
    )
    .bind(&session.0)
    .bind(&owner.target.id.0)
    .fetch_one(&store.pool)
    .await
    .unwrap();
    let mut current: serde_json::Value = serde_json::from_str(&text).unwrap();
    match case {
        0 => {
            current.as_object_mut().unwrap().remove("cancellation");
            current["snapshot"]
                .as_object_mut()
                .unwrap()
                .remove("cancellation");
        }
        1 => {
            current["cancellation"]["request"]["claim"]["authority_claim"] = "replacement".into();
        }
        2 => {
            current["cancellation"]["observed"] = serde_json::Value::Null;
            current["snapshot"]["cancellation"]["observed"] = serde_json::Value::Null;
        }
        3 | 4 => {
            sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
                .bind(&session.0)
                .bind(if case == 3 {
                    &request.id.0
                } else {
                    &observation.id.0
                })
                .execute(&store.pool)
                .await
                .unwrap();
        }
        5 => {
            current["owner"] = "x".repeat(1_048_576).into();
        }
        6 => {
            sqlx::query("UPDATE session_owner_audits SET data_json=$3 WHERE session_id=$1 AND operation_id=$2").bind(&session.0).bind(&request.id.0).bind("x".repeat(1_048_576)).execute(&store.pool).await.unwrap();
        }
        7 => {
            sqlx::query("UPDATE session_owner_audits SET operation='Renew' WHERE session_id=$1 AND operation_id=$2").bind(&session.0).bind(&request.id.0).execute(&store.pool).await.unwrap();
        }
        8 => {
            sqlx::query("UPDATE session_owner_audits SET data_json=$3 WHERE session_id=$1 AND operation_id=$2").bind(&session.0).bind(&observation.id.0).bind("x".repeat(1_048_576)).execute(&store.pool).await.unwrap();
        }
        _ => unreachable!(),
    }
    if matches!(case, 0 | 1 | 2 | 5) {
        sqlx::query(
            "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&session.0)
        .bind(&owner.target.id.0)
        .bind(serde_json::to_string(&current).unwrap())
        .execute(&store.pool)
        .await
        .unwrap();
    }
}
