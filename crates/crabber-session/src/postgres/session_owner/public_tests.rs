use super::*;
use crate::PostgresStore;
use crate::session_owner::*;
use crate::{PublicSessionError, PublicSessionLimit, Store};
use crabber_core::{SessionId, SessionOwnerOperationId};
async fn read(
    store: &PostgresStore,
    session: &SessionId,
    limit: u16,
    cap: usize,
) -> (Result<Vec<PublicRegisteredOwner>, Failure>, usize) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await
        .unwrap();
    let mut safe = Budget::new(65536, PublicSessionLimit::HeaderBytes);
    let mut proof = Proof::bounded(cap);
    let result = registered(&mut tx, session, limit, &mut safe, &mut proof).await;
    tx.rollback().await.unwrap();
    (result, proof.used())
}
#[tokio::test]
#[allow(clippy::too_many_lines)] // Preserve the causal history inside its explicit fixture scope.
async fn aggregate_owner_membership_and_shared_proof_budget() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let request = crate::session_owner_contract::reservation(SessionId::new());
        let SessionOwnerOutcome::Applied { receipt, authority } = store
            .reserve_session_owner(request.clone(), None)
            .await
            .unwrap()
        else {
            panic!("fresh")
        };
        let (result, used) = read(&store, &request.binding.session_id, 1, 65536).await;
        assert_eq!(result.unwrap()[0].target, receipt.owner.target);
        assert!(used > 1000);
        assert!(matches!(
            read(&store, &request.binding.session_id, 0, 65536).await.0,
            Err(Failure::Limited(PublicSessionLimitV2::RegisteredOwners))
        ));
        assert!(matches!(
            read(&store, &request.binding.session_id, 1, used - 1)
                .await
                .0,
            Err(Failure::Limited(PublicSessionLimitV2::ProofBytes))
        ));
        let original: String = sqlx::query_scalar(
            "SELECT data_json FROM session_owner_current WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        let mut record: super::super::Record = serde_json::from_str(&original).unwrap();
        record.snapshot.state = SessionOwnerState::Joined;
        record.snapshot.termination = Some(SessionOwnerTermination::Returned);
        record.token.clear();
        sqlx::query(
            "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .bind(serde_json::to_string(&record).unwrap())
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(matches!(
            read(&store, &request.binding.session_id, 0, 65536).await.0,
            Err(Failure::Error(PublicSessionError::UnknownStoreOutcome))
        ));
        sqlx::query(
            "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .bind(&original)
        .execute(&store.pool)
        .await
        .unwrap();
        store
            .mutate_session_owner(
                MutateSessionOwner {
                    id: SessionOwnerOperationId::new(),
                    session_id: request.binding.session_id.clone(),
                    target: receipt.owner.target,
                    command: SessionOwnerCommand::Join {
                        termination: SessionOwnerTermination::Cancelled,
                    },
                },
                authority,
            )
            .await
            .unwrap();
        assert_eq!(
            read(&store, &request.binding.session_id, 0, 65536)
                .await
                .0
                .unwrap(),
            [] as [crate::PublicRegisteredOwner; 0]
        );
        // Cohort membership prevents erasing a Joined identity. A malformed
        // mutable carrier must still refuse rather than becoming an empty ledger.
        sqlx::query("DELETE FROM session_owner_current WHERE session_id=$1 AND owner_id=$2")
            .bind(&request.binding.session_id.0)
            .bind(&request.binding.id.0)
            .execute(&store.pool)
            .await
            .unwrap_err();
        sqlx::query(
            "UPDATE session_owner_current SET data_json='{}' WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(matches!(
            read(&store, &request.binding.session_id, 1, 65536).await.0,
            Err(Failure::Error(PublicSessionError::UnknownStoreOutcome))
        ));
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn aggregate_owner_private_transfer_is_preflighted() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let request = crate::session_owner_contract::reservation(SessionId::new());
        store
            .reserve_session_owner(request.clone(), None)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .bind(format!(
            "{{\"origin\":{},\"private\":{}}}",
            serde_json::to_string(&request.id).unwrap(),
            serde_json::to_string(&"private-carrier-marker".repeat(100_000)).unwrap()
        ))
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(matches!(
            read(&store, &request.binding.session_id, 1, 128).await.0,
            Err(Failure::Limited(PublicSessionLimitV2::ProofBytes))
        ));
        assert!(matches!(
            read(&store, &request.binding.session_id, 1, 8 * 1024 * 1024)
                .await
                .0,
            Err(Failure::Error(PublicSessionError::UnknownStoreOutcome))
        ));
        sqlx::query(
            "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .bind("malformed-private-body".repeat(100_000))
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(matches!(
            read(&store, &request.binding.session_id, 1, 128).await.0,
            Err(Failure::Limited(PublicSessionLimitV2::ProofBytes))
        ));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn aggregate_owner_keyset_history_and_invalid_sentinel() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let session = SessionId::new();
        for index in 0..35 {
            let mut request = crate::session_owner_contract::reservation(session.clone());
            request.binding.id = crabber_core::SessionOwnerId(format!("owner-{index:03}"));
            let SessionOwnerOutcome::Applied { receipt, authority } = store
                .reserve_session_owner(request.clone(), None)
                .await
                .unwrap()
            else {
                panic!("fresh")
            };
            if index < 34 {
                store
                    .mutate_session_owner(
                        MutateSessionOwner {
                            id: SessionOwnerOperationId::new(),
                            session_id: session.clone(),
                            target: receipt.owner.target,
                            command: SessionOwnerCommand::Join {
                                termination: SessionOwnerTermination::Cancelled,
                            },
                        },
                        authority,
                    )
                    .await
                    .unwrap();
            }
        }
        let (owners, used) = read(&store, &session, 1, 2 * 1024 * 1024).await;
        assert_eq!(owners.unwrap()[0].target.id.0, "owner-034");
        assert!(used > 35_000);
        assert!(matches!(
            read(&store, &session, 0, 2 * 1024 * 1024).await.0,
            Err(Failure::Limited(PublicSessionLimitV2::RegisteredOwners))
        ));
        // A terminal label in a second keyset page cannot hide the live owner.
        let raw: String = sqlx::query_scalar(
            "SELECT data_json FROM session_owner_current WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&session.0)
        .bind("owner-034")
        .fetch_one(&store.pool)
        .await
        .unwrap();
        let mut record: super::super::Record = serde_json::from_str(&raw).unwrap();
        record.snapshot.state = SessionOwnerState::Joined;
        record.snapshot.termination = Some(SessionOwnerTermination::Cancelled);
        record.token.clear();
        sqlx::query(
            "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&session.0)
        .bind("owner-034")
        .bind(serde_json::to_string(&record).unwrap())
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(matches!(
            read(&store, &session, 0, 2 * 1024 * 1024).await.0,
            Err(Failure::Error(PublicSessionError::UnknownStoreOutcome))
        ));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn aggregate_history_profile_cannot_cross_raw_boundaries() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let (session, expected) = crate::public_history::tests::seed(&store).await;
        let mut limits = crate::public_session::tests::limits().history;
        limits.messages = 1;
        let request = crate::SnapshotRequest {
            session_id: session,
            limits,
            continuation: None,
        };
        let crate::SnapshotOutcome::Page(first) = store
            .read_snapshot_profile(
                request.clone(),
                crate::snapshot_profile::Profile::AggregateV2,
            )
            .await
            .unwrap()
        else {
            panic!("page")
        };
        let mut next = request.clone();
        next.continuation = first.continuation;
        assert!(store.read_snapshot(next.clone()).await.is_err());
        let crate::SnapshotOutcome::Page(second) = store
            .read_snapshot_profile(next.clone(), crate::snapshot_profile::Profile::AggregateV2)
            .await
            .unwrap()
        else {
            panic!("page")
        };
        assert_eq!(second.messages[0].id, expected.id);
        let crate::SnapshotOutcome::Page(raw) = store.read_snapshot(request).await.unwrap() else {
            panic!("page")
        };
        next.continuation = raw.continuation;
        assert!(
            store
                .read_snapshot_profile(next, crate::snapshot_profile::Profile::AggregateV2)
                .await
                .is_err()
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn aggregate_owner_private_transfer_is_preflighted() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let request = crate::session_owner_contract::reservation(SessionId::new());
        store
            .reserve_session_owner(request.clone(), None)
            .await
            .unwrap();
        let original: String = sqlx::query_scalar(
            "SELECT data_json FROM session_owner_current WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .bind(format!(
            "{{\"origin\":{},\"private\":{}}}",
            serde_json::to_string(&request.id).unwrap(),
            serde_json::to_string(&"private-carrier-marker".repeat(100_000)).unwrap()
        ))
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(matches!(
            read(&store, &request.binding.session_id, 1, 128).await.0,
            Err(Failure::Limited(PublicSessionLimitV2::ProofBytes))
        ));
        assert!(matches!(
            read(&store, &request.binding.session_id, 1, 8 * 1024 * 1024)
                .await
                .0,
            Err(Failure::Error(PublicSessionError::UnknownStoreOutcome))
        ));
        sqlx::query(
            "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .bind("malformed-private-body".repeat(100_000))
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(matches!(
            read(&store, &request.binding.session_id, 1, 128).await.0,
            Err(Failure::Limited(PublicSessionLimitV2::ProofBytes))
        ));
        // Restore the exact private carrier so later tests and repeated full runs
        // never inherit this deliberately malformed JSON fixture.
        sqlx::query(
            "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .bind(&original)
        .execute(&store.pool)
        .await
        .unwrap();
    })
    .await
    .unwrap();
}
