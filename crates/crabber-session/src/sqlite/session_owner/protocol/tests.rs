//! Independent selected transition sources survive audit/roster/current rollback.
use crate::Store;
#[tokio::test]
async fn owner_publication_roster_and_monotonic_head_detect_cancel_rollback() {
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    for both in [false, true] {
        let (access, _service, cancel) =
            crate::session_owner_contract::services::cohort::cancelled_across_roots(&store).await;
        let session = &access.source.root.session_id;
        let id = &access.authority.target().id;
        let revision = store.session_revision(session).await.unwrap();
        let old:String=sqlx::query_scalar("SELECT data_json FROM session_owner_audits WHERE session_id=$1 AND owner_id=$2 AND operation='ActivateScope'")
            .bind(&session.0).bind(&id.0).fetch_one(&store.writer).await.unwrap();
        let old: serde_json::Value = serde_json::from_str(&old).unwrap();
        assert!(sqlx::query("UPDATE session_owner_publication_cohorts SET head_sequence=head_sequence-1 WHERE session_id=$1 AND owner_id=$2")
            .bind(&session.0).bind(&id.0).execute(&store.writer).await.is_err());
        assert!(
            sqlx::query(
                "DELETE FROM session_owner_publications WHERE session_id=$1 AND operation_id=$2"
            )
            .bind(&session.0)
            .bind(&cancel.0)
            .execute(&store.writer)
            .await
            .is_err()
        );
        let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
        sqlx::raw_sql("DROP TRIGGER session_owner_publication_no_delete")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(
            "DELETE FROM session_owner_publications WHERE session_id=$1 AND operation_id=$2",
        )
        .bind(&session.0)
        .bind(&cancel.0)
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::raw_sql("CREATE TRIGGER session_owner_publication_no_delete BEFORE DELETE ON session_owner_publications WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_owner_publication_immutable'); END").execute(&mut *tx).await.unwrap();
        if both {
            sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
                .bind(&session.0)
                .bind(&cancel.0)
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE session_owner_current SET data_json=$1 WHERE session_id=$2 AND owner_id=$3",
            )
            .bind(old["record"].to_string())
            .bind(&session.0)
            .bind(&id.0)
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        assert_eq!(store.session_revision(session).await.unwrap(), revision);
        assert_eq!(
            store
                .check_session_service_dependency_access(access)
                .await
                .unwrap_err(),
            crate::session_owner::SessionOwnerError::UnknownStoreOutcome
        );
    }
}
#[tokio::test]
async fn selected_owner_publication_sources_charge_shared_budget() {
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    let (request, receipt, _next) =
        crate::session_owner_contract::services::cohort::source_fixture(&store).await;
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let mut proof = crate::public_session::v2::proof::Proof::bounded(1);
    assert!(matches!(
        super::verify(
            &mut tx,
            &receipt.source.root.session_id,
            &receipt.began.owner.target.id,
            &mut proof
        )
        .await,
        Err(crate::public_session::v2::proof::ReadError::Budget(_))
    ));
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 * 1024 * 1024);
    assert_eq!(
        super::verify(
            &mut tx,
            &receipt.source.root.session_id,
            &receipt.began.owner.target.id,
            &mut proof
        )
        .await
        .unwrap(),
        crate::session_owner::publication::Coverage::Verified
    );
    assert!(proof.used() > receipt.source.generation.id.len());
    let audit = super::super::reads::audit(
        &mut tx,
        &receipt.source.root.session_id,
        &receipt.began.operation_id,
    )
    .await
    .unwrap()
    .unwrap();
    let consumed = proof.used();
    // Each materialization consumes the remaining caller allowance, including
    // a compound writer's recheck. A separate fresh writer budget would pass
    // the guard and instead reach the duplicate INSERT below.
    let mut shared = crate::public_session::v2::proof::Proof::bounded(consumed + 1);
    shared.charge(consumed).unwrap();
    assert!(matches!(
        super::super::reads::publish_with_proof(&mut tx, &audit, false, &mut shared).await,
        Err(crate::public_session::v2::proof::ReadError::Budget(
            crate::public_session::v2::proof::Failure::Limited(
                crate::PublicSessionLimitV2::ProofBytes
            )
        ))
    ));
    assert_eq!(
        super::verify(
            &mut tx,
            &receipt.source.root.session_id,
            &receipt.began.owner.target.id,
            &mut crate::public_session::v2::proof::Proof::bounded(32 * 1024 * 1024),
        )
        .await
        .unwrap(),
        crate::session_owner::publication::Coverage::Verified
    );

    assert!(
        sqlx::query(
            "UPDATE session_owner_current SET source_protocol=0 WHERE session_id=$1 AND owner_id=$2"
        )
        .bind(&request.grant.owner_authority().session_id().0)
        .bind(&receipt.began.owner.target.id.0)
        .execute(&mut *tx)
        .await
        .is_err()
    );
}

#[tokio::test]
async fn legacy_owner_mutation_never_mints_a_publication_cohort() {
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    let mut request = crate::session_owner_contract::reservation(crabber_core::SessionId::new());
    request.lease_ms = 30_000;
    let crate::session_owner::SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store
        .reserve_session_owner(request.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh owner")
    };
    legacy_fixture(&store, &request).await;
    for command in [
        crate::session_owner::SessionOwnerCommand::Begin,
        crate::session_owner::SessionOwnerCommand::Renew { lease_ms: 30_000 },
    ] {
        store
            .mutate_session_owner(
                crate::session_owner::MutateSessionOwner {
                    id: crabber_core::SessionOwnerOperationId::new(),
                    session_id: request.binding.session_id.clone(),
                    target: authority.target().clone(),
                    command,
                },
                Some(authority.clone()),
            )
            .await
            .unwrap();
    }
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 * 1024 * 1024);
    assert_eq!(
        super::verify(
            &mut tx,
            &request.binding.session_id,
            &request.binding.id,
            &mut proof
        )
        .await
        .unwrap(),
        crate::session_owner::publication::Coverage::Legacy
    );
    assert!(matches!(
        super::require(
            &mut tx,
            &request.binding.session_id,
            &request.binding.id,
            &mut proof
        )
        .await,
        Err(crate::public_session::v2::proof::ReadError::Domain(
            crate::session_owner::SessionOwnerError::Unsupported
        ))
    ));
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM session_owner_publications WHERE session_id=$1")
            .bind(&request.binding.session_id.0)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(count, 0);
    assert!(
        sqlx::query(
            "UPDATE session_owner_current SET source_protocol=1 WHERE session_id=$1 AND owner_id=$2"
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .execute(&mut *tx)
        .await
        .is_err()
    );
}
// Reconstruct an actual old-v2 row without inventing a positive new origin.
// Dropping guards is confined to this fixture transaction and restored before commit.
pub(in super::super) async fn legacy_fixture(
    store: &crate::SqliteStore,
    request: &crate::session_owner::ReserveSessionOwner,
) {
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let text: String = sqlx::query_scalar(
        "SELECT data_json FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&request.binding.session_id.0)
    .bind(&request.id.0)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let mut old: serde_json::Value = serde_json::from_str(&text).unwrap();
    old["version"] = serde_json::json!(2);
    sqlx::raw_sql(
        r"DROP TRIGGER session_owner_publication_no_delete;
DROP TRIGGER session_owner_cohort_no_delete;
DROP TRIGGER session_owner_protocol_immutable;",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("DELETE FROM session_owner_publications WHERE session_id=$1 AND owner_id=$2")
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "DELETE FROM session_owner_publication_cohorts WHERE session_id=$1 AND owner_id=$2",
    )
    .bind(&request.binding.session_id.0)
    .bind(&request.binding.id.0)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE session_owner_current SET source_protocol=0 WHERE session_id=$1 AND owner_id=$2",
    )
    .bind(&request.binding.session_id.0)
    .bind(&request.binding.id.0)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE session_owner_audits SET data_json=$1 WHERE session_id=$2 AND operation_id=$3",
    )
    .bind(old.to_string())
    .bind(&request.binding.session_id.0)
    .bind(&request.id.0)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::raw_sql(r"CREATE TRIGGER session_owner_publication_no_delete BEFORE DELETE ON session_owner_publications WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_owner_publication_immutable'); END;
CREATE TRIGGER session_owner_cohort_no_delete BEFORE DELETE ON session_owner_publication_cohorts WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_owner_cohort_immutable'); END;
CREATE TRIGGER session_owner_protocol_immutable BEFORE UPDATE OF source_protocol ON session_owner_current WHEN NEW.source_protocol<>OLD.source_protocol BEGIN SELECT RAISE(ABORT,'crabber_owner_protocol_immutable'); END;").execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn alternate_unique_replace_cannot_erase_a_historical_publication() {
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    let (access, _service, _cancel) =
        crate::session_owner_contract::services::cohort::cancelled_across_roots(&store).await;
    let session = &access.source.root.session_id;
    let owner = &access.authority.target().id;
    sqlx::query("PRAGMA recursive_triggers=OFF")
        .execute(&store.writer)
        .await
        .unwrap();
    let recursive: i64 = sqlx::query_scalar("PRAGMA recursive_triggers")
        .fetch_one(&store.writer)
        .await
        .unwrap();
    assert_eq!(recursive, 0);
    let replaced=sqlx::query("INSERT OR REPLACE INTO session_owner_publications(session_id,owner_id,operation_id,sequence,audit_digest,data_json,operation,state) SELECT session_id,owner_id,'alternate-operation-identity',sequence,audit_digest,data_json,operation,state FROM session_owner_publications WHERE session_id=$1 AND owner_id=$2 AND sequence=2")
        .bind(&session.0).bind(&owner.0).execute(&store.writer).await;
    assert!(
        replaced.is_err(),
        "alternate UNIQUE replacement bypassed immutability"
    );
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 * 1024 * 1024);
    assert_eq!(
        super::verify(&mut tx, session, owner, &mut proof)
            .await
            .unwrap(),
        crate::session_owner::publication::Coverage::Verified
    );
}
