//! Independent selected transition sources survive audit/roster/current rollback.
use crate::Store;
#[tokio::test]
async fn owner_publication_roster_and_monotonic_head_detect_cancel_rollback() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    for both in [false, true] {
        let (access, _service, cancel) =
            crate::session_owner_contract::services::cohort::cancelled_across_roots(&store).await;
        let session = &access.source.root.session_id;
        let id = &access.authority.target().id;
        let revision = store.session_revision(session).await.unwrap();
        let old:String=sqlx::query_scalar("SELECT data_json FROM session_owner_audits WHERE session_id=$1 AND owner_id=$2 AND operation='ActivateScope'")
            .bind(&session.0).bind(&id.0).fetch_one(&store.pool).await.unwrap();
        let old: serde_json::Value = serde_json::from_str(&old).unwrap();
        assert!(sqlx::query("UPDATE session_owner_publication_cohorts SET head_sequence=head_sequence-1 WHERE session_id=$1 AND owner_id=$2")
            .bind(&session.0).bind(&id.0).execute(&store.pool).await.is_err());
        assert!(
            sqlx::query(
                "DELETE FROM session_owner_publications WHERE session_id=$1 AND operation_id=$2"
            )
            .bind(&session.0)
            .bind(&cancel.0)
            .execute(&store.pool)
            .await
            .is_err()
        );
        let mut tx = store.pool.begin().await.unwrap();
        sqlx::raw_sql(
            "DROP TRIGGER session_owner_publication_immutable ON session_owner_publications",
        )
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
        sqlx::raw_sql("CREATE TRIGGER session_owner_publication_immutable BEFORE UPDATE OR DELETE ON session_owner_publications FOR EACH ROW EXECUTE FUNCTION crabber_owner_publication_guard()").execute(&mut *tx).await.unwrap();
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
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let (request, receipt, _next) =
        crate::session_owner_contract::services::cohort::source_fixture(&store).await;
    let mut tx = store.pool.begin().await.unwrap();
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
    assert_eq!(
        super::super::reads::publish_with_proof(&mut tx, &audit, false, &mut shared,)
            .await
            .unwrap_err(),
        crate::session_owner::SessionOwnerError::Capacity
    );
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
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
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
    let mut tx = store.pool.begin().await.unwrap();
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
    store: &crate::PostgresStore,
    request: &crate::session_owner::ReserveSessionOwner,
) {
    let mut tx = store.pool.begin().await.unwrap();
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
        r"DROP TRIGGER session_owner_publication_immutable ON session_owner_publications;
DROP TRIGGER session_owner_cohort_immutable ON session_owner_publication_cohorts;
DROP TRIGGER session_owner_protocol_immutable ON session_owner_current;",
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
    sqlx::raw_sql(r"CREATE TRIGGER session_owner_publication_immutable BEFORE UPDATE OR DELETE ON session_owner_publications FOR EACH ROW EXECUTE FUNCTION crabber_owner_publication_guard();
CREATE TRIGGER session_owner_cohort_immutable BEFORE INSERT OR UPDATE OR DELETE ON session_owner_publication_cohorts FOR EACH ROW EXECUTE FUNCTION crabber_owner_cohort_guard();
CREATE TRIGGER session_owner_protocol_immutable BEFORE UPDATE OF source_protocol ON session_owner_current FOR EACH ROW EXECUTE FUNCTION crabber_owner_protocol_immutable();").execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn migration_preserves_legacy_owner_bytes_without_positive_backfill() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let mut tx = store.pool.begin().await.unwrap();
    let namespace = format!("owner_protocol_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {namespace}; SET LOCAL search_path TO {namespace}"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::raw_sql("CREATE TABLE schema_version(version INTEGER PRIMARY KEY); INSERT INTO schema_version SELECT generate_series(1,17); CREATE TABLE session_owner_current(session_id TEXT,owner_id TEXT,data_json TEXT,PRIMARY KEY(session_id,owner_id)); CREATE TABLE session_owner_audits(session_id TEXT,owner_id TEXT,data_json TEXT); CREATE TABLE session_tombstones(session_id TEXT); CREATE TABLE sessions(id TEXT PRIMARY KEY); CREATE TABLE session_owner_contexts(session_id TEXT PRIMARY KEY,data_json TEXT)")
        .execute(&mut *tx).await.unwrap();
    sqlx::raw_sql("INSERT INTO sessions VALUES('legacy-session'); INSERT INTO session_owner_contexts VALUES('legacy-session','{}')").execute(&mut *tx).await.unwrap();
    let original = r#"{"version":2,"opaque":"unchanged legacy source"}"#;
    sqlx::query("INSERT INTO session_owner_current VALUES('legacy-session','legacy-owner',$1)")
        .bind(original)
        .execute(&mut *tx)
        .await
        .unwrap();
    // This fixture deliberately models only the tables consumed by migration 18.
    sqlx::raw_sql(include_str!(
        "../../../../migrations/0018_owner_publications.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    let saved: String = sqlx::query_scalar("SELECT data_json FROM session_owner_current")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let protocol: i16 = sqlx::query_scalar("SELECT source_protocol FROM session_owner_current")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(saved, original);
    assert_eq!(protocol, 0);
    let markers: (i16,i16) = sqlx::query_as("SELECT s.owner_scope_protocol,c.scope_protocol FROM sessions s JOIN session_owner_contexts c ON c.session_id=s.id").fetch_one(&mut *tx).await.unwrap();
    assert_eq!(markers, (0, 0));
    let origins: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM session_owner_scope_origins")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(origins, 0);
    let count: i64 = sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM session_owner_publications)+(SELECT COUNT(*) FROM session_owner_publication_cohorts)")
        .fetch_one(&mut *tx).await.unwrap();
    assert_eq!(count, 0);
    tx.rollback().await.unwrap();
}
