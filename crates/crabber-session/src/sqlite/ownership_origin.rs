//! Creation coverage is committed with its independent original source.
#[path = "ownership_origin/admission.rs"]
mod admission;
use crate::deletion::origin::OwnershipOrigin;
pub(super) use admission::record_admission;
pub(super) async fn record(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    origin: &OwnershipOrigin,
) -> Result<(), crate::StoreError> {
    sqlx::query("INSERT INTO session_ownership_origins(session_id,data_json) VALUES($1,$2)")
        .bind(&origin.session_id.0)
        .bind(origin.encoded()?)
        .execute(&mut **tx)
        .await
        .map_err(super::db)?;
    Ok(())
}

use crate::DeletionError as Error;
use crate::deletion::coverage::{CoverageRead, sql::OriginEvidence};
use crate::deletion::origin::OriginKind;
use crabber_core::SessionId;
use sqlx::Row;

fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}

/// Caller holds the ordinary session mutation gate throughout these reads and
/// native readiness/purge. No carrier is fetched before its SQL byte cap.
pub(super) async fn coverage(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    session: &SessionId,
) -> Result<CoverageRead, Error> {
    let row = sqlx::query(CURRENT)
        .bind(&session.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    if !row.try_get::<bool, _>("present").map_err(unknown)? {
        return Ok(CoverageRead::Unknown);
    }
    if row.try_get::<i64, _>("session_bytes").map_err(unknown)? > 16384
        || row.try_get::<i64, _>("origin_bytes").map_err(unknown)? > 8192
    {
        return Ok(CoverageRead::Limited);
    }
    let evidence = OriginEvidence::decode(
        session,
        row.try_get("session_data").map_err(unknown)?,
        row.try_get("origin_data").map_err(unknown)?,
        row.try_get("public_revision").map_err(unknown)?,
        row.try_get("protocol").map_err(unknown)?,
    )?;
    original(tx, session, evidence).await
}

async fn original(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    session: &SessionId,
    evidence: OriginEvidence,
) -> Result<CoverageRead, Error> {
    let query = match evidence.kind() {
        OriginKind::ManagedCreate => MANAGED,
        OriginKind::Fork => FORK,
        OriginKind::PreparedAdmission => return admission::coverage(tx, session, evidence).await,
    };
    let row = sqlx::query(query)
        .bind(&session.0)
        .bind(evidence.operation())
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    for (column, cap) in [
        ("kind_bytes", 32),
        ("digest_bytes", 64),
        ("receipt_bytes", 32768),
        ("audit_bytes", 32768),
    ] {
        if row.try_get::<i64, _>(column).map_err(unknown)? > cap {
            return Ok(CoverageRead::Limited);
        }
    }
    evidence.verify(
        row.try_get("kind").map_err(unknown)?,
        row.try_get("digest").map_err(unknown)?,
        row.try_get("event_seq").map_err(unknown)?,
        row.try_get("receipt").map_err(unknown)?,
        row.try_get("audit").map_err(unknown)?,
    )
}

const CURRENT: &str = "SELECT s.ownership_protocol AS protocol,s.public_revision,o.session_id IS NOT NULL AS present,length(CAST(s.data AS BLOB)) AS session_bytes,length(CAST(o.data_json AS BLOB)) AS origin_bytes,CASE WHEN length(CAST(s.data AS BLOB))<=16384 THEN s.data END AS session_data,CASE WHEN length(CAST(o.data_json AS BLOB))<=8192 THEN o.data_json END AS origin_data FROM sessions s LEFT JOIN session_ownership_origins o ON o.session_id=s.id WHERE s.id=$1";
const MANAGED: &str = "SELECT length(CAST(r.kind AS BLOB)) AS kind_bytes,length(CAST(r.semantic_digest AS BLOB)) AS digest_bytes,length(CAST(r.data_json AS BLOB)) AS receipt_bytes,length(CAST(a.data_json AS BLOB)) AS audit_bytes,CASE WHEN length(CAST(r.kind AS BLOB))<=32 THEN r.kind END AS kind,CASE WHEN length(CAST(r.semantic_digest AS BLOB))<=64 THEN r.semantic_digest END AS digest,r.event_seq,CASE WHEN length(CAST(r.data_json AS BLOB))<=32768 THEN r.data_json END AS receipt,CASE WHEN length(CAST(a.data_json AS BLOB))<=32768 THEN a.data_json END AS audit FROM operation_receipts r LEFT JOIN session_management_audit a ON a.session_id=r.session_id AND a.operation_id=r.operation_id WHERE r.session_id=$1 AND r.operation_id=$2";
const FORK: &str = "SELECT length(CAST(r.kind AS BLOB)) AS kind_bytes,length(CAST(r.semantic_digest AS BLOB)) AS digest_bytes,length(CAST(r.data_json AS BLOB)) AS receipt_bytes,length(CAST(a.data_json AS BLOB)) AS audit_bytes,CASE WHEN length(CAST(r.kind AS BLOB))<=32 THEN r.kind END AS kind,CASE WHEN length(CAST(r.semantic_digest AS BLOB))<=64 THEN r.semantic_digest END AS digest,r.event_seq,CASE WHEN length(CAST(r.data_json AS BLOB))<=32768 THEN r.data_json END AS receipt,CASE WHEN length(CAST(a.data_json AS BLOB))<=32768 THEN a.data_json END AS audit FROM operation_receipts r LEFT JOIN session_fork_audit a ON a.session_id=r.session_id AND a.operation_id=r.operation_id WHERE r.session_id=$1 AND r.operation_id=$2";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeletionError, Store};
    use crabber_core::SessionRevision;

    async fn read(store: &crate::SqliteStore, session: &SessionId) -> Result<CoverageRead, Error> {
        let mut tx = store.begin_write().await.unwrap();

        let result = coverage(&mut tx, session).await;
        tx.rollback().await.unwrap();
        result
    }
    async fn known(store: &crate::SqliteStore, session: &SessionId, revision: SessionRevision) {
        let CoverageRead::Verified(proof) = read(store, session).await.unwrap() else {
            panic!("original creation coverage");
        };
        assert_eq!(proof.session_id(), session);
        assert_eq!(proof.revision(), revision);
        assert!(proof.proof_bytes() > 0);
    }
    #[tokio::test]
    async fn creation_coverage_authenticates_original_managed_and_fork_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("coverage.db");
        crate::SqliteStore::migrate(&path).await.unwrap();
        let store = crate::SqliteStore::connect(&path).await.unwrap();
        let created = store
            .manage_session(crate::management_contract::create())
            .await
            .unwrap();
        known(&store, &created.session_id, created.revision).await;
        let fork = store
            .fork_session(crate::sql_fork_contract::request(&created.summary))
            .await
            .unwrap();
        let crate::ForkOutcome::Applied(fork) = fork else {
            panic!("new fork")
        };
        known(&store, &fork.target.session_id, fork.target.revision).await;
        let renamed = store
            .manage_session(crate::ManageSession {
                id: crabber_core::ControlId::new(),
                session_id: created.session_id.clone(),
                expected_revision: created.revision,
                change: crate::SessionChange::Rename {
                    title: "later metadata".into(),
                },
            })
            .await
            .unwrap();
        known(&store, &created.session_id, renamed.revision).await;
        sqlx::query("DELETE FROM session_management_audit WHERE session_id=$1 AND operation_id=$2")
            .bind(&created.session_id.0)
            .bind(&created.operation_id.0)
            .execute(&store.writer)
            .await
            .unwrap();
        assert!(matches!(
            read(&store, &created.session_id).await,
            Err(DeletionError::UnknownStoreOutcome)
        ));
        // The target authenticates its own immutable fork origin, independently
        // of current source metadata and source management proof availability.
        known(&store, &fork.target.session_id, fork.target.revision).await;
        sqlx::query("DELETE FROM session_fork_audit WHERE session_id=$1 AND operation_id=$2")
            .bind(&fork.target.session_id.0)
            .bind(&fork.operation_id.0)
            .execute(&store.writer)
            .await
            .unwrap();
        assert!(matches!(
            read(&store, &fork.target.session_id).await,
            Err(DeletionError::UnknownStoreOutcome)
        ));
    }
    #[tokio::test]
    async fn creation_coverage_bounds_original_carriers_and_preserves_legacy_unknown() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("coverage.db");
        crate::SqliteStore::migrate(&path).await.unwrap();
        let store = crate::SqliteStore::connect(&path).await.unwrap();
        let legacy = SessionId::new();
        store
            .admit_run(crate::deletion::contract::admission(&legacy))
            .await
            .unwrap();
        assert!(matches!(
            read(&store, &legacy).await.unwrap(),
            CoverageRead::Unknown
        ));
        let created = store
            .manage_session(crate::management_contract::create())
            .await
            .unwrap();
        sqlx::query("UPDATE session_management_audit SET data_json=$3 WHERE session_id=$1 AND operation_id=$2")
            .bind(&created.session_id.0).bind(&created.operation_id.0).bind(" ".repeat(32769)).execute(&store.writer).await.unwrap();
        assert!(matches!(
            read(&store, &created.session_id).await.unwrap(),
            CoverageRead::Limited
        ));
        assert!(
            store
                .get_session(&created.session_id)
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[cfg(test)]
#[path = "ownership_origin/tests.rs"]
mod prepared_tests;
