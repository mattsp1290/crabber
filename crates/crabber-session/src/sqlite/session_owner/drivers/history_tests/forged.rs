//! Coherent mutable Joined metadata cannot settle a physically running main.
use super::*;
use crate::Store;
use crate::session_owner::{
    SessionOwnerOperation,
    audit::{Audit, Publication, RevisionState},
    owned_scope::{JoinSource, OwnedMainDisposition},
};
#[tokio::test]
async fn selected_driver_join_refuses_forgery_while_actual_main_waits() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("forged-driver.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = std::sync::Arc::new(crate::SqliteStore::connect(&path).await.unwrap());
    for unkeyed in [false, true] {
        let active = history_fixture::active::start(store.clone(), unkeyed).await;
        let session = active.request.context.session_id();
        let revision = store.session_revision(session).await.unwrap();
        let mut tx = store.writer.begin().await.unwrap();
        let mut record = super::super::super::super::reads::current(
            &mut tx,
            session,
            &active.grant.authority().target().id,
        )
        .await
        .unwrap();
        let metadata = JoinSource {
            activation: active.main.activation().clone(),
            disposition: OwnedMainDisposition::Returned,
        };
        let request = metadata.request().unwrap();
        record
            .mutate(
                &request,
                Some(active.grant.authority()),
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
        record.owned_join = Some(metadata.clone());
        let audit = Audit {
            version: 3,
            receipt: record.receipt(
                request.id.clone(),
                metadata.fingerprint().unwrap(),
                SessionOwnerOperation::Join,
            ),
            record: record.clone(),
            publication: Some(Publication::new(RevisionState::Present(revision), true).unwrap()),
        };
        audit.validate().unwrap();
        sqlx::query("INSERT INTO session_owner_audits(session_id,operation_id,owner_id,sequence,operation,state,data_json) VALUES($1,$2,$3,$4,'Join','Joined',$5)")
            .bind(&session.0).bind(&request.id.0).bind(&record.snapshot.target.id.0)
            .bind(i64::try_from(record.sequence).unwrap()).bind(serde_json::to_string(&audit).unwrap())
            .execute(&mut *tx).await.unwrap();
        sqlx::query(
            "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&session.0)
        .bind(&record.snapshot.target.id.0)
        .bind(serde_json::to_string(&record).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
        super::super::super::super::reads::current(&mut tx, session, &record.snapshot.target.id)
            .await
            .unwrap();
        assert!(matches!(
            classified(&mut tx, &record, &mut Proof::bounded(32 * 1024 * 1024)).await,
            Err(Failure::Error(_))
        ));
        tx.rollback().await.unwrap();
        // Only now release and observe the actual main and independent cleanup task.
        active.finish(store.as_ref()).await;
    }
}
