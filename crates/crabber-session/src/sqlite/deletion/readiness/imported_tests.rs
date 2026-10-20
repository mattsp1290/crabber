//! Original immutable fork digest authenticates ownership of imported IDs.
use super::*;
use crate::{Store, deletion::coverage::CoverageRead};
async fn check(
    store: &crate::SqliteStore,
    session: &SessionId,
) -> Result<Readiness, DeletionError> {
    let mut tx = store.begin_write().await.unwrap();

    let CoverageRead::Verified(coverage) =
        crate::sqlite::ownership_origin::coverage(&mut tx, session).await?
    else {
        panic!("creation origin");
    };
    let outcome = readiness(&mut tx, session, &coverage).await;
    tx.rollback().await.unwrap();
    outcome
}
#[tokio::test]
async fn imported_history_requires_original_digest_before_readiness() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;

    let store = store.with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let fork = crate::sql_deletion::readiness::contract::closed_fork(&store).await;
    assert!(matches!(
        check(&store, &fork.target.session_id).await.unwrap(),
        Readiness::Ready(_)
    ));
    let orphan = crabber_core::ToolCallId::new();
    sqlx::query("INSERT INTO tool_call_identities(id,kind) VALUES($1,'imported')")
        .bind(&orphan.0)
        .execute(&store.writer)
        .await
        .unwrap();
    corrupt_identity(&store, &fork.target.session_id, &orphan).await;
    let revision = store
        .session_revision(&fork.target.session_id)
        .await
        .unwrap();
    let before = store
        .list_all_messages(&fork.target.session_id)
        .await
        .unwrap();
    assert!(matches!(
        check(&store, &fork.target.session_id).await,
        Err(DeletionError::UnknownStoreOutcome)
    ));
    assert_eq!(
        store
            .session_revision(&fork.target.session_id)
            .await
            .unwrap(),
        revision
    );
    assert_eq!(
        store
            .list_all_messages(&fork.target.session_id)
            .await
            .unwrap(),
        before
    );
    let retained: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_call_identities WHERE id=$1)")
            .bind(&orphan.0)
            .fetch_one(&store.writer)
            .await
            .unwrap();
    assert!(retained);
}
async fn corrupt_identity(
    store: &crate::SqliteStore,
    session: &SessionId,
    new: &crabber_core::ToolCallId,
) {
    let raw: String =
        sqlx::query_scalar("SELECT data_json FROM imported_tool_calls WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&store.writer)
            .await
            .unwrap();
    let mut call: crabber_core::ImportedToolCall = serde_json::from_str(&raw).unwrap();
    let old = std::mem::replace(&mut call.id, new.clone());
    let mut messages = store.list_all_messages(session).await.unwrap();
    let mut tx = store.begin_write().await.unwrap();
    let accounting = crate::sql_snapshot::imported_call_record(&call).unwrap();
    sqlx::query("UPDATE imported_tool_calls SET id=$1,data_json=$2,snapshot_text=$3,snapshot_bytes=$4 WHERE id=$5")
        .bind(&new.0).bind(accounting.record).bind(accounting.text).bind(accounting.bytes).bind(&old.0).execute(&mut *tx).await.unwrap();
    for message in &mut messages {
        for part in &mut message.parts {
            match &mut part.content {
                crabber_core::ContentBlock::ToolCall { call_id, .. }
                | crabber_core::ContentBlock::ToolResult { call_id, .. }
                    if *call_id == old =>
                {
                    *call_id = new.clone();
                }
                _ => (),
            }
            sqlx::query("UPDATE parts SET data=$2 WHERE id=$1")
                .bind(&part.id.0)
                .bind(crate::sqlite::text(&part).unwrap())
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        let accounting = crate::sql_snapshot::message_record(message).unwrap();
        sqlx::query("UPDATE messages SET data=$2,snapshot_parts=$3,snapshot_text=$4,snapshot_bytes=$5 WHERE id=$1")
            .bind(&message.id.0).bind(accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
}
