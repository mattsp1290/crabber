use crate::SqliteStore;
use crabber_core::SessionId;
async fn disable(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> Vec<(String, String)> {
    let triggers: Vec<(String,String)> = sqlx::query_as("SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND name IN ('owner_scope_origin_delete','owner_scope_origin_immutable','contexts_owner_scope_immutable','session_owner_publication_no_delete','session_owner_cohort_no_delete')")
        .fetch_all(&mut **tx).await.unwrap();
    assert_eq!(triggers.len(), 5);
    for (name, _) in &triggers {
        sqlx::query(&format!("DROP TRIGGER {name}"))
            .execute(&mut **tx)
            .await
            .unwrap();
    }
    triggers
}
async fn restore(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>, triggers: Vec<(String, String)>) {
    for (_, ddl) in triggers {
        sqlx::query(&ddl).execute(&mut **tx).await.unwrap();
    }
}
pub(super) async fn corrupt_origin(
    store: &SqliteStore,
    session: &SessionId,
    value: Option<String>,
) {
    let mut tx = store.begin_write().await.unwrap();
    let triggers = disable(&mut tx).await;
    if let Some(value) = value {
        sqlx::query("UPDATE session_owner_scope_origins SET data_json=$2 WHERE session_id=$1")
            .bind(&session.0)
            .bind(value)
            .execute(&mut *tx)
            .await
            .unwrap();
    } else {
        sqlx::query("DELETE FROM session_owner_scope_origins WHERE session_id=$1")
            .bind(&session.0)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    restore(&mut tx, triggers).await;
    tx.commit().await.unwrap();
}
pub(super) async fn erase_old_scope(store: &SqliteStore, session: &SessionId) {
    let mut tx = store.begin_write().await.unwrap();
    let triggers = disable(&mut tx).await;
    sqlx::query("UPDATE session_owner_contexts SET scope_protocol=0 WHERE session_id=$1")
        .bind(&session.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    for table in [
        "session_owner_scope_origins",
        "session_owner_publications",
        "session_owner_publication_cohorts",
        "session_owner_audits",
        "session_owner_current",
    ] {
        sqlx::query(&format!("DELETE FROM {table} WHERE session_id=$1"))
            .bind(&session.0)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    restore(&mut tx, triggers).await;
    tx.commit().await.unwrap();
}
