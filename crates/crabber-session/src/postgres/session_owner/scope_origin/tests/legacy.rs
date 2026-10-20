use crate::PostgresStore;
use crabber_core::SessionId;
pub(super) async fn corrupt_origin(
    store: &PostgresStore,
    session: &SessionId,
    value: Option<String>,
) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE session_owner_scope_origins DISABLE TRIGGER USER")
        .execute(&mut *tx)
        .await
        .unwrap();
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
    sqlx::query("ALTER TABLE session_owner_scope_origins ENABLE TRIGGER USER")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
pub(super) async fn erase_old_scope(store: &PostgresStore, session: &SessionId) {
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    for table in [
        "session_owner_scope_origins",
        "session_owner_contexts",
        "session_owner_publications",
        "session_owner_publication_cohorts",
    ] {
        sqlx::query(&format!("ALTER TABLE {table} DISABLE TRIGGER USER"))
            .execute(&mut *tx)
            .await
            .unwrap();
    }
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
    for table in [
        "session_owner_scope_origins",
        "session_owner_contexts",
        "session_owner_publications",
        "session_owner_publication_cohorts",
    ] {
        sqlx::query(&format!("ALTER TABLE {table} ENABLE TRIGGER USER"))
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
}
