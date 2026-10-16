//! New creation coverage is committed with its independent original receipt.
use crate::deletion::origin::OwnershipOrigin;
pub(super) async fn record(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
