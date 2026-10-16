//! Current-schema initialization is read-only under the migration gate. Replaying
//! DDL against an active store can invert runtime table lock ordering.
use super::{Postgres, StoreError, Transaction, db};
use sqlx::Row;

pub(super) async fn is_current(tx: &mut Transaction<'_, Postgres>) -> Result<bool, StoreError> {
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='schema_version')")
        .fetch_one(&mut **tx).await.map_err(db)?;
    if !exists {
        return Ok(false);
    }
    // Unique integral version keys plus these three bounded scalars prove the
    // complete contiguous roster, without loading arbitrary version rows.
    let row = sqlx::query("SELECT COALESCE(MIN(version),0) AS minimum,COALESCE(MAX(version),0) AS maximum,COUNT(*) AS count FROM schema_version")
        .fetch_one(&mut **tx).await.map_err(db)?;
    let maximum: i32 = row.try_get("maximum").map_err(db)?;
    if maximum > crate::POSTGRES_SCHEMA_VERSION {
        return Err(StoreError::Validation(
            "unsupported PostgreSQL schema version".into(),
        ));
    }
    Ok(maximum == crate::POSTGRES_SCHEMA_VERSION
        && row.try_get::<i32, _>("minimum").map_err(db)? == 1
        && row.try_get::<i64, _>("count").map_err(db)? == i64::from(maximum))
}
