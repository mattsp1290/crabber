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

/// Later immutable source protocols are applied only once; old runtime tables
/// are not rebuilt when advancing the current roster.
pub(super) async fn apply_sources(tx: &mut Transaction<'_, Postgres>) -> Result<(), StoreError> {
    for (version, source) in [
        (
            17,
            include_str!("../../migrations/0017_admission_execution_sources.sql"),
        ),
        (
            18,
            include_str!("../../migrations/0018_owner_publications.sql"),
        ),
        (
            19,
            include_str!("../../migrations/0019_native_run_sources.sql"),
        ),
        (
            20,
            include_str!("../../migrations/0020_continuation_input_sources.sql"),
        ),
        (
            21,
            include_str!("../../migrations/0021_queue_publications.sql"),
        ),
        (
            22,
            include_str!("../../migrations/0022_native_execution_enrollment.sql"),
        ),
        (
            23,
            include_str!("../../migrations/0023_pending_initial.sql"),
        ),
        (
            24,
            include_str!("../../migrations/0024_pending_initial_consumer.sql"),
        ),
        (
            25,
            include_str!("../../migrations/0025_workspace_isolation.sql"),
        ),
    ] {
        let applied: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM schema_version WHERE version=$1)")
                .bind(version)
                .fetch_one(&mut **tx)
                .await
                .map_err(db)?;
        if !applied {
            sqlx::raw_sql(source).execute(&mut **tx).await.map_err(db)?;
        }
    }
    Ok(())
}
