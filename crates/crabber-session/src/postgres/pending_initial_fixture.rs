//! Test-only physical fixture teardown, never lifecycle readiness evidence.
use super::*;
impl PostgresStore {
    /// Caller has already awaited its exact original producer and supervisor.
    /// This deliberately does not assert `MainJoin` or whole-resource coverage.
    pub(crate) async fn purge_pending_initial_fixture(
        &self,
        session: &SessionId,
    ) -> Result<(), StoreError> {
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(db)?;
        super::lock_session_scope(&mut tx, session).await?;
        let corrupt: bool = sqlx::query_scalar(crate::sql_deletion::CROSS_SCOPE)
            .bind(&session.0)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        if corrupt {
            return Err(StoreError::Validation("inconsistent fixture scope".into()));
        }
        let retained: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=$1)",
        )
        .bind(&session.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if !retained {
            let revision: Option<i64> =
                sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
                    .bind(&session.0)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?;
            let revision = crabber_core::SessionRevision::new(
                u64::try_from(revision.unwrap_or(0))
                    .map_err(|_| StoreError::Validation("invalid fixture revision".into()))?,
            )?;
            let request = crate::DeleteSession {
                id: crabber_core::ControlId::new(),
                session_id: session.clone(),
                expected_revision: revision,
            };
            let saved = crate::deletion::coordinator::tombstone(
                request.clone(),
                request
                    .fingerprint()
                    .map_err(|_| StoreError::Validation("invalid fixture tombstone".into()))?,
                self.clock.now(),
            )
            .map_err(|_| StoreError::Validation("invalid fixture tombstone".into()))?;
            sqlx::query("INSERT INTO session_tombstones(session_id,operation_id,expected_revision,final_revision,semantic_digest,deleted_at_seconds,deleted_at_nanos,format_version) VALUES($1,$2,$3,$4,$5,$6,$7,1)")
                .bind(&session.0).bind(&saved.receipt.operation_id.0)
                .bind(i64::try_from(saved.expected_revision.get()).map_err(|_| StoreError::Validation("invalid fixture revision".into()))?)
                .bind(i64::try_from(saved.receipt.final_revision.get()).map_err(|_| StoreError::Validation("invalid fixture revision".into()))?)
                .bind(saved.receipt.semantic_digest.as_str()).bind(saved.receipt.deleted_at.unix_timestamp())
                .bind(i32::try_from(saved.receipt.deleted_at.nanosecond()).map_err(|_| StoreError::Validation("invalid fixture time".into()))?)
                .execute(&mut *tx).await.map_err(db)?;
        }
        for statement in crate::sql_deletion::PURGE {
            sqlx::query(statement)
                .bind(&session.0)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        sqlx::query("DELETE FROM sessions WHERE id=$1")
            .bind(&session.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }
}
