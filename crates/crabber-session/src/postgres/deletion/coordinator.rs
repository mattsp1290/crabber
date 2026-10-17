//! One scope transaction from original receipt reconciliation through erasure.
use super::super::PostgresStore;
use crate::deletion::{coordinator, coverage::CoverageRead};
use crate::sql_deletion::readiness::Readiness;
use crate::{DeleteSession, DeletionBlocker as Blocker, DeletionError as Error, DeletionOutcome};

impl PostgresStore {
    pub(in crate::postgres) async fn guarded_delete(
        &self,
        request: DeleteSession,
    ) -> Result<DeletionOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(super::unknown)?;
        super::super::lock_session_scope(&mut tx, &request.session_id)
            .await
            .map_err(super::unknown)?;
        let row = sqlx::query(super::VIEW)
            .bind(&request.session_id.0)
            .fetch_one(&mut *tx)
            .await
            .map_err(super::unknown)?;
        let (saved, revision) = super::decode_view(&row, &request.session_id)?;
        if let Some(saved) = saved {
            let result = coordinator::replay(&saved, &request, &digest)?;
            tx.commit().await.map_err(super::unknown)?;
            return Ok(result);
        }
        let occupied: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM operation_receipts WHERE session_id=$1 AND operation_id=$2)")
            .bind(&request.session_id.0).bind(&request.id.0).fetch_one(&mut *tx).await.map_err(super::unknown)?;
        if occupied {
            return Err(Error::SemanticConflict);
        }
        let revision = revision.ok_or(Error::NotFound)?;
        if revision != request.expected_revision {
            return Err(Error::RevisionConflict);
        }
        let coverage =
            match super::super::ownership_origin::coverage(&mut tx, &request.session_id).await? {
                CoverageRead::Unknown => {
                    return Ok(DeletionOutcome::Blocked(Blocker::OwnershipUnknown));
                }
                CoverageRead::Limited => return Ok(DeletionOutcome::Blocked(Blocker::ProofLimit)),
                CoverageRead::Verified(value) => value,
            };
        let scope =
            match super::readiness::readiness(&mut tx, &request.session_id, &coverage).await? {
                Readiness::Blocked(blocker) => return Ok(DeletionOutcome::Blocked(blocker)),
                Readiness::Ready(scope) => scope,
            };
        let saved = coordinator::tombstone(request, digest, self.clock.now())?;
        sqlx::query("INSERT INTO session_tombstones(session_id,operation_id,expected_revision,final_revision,semantic_digest,deleted_at_seconds,deleted_at_nanos,format_version) VALUES($1,$2,$3,$4,$5,$6,$7,1)")
            .bind(&saved.receipt.session_id.0).bind(&saved.receipt.operation_id.0)
            .bind(i64::try_from(saved.expected_revision.get()).map_err(super::unknown)?)
            .bind(i64::try_from(saved.receipt.final_revision.get()).map_err(super::unknown)?)
            .bind(saved.receipt.semantic_digest.as_str()).bind(saved.receipt.deleted_at.unix_timestamp())
            .bind(i32::try_from(saved.receipt.deleted_at.nanosecond()).map_err(super::unknown)?).execute(&mut *tx).await.map_err(super::unknown)?;
        super::purge::purge(&mut tx, &scope).await?;
        #[cfg(test)]
        crate::binding_fault::trip(&self.deletion_fault, 1).map_err(super::unknown)?;
        #[cfg(test)]
        crate::deletion::process::pause("before_commit");
        tx.commit().await.map_err(super::unknown)?;
        #[cfg(test)]
        crate::deletion::process::pause("after_commit");
        #[cfg(test)]
        crate::binding_fault::trip(&self.deletion_fault, 2).map_err(super::unknown)?;
        Ok(DeletionOutcome::Applied(saved.receipt))
    }
}

#[cfg(test)]
mod tests;
