//! Fresh owned factory eligibility under one bounded original-source proof.
use super::{PostgresStore, protocol, reads};
use crate::public_session::v2::proof::Proof;
use crate::{QueueAuthority, QueueState, RetainedQueue, StoreError};
use crabber_core::{SessionContext, SessionRevision};
impl PostgresStore {
    pub(in crate::postgres) async fn queue_owned_inspection(
        &self,
        context: &SessionContext,
        authority: &QueueAuthority,
        expected_revision: SessionRevision,
    ) -> Result<RetainedQueue, StoreError> {
        if context.session_id() != &authority.session_id {
            return Err(StoreError::SessionIdentityMismatch);
        }
        crate::queue::scope(
            context.session_id(),
            &crabber_core::ControlId("inspect".into()),
        )?;
        crate::queue::identifier(&authority.queue_id.0)?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(super::super::db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(super::super::db)?;
        let mut proof = Proof::bounded(32 << 20);
        let (session, metadata, revision) =
            reads::current_with_proof(&mut tx, context.session_id(), &mut proof)
                .await
                .map_err(protocol::domain)?
                .ok_or(StoreError::NotFound)?;
        session
            .ensure_identity(context.workspace_id(), context.directory())
            .map_err(|_| StoreError::SessionIdentityMismatch)?;
        if metadata.archived {
            return Err(StoreError::Conflict);
        }
        if revision != expected_revision {
            return Err(StoreError::AdmissionRevisionConflict);
        }
        protocol::routing(&mut tx, &authority.session_id, &mut proof)
            .await
            .map_err(protocol::domain)?;
        let saved = reads::record_with_proof(
            &mut tx,
            &authority.session_id,
            &authority.queue_id,
            &mut proof,
        )
        .await?
        .ok_or(StoreError::NotFound)?;
        if !protocol::protocol(
            &mut tx,
            &crate::queue::source::Scope {
                session: authority.session_id.clone(),
                queue: authority.queue_id.clone(),
            },
            &mut proof,
        )
        .await
        .map_err(protocol::domain)?
        {
            return Err(StoreError::HostUnsupported);
        }
        let claim = saved.claim.as_ref().ok_or(StoreError::Conflict)?;
        if saved.summary.state != QueueState::Claimed
            || claim.authority != *authority
            || claim.expires <= self.clock.now()
            || reads::head(&mut tx, &authority.session_id)
                .await?
                .is_none_or(|head| head.queue_id != authority.queue_id)
        {
            return Err(StoreError::Conflict);
        }
        let busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE session_id=$1 AND status IN ('pending','running','paused'))").bind(&authority.session_id.0).fetch_one(&mut *tx).await.map_err(super::super::db)?;
        if busy || super::super::has_pending_continuation(&mut tx, &authority.session_id).await? {
            return Err(StoreError::Busy);
        }
        let result = saved.retained(&mut proof)?;
        tx.rollback().await.map_err(super::super::db)?;
        Ok(result)
    }
}
