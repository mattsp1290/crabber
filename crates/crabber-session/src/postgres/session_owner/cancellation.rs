//! Cancellation publishes intent and signal acknowledgement without granting work.
use super::{
    Audit, Error, PostgresStore, SessionOwnerAuthority, SessionOwnerOperation, SessionOwnerOutcome,
    after, before, finish, publish_revision, reads, revision, unknown, validate_scope,
};
use crate::session_owner::{
    ObserveSessionOwnerCancel, RequestSessionOwnerCancel, SessionOwnerCancellation,
};
impl PostgresStore {
    pub(in crate::postgres) async fn request_owner_cancel(
        &self,
        request: RequestSessionOwnerCancel,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(unknown)?;
        let result = async {
            super::super::lock_session(&mut tx, &request.session_id)
                .await
                .map_err(crate::deletion::owner_error)?;
            if let Some(audit) = reads::audit(&mut tx, &request.session_id, &request.id).await? {
                if audit.receipt.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(SessionOwnerOutcome::Replayed(audit.receipt));
            }
            reads::scope(&mut tx, &request.session_id).await?;
            let mut record =
                reads::current(&mut tx, &request.session_id, &request.target.id).await?;
            let current = revision(&mut tx, &request.session_id).await?;
            record.request_cancel(&request, current, self.clock.now())?;
            let receipt = record.receipt(
                request.id.clone(),
                digest,
                SessionOwnerOperation::RequestCancel,
            );
            let publication = publish_revision(&mut tx, &request.session_id, true).await?;
            reads::publish(
                &mut tx,
                &Audit {
                    version: 2,
                    receipt: receipt.clone(),
                    record,
                    publication: Some(publication),
                },
                false,
            )
            .await?;
            before(self)?;
            Ok(SessionOwnerOutcome::Applied {
                receipt,
                authority: None,
            })
        }
        .await;
        let result = finish(tx, result).await?;
        after(self, result)
    }
    pub(in crate::postgres) async fn observe_owner_cancel(
        &self,
        request: ObserveSessionOwnerCancel,
        authority: SessionOwnerAuthority,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(unknown)?;
        let result = async {
            super::super::lock_session(&mut tx, &request.session_id)
                .await
                .map_err(crate::deletion::owner_error)?;
            if let Some(audit) = reads::audit(&mut tx, &request.session_id, &request.id).await? {
                if audit.receipt.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(SessionOwnerOutcome::Replayed(audit.receipt));
            }
            reads::scope(&mut tx, &request.session_id).await?;
            let mut record =
                reads::current(&mut tx, &request.session_id, &request.target.id).await?;
            record.observe_cancel(&request, &authority, self.clock.now())?;
            let receipt = record.receipt(
                request.id.clone(),
                digest,
                SessionOwnerOperation::ObserveCancel,
            );
            let publication = publish_revision(&mut tx, &request.session_id, true).await?;
            reads::publish(
                &mut tx,
                &Audit {
                    version: 2,
                    receipt: receipt.clone(),
                    record,
                    publication: Some(publication),
                },
                false,
            )
            .await?;
            before(self)?;
            Ok(SessionOwnerOutcome::Applied {
                receipt,
                authority: None,
            })
        }
        .await;
        let result = finish(tx, result).await?;
        after(self, result)
    }
    pub(in crate::postgres) async fn pending_owner_cancel(
        &self,
        authority: &SessionOwnerAuthority,
    ) -> Result<Option<SessionOwnerCancellation>, Error> {
        validate_scope(&authority.session_id, &authority.target.id.0)?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(unknown)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(unknown)?;
        let result = async {
            super::super::deletion::ensure_live(&mut tx, &authority.session_id)
                .await
                .map_err(crate::deletion::owner_error)?;
            let record =
                reads::current(&mut tx, &authority.session_id, &authority.target.id).await?;
            record.pending_cancel(authority)
        }
        .await;
        finish(tx, result).await
    }
}
