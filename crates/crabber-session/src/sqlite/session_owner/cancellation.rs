//! Cancellation publishes intent and signal acknowledgement without granting work.
use super::{
    Audit, Error, SessionOwnerAuthority, SessionOwnerOperation, SessionOwnerOutcome, SqliteStore,
    after, before, finish, publish_revision, reads, revision, unknown, validate_scope,
};
use crate::session_owner::{
    ObserveSessionOwnerCancel, RequestSessionOwnerCancel, SessionOwnerCancellation,
};
impl SqliteStore {
    pub(in crate::sqlite) async fn request_owner_cancel(
        &self,
        request: RequestSessionOwnerCancel,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = async {
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
    pub(in crate::sqlite) async fn observe_owner_cancel(
        &self,
        request: ObserveSessionOwnerCancel,
        authority: SessionOwnerAuthority,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = async {
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
    pub(in crate::sqlite) async fn pending_owner_cancel(
        &self,
        authority: &SessionOwnerAuthority,
    ) -> Result<Option<SessionOwnerCancellation>, Error> {
        validate_scope(&authority.session_id, &authority.target.id.0)?;
        let mut tx = self.readers.begin().await.map_err(unknown)?;

        let result = async {
            let record =
                reads::current(&mut tx, &authority.session_id, &authority.target.id).await?;
            record.pending_cancel(authority)
        }
        .await;
        finish(tx, result).await
    }
}
