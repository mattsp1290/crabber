//! SQL ownership transactions. The session gate includes pre-session reservations.
mod parent;
mod reads;
mod run;
use super::PostgresStore;
use crate::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerAuthority, SessionOwnerCommand,
    SessionOwnerError as Error, SessionOwnerOperation, SessionOwnerOutcome,
    SessionOwnerParentAuthority, SessionOwnerReceipt, SessionOwnerSnapshot, SessionOwnerState,
    audit::{Audit, verify_current},
    record::Record,
};
use crabber_core::{SessionId, SessionOwnerId, SessionOwnerOperationId};
use sqlx::{Postgres, Transaction};
fn unknown<T>(_: T) -> Error {
    Error::UnknownStoreOutcome
}
fn operation(value: SessionOwnerOperation) -> &'static str {
    match value {
        SessionOwnerOperation::Reserve => "Reserve",
        SessionOwnerOperation::Begin => "Begin",
        SessionOwnerOperation::Renew => "Renew",
        SessionOwnerOperation::Join => "Join",
        SessionOwnerOperation::MarkUncertain => "MarkUncertain",
        SessionOwnerOperation::ReclaimUnstarted => "ReclaimUnstarted",
        SessionOwnerOperation::ResolveJoined => "ResolveJoined",
    }
}
fn state(value: SessionOwnerState) -> &'static str {
    match value {
        SessionOwnerState::ReservedUnstarted => "ReservedUnstarted",
        SessionOwnerState::Active => "Active",
        SessionOwnerState::Uncertain => "Uncertain",
        SessionOwnerState::Joined => "Joined",
    }
}
async fn finish<T>(tx: Transaction<'_, Postgres>, result: Result<T, Error>) -> Result<T, Error> {
    match result {
        Ok(value) => {
            tx.commit().await.map_err(unknown)?;
            Ok(value)
        }
        Err(error) => {
            tx.rollback().await.map_err(unknown)?;
            Err(error)
        }
    }
}
pub(super) async fn context(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    workspace: &str,
    directory: &str,
) -> Result<(), Error> {
    reads::context(tx, session, workspace, directory).await
}
impl PostgresStore {
    pub(super) async fn reserve_owner(
        &self,
        request: ReserveSessionOwner,
        parent: Option<SessionOwnerParentAuthority>,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = self.pool.begin().await.map_err(unknown)?;
        let result = async {
            super::lock_session(&mut tx, &request.binding.session_id)
                .await
                .map_err(unknown)?;
            if let Some(audit) =
                reads::audit(&mut tx, &request.binding.session_id, &request.id).await?
            {
                if audit.receipt.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(SessionOwnerOutcome::Replayed(audit.receipt));
            }
            self.reserve_fresh(&mut tx, &request, parent.as_ref(), digest)
                .await
        }
        .await;
        let result = finish(tx, result).await?;
        after(self, result)
    }
    async fn reserve_fresh(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: &ReserveSessionOwner,
        parent: Option<&SessionOwnerParentAuthority>,
        digest: crabber_core::InputFingerprint,
    ) -> Result<SessionOwnerOutcome, Error> {
        let binding = &request.binding;
        reads::scope(tx, &binding.session_id).await?;
        reads::context(
            tx,
            &binding.session_id,
            &binding.workspace_id,
            &binding.directory,
        )
        .await?;
        if let Some((session, metadata, _)) = super::management::current(tx, &binding.session_id)
            .await
            .map_err(unknown)?
        {
            session
                .ensure_identity(&binding.workspace_id, &binding.directory)
                .map_err(|_| Error::IdentityMismatch)?;
            if metadata.archived {
                return Err(Error::InvalidTransition);
            }
        }
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_current WHERE session_id=$1 AND owner_id=$2)")
            .bind(&binding.session_id.0)
            .bind(&binding.id.0)
            .fetch_one(&mut **tx)
            .await.map_err(unknown)?;
        if exists {
            return Err(Error::SemanticConflict);
        }
        reads::capacity(tx, &binding.session_id).await?;
        let now = self.clock.now();
        let witness = parent::proof(tx, request, parent, now).await?;
        let record = Record::reserve(request, witness, now)?;
        let authority = record.grant()?;
        let receipt = record.receipt(request.id.clone(), digest, SessionOwnerOperation::Reserve);
        reads::publish(
            tx,
            &Audit {
                version: 1,
                receipt: receipt.clone(),
                record,
            },
            true,
        )
        .await?;
        bump_existing(tx, &binding.session_id).await?;
        before(self)?;
        Ok(SessionOwnerOutcome::Applied {
            receipt,
            authority: Some(authority),
        })
    }
    pub(super) async fn mutate_owner(
        &self,
        request: MutateSessionOwner,
        authority: Option<SessionOwnerAuthority>,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = self.pool.begin().await.map_err(unknown)?;
        let result = async {
            super::lock_session(&mut tx, &request.session_id)
                .await
                .map_err(unknown)?;
            if let Some(audit) = reads::audit(&mut tx, &request.session_id, &request.id).await? {
                if audit.receipt.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(SessionOwnerOutcome::Replayed(audit.receipt));
            }
            reads::scope(&mut tx, &request.session_id).await?;
            let mut record =
                reads::current(&mut tx, &request.session_id, &request.target.id).await?;
            let public = record.mutate(&request, authority.as_ref(), self.clock.now())?;
            let op = match request.command {
                SessionOwnerCommand::Begin => SessionOwnerOperation::Begin,
                SessionOwnerCommand::Renew { .. } => SessionOwnerOperation::Renew,
                SessionOwnerCommand::Join { .. } => SessionOwnerOperation::Join,
                SessionOwnerCommand::MarkUncertain { .. } => SessionOwnerOperation::MarkUncertain,
                SessionOwnerCommand::ReclaimUnstarted { .. } => {
                    SessionOwnerOperation::ReclaimUnstarted
                }
                SessionOwnerCommand::ResolveJoined { .. } => SessionOwnerOperation::ResolveJoined,
            };
            let authority = if op == SessionOwnerOperation::ReclaimUnstarted {
                Some(record.grant()?)
            } else {
                None
            };
            let receipt = record.receipt(request.id.clone(), digest, op);
            reads::publish(
                &mut tx,
                &Audit {
                    version: 1,
                    receipt: receipt.clone(),
                    record,
                },
                false,
            )
            .await?;
            if public {
                bump_existing(&mut tx, &request.session_id).await?;
            }
            before(self)?;
            Ok(SessionOwnerOutcome::Applied { receipt, authority })
        }
        .await;
        let result = finish(tx, result).await?;
        after(self, result)
    }
    pub(super) async fn owner_snapshot(
        &self,
        session: &SessionId,
        id: &SessionOwnerId,
    ) -> Result<SessionOwnerSnapshot, Error> {
        validate_scope(session, &id.0)?;
        let mut tx = self.pool.begin().await.map_err(unknown)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(unknown)?;
        let result = reads::current(&mut tx, session, id)
            .await
            .map(|record| record.snapshot);
        finish(tx, result).await
    }
    pub(super) async fn owner_receipt(
        &self,
        session: &SessionId,
        id: &SessionOwnerOperationId,
    ) -> Result<Option<SessionOwnerReceipt>, Error> {
        validate_scope(session, &id.0)?;
        let mut tx = self.pool.begin().await.map_err(unknown)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(unknown)?;
        let result = reads::audit(&mut tx, session, id)
            .await
            .map(|audit| audit.map(|audit| audit.receipt));
        finish(tx, result).await
    }
}
fn validate_scope(session: &SessionId, id: &str) -> Result<(), Error> {
    crate::session_owner::validation::identifier(&session.0, 512, false)?;
    crate::session_owner::validation::identifier(id, 128, false)
}
async fn bump_existing(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<(), Error> {
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=$1)")
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if exists {
        super::bump_public_revision(tx, session)
            .await
            .map_err(unknown)?;
    }
    Ok(())
}
fn before(store: &PostgresStore) -> Result<(), Error> {
    #[cfg(test)]
    crate::session_owner_contract::process::pause("before");
    if fault(store) == 1 {
        Err(Error::UnknownStoreOutcome)
    } else {
        Ok(())
    }
}
fn after(store: &PostgresStore, result: SessionOwnerOutcome) -> Result<SessionOwnerOutcome, Error> {
    #[cfg(test)]
    if matches!(result, SessionOwnerOutcome::Applied { .. }) {
        crate::session_owner_contract::process::pause("after");
    }
    if fault(store) == 2 && matches!(result, SessionOwnerOutcome::Applied { .. }) {
        Err(Error::UnknownStoreOutcome)
    } else {
        Ok(result)
    }
}
fn fault(store: &PostgresStore) -> u8 {
    #[cfg(test)]
    {
        store
            .session_owner_fault
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    #[cfg(not(test))]
    {
        let _ = store;
        0
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod corruption;
