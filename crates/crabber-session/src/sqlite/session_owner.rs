//! SQL ownership transactions. The session gate includes pre-session reservations.
mod cancellation;
pub(in crate::sqlite) mod drivers;
mod owned_scope;
mod parent;
mod preflight;
mod preparation;
pub(super) use preparation::{pending_prefix, pending_raw};
pub(super) mod public;
mod reads;
pub(in crate::sqlite) use owned_scope::initial_driver_roster;
pub(in crate::sqlite) use reads::current_with_proof as current_owner_with_proof;
mod resources;
pub(super) mod run;
mod services;
use super::SqliteStore;
use crate::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerAuthority, SessionOwnerCommand,
    SessionOwnerError as Error, SessionOwnerOperation, SessionOwnerOutcome,
    SessionOwnerParentAuthority, SessionOwnerReceipt, SessionOwnerSnapshot, SessionOwnerState,
    audit::{Audit, Publication, RevisionState, verify_current},
    record::Record,
};
use crate::transaction::Transaction;
use crabber_core::{SessionId, SessionOwnerId, SessionOwnerOperationId};
use sqlx::Sqlite;
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
        SessionOwnerOperation::RequestCancel => "RequestCancel",
        SessionOwnerOperation::ObserveCancel => "ObserveCancel",
        SessionOwnerOperation::ActivateScope => "ActivateScope",
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
async fn finish<T>(tx: Transaction<'_, Sqlite>, result: Result<T, Error>) -> Result<T, Error> {
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
pub(super) async fn context_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    workspace: &str,
    directory: &str,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    reads::context_with_proof(tx, session, workspace, directory, proof).await
}
pub(super) async fn context(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    workspace: &str,
    directory: &str,
) -> Result<(), Error> {
    reads::context(tx, session, workspace, directory).await
}
impl SqliteStore {
    pub(super) async fn reserve_owner(
        &self,
        request: ReserveSessionOwner,
        parent: Option<SessionOwnerParentAuthority>,
    ) -> Result<SessionOwnerOutcome, Error> {
        Box::pin(self.reserve_owner_inner(request, parent, None)).await
    }
    pub(super) async fn reserve_preparation(
        &self,
        request: crate::session_owner::preparation::ReservePreparation,
    ) -> Result<SessionOwnerOutcome, Error> {
        request.fingerprint()?;
        Box::pin(self.reserve_owner_inner(request.owner.clone(), None, Some(request))).await
    }
    async fn reserve_owner_inner(
        &self,
        request: ReserveSessionOwner,
        parent: Option<SessionOwnerParentAuthority>,
        preparation: Option<crate::session_owner::preparation::ReservePreparation>,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = preparation.as_ref().map_or_else(
            || request.fingerprint(),
            super::super::session_owner::preparation::ReservePreparation::fingerprint,
        )?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = async {
            super::deletion::ensure_live(&mut tx, &request.binding.session_id)
                .await
                .map_err(crate::deletion::owner_error)?;
            if let Some(audit) =
                reads::audit(&mut tx, &request.binding.session_id, &request.id).await?
            {
                if audit.receipt.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(SessionOwnerOutcome::Replayed(audit.receipt));
            }
            Box::pin(self.reserve_fresh(
                &mut tx,
                &request,
                parent.as_ref(),
                digest,
                preparation.as_ref(),
            ))
            .await
        }
        .await;
        let result = finish(tx, result).await?;
        after(self, result)
    }
    async fn reserve_fresh(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        request: &ReserveSessionOwner,
        parent: Option<&SessionOwnerParentAuthority>,
        digest: crabber_core::InputFingerprint,
        preparation: Option<&crate::session_owner::preparation::ReservePreparation>,
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
        let mut read_proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        let current = if preparation.is_some_and(|value| {
            matches!(
                value.purpose,
                crate::session_owner::preparation::PreparationPurpose::Queue { .. }
            )
        }) {
            super::queue::reads::current_with_proof(tx, &binding.session_id, &mut read_proof)
                .await
                .map_err(unknown)?
        } else {
            super::management::current(tx, &binding.session_id)
                .await
                .map_err(unknown)?
        };
        if binding.parent.is_some() && current.is_none() {
            return Err(Error::UnknownStoreOutcome);
        }
        if let Some(preparation) = preparation {
            let actual = current.as_ref().map_or(
                crabber_core::SessionRevision::INITIAL,
                |(_, _, revision)| *revision,
            );
            self.preparation_preflight(tx, preparation, actual, &mut read_proof)
                .await?;
        }
        if let Some((session, metadata, _)) = current {
            session
                .ensure_identity(&binding.workspace_id, &binding.directory)
                .map_err(|_| Error::IdentityMismatch)?;
            if metadata.archived
                && binding.parent.is_none()
                && !preparation.is_some_and(|value| {
                    matches!(
                        value.purpose,
                        crate::session_owner::preparation::PreparationPurpose::ExistingRun { .. }
                    )
                })
            {
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
        let mut record = Record::reserve(request, witness, now)?;
        record.preparation = preparation.map(super::super::session_owner::preparation::origin);
        let authority = record.grant()?;
        let receipt = record.receipt(request.id.clone(), digest, SessionOwnerOperation::Reserve);
        let publication = publish_revision(tx, &binding.session_id, true).await?;
        reads::publish(
            tx,
            &Audit {
                version: 2,
                receipt: receipt.clone(),
                record,
                publication: Some(publication),
            },
            true,
        )
        .await?;
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
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = async {
            super::deletion::ensure_live(&mut tx, &request.session_id)
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
            super::expired::verify_begin(&mut tx, &record, &request, self.clock.now()).await?;
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
            let publication = publish_revision(&mut tx, &request.session_id, public).await?;
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
            Ok(SessionOwnerOutcome::Applied { receipt, authority })
        }
        .await;
        let result = finish(tx, result).await?;
        after(self, result)
    }
    pub(super) async fn preparation_metadata(
        &self,
        session: &SessionId,
        key: &crate::session_owner::preparation::PreparationKey,
    ) -> Result<Option<crate::session_owner::preparation::PreparationReconciliation>, Error> {
        validate_scope(session, "preparation")?;
        let ids = crate::session_owner::preparation::identity::ids(key, session)?;
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(unknown)?;
        super::deletion::ensure_live(&mut tx, session)
            .await
            .map_err(crate::deletion::owner_error)?;
        let result = reads::audit(&mut tx, session, &ids.reserve)
            .await?
            .map(|audit| {
                crate::session_owner::preparation::identity::metadata(&audit, session, key)
            })
            .transpose();
        finish(tx, result).await
    }
    pub(super) async fn owner_snapshot(
        &self,
        session: &SessionId,
        id: &SessionOwnerId,
    ) -> Result<SessionOwnerSnapshot, Error> {
        validate_scope(session, &id.0)?;
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(unknown)?;

        super::deletion::ensure_live(&mut tx, session)
            .await
            .map_err(crate::deletion::owner_error)?;
        let result = reads::current_with_proof(
            &mut tx,
            session,
            id,
            &mut crate::public_session::v2::proof::Proof::bounded(32 * 1024 * 1024),
        )
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
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(unknown)?;

        super::deletion::ensure_live(&mut tx, session)
            .await
            .map_err(crate::deletion::owner_error)?;
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
async fn publish_revision(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    public_change: bool,
) -> Result<Publication, Error> {
    let revision: Option<i64> =
        sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
            .bind(&session.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unknown)?;
    let before = match revision {
        Some(value) => RevisionState::Present(
            crabber_core::SessionRevision::new(value.try_into().map_err(unknown)?)
                .map_err(unknown)?,
        ),
        None => RevisionState::Absent(reads::publication_count(tx, session).await?),
    };
    let publication = Publication::new(before, public_change)?;
    if revision.is_some() && public_change {
        super::records::bump_public_revision(tx, session)
            .await
            .map_err(unknown)?;
    }
    Ok(publication)
}

fn before(store: &SqliteStore) -> Result<(), Error> {
    #[cfg(test)]
    crate::session_owner_contract::process::pause("before");
    if fault(store) == 1 {
        Err(Error::UnknownStoreOutcome)
    } else {
        Ok(())
    }
}
fn after(store: &SqliteStore, result: SessionOwnerOutcome) -> Result<SessionOwnerOutcome, Error> {
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
fn fault(store: &SqliteStore) -> u8 {
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

async fn revision(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
) -> Result<RevisionState, Error> {
    let value: Option<i64> = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&session.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?;
    match value {
        Some(value) => Ok(RevisionState::Present(
            crabber_core::SessionRevision::new(value.try_into().map_err(unknown)?)
                .map_err(unknown)?,
        )),
        None => Ok(RevisionState::Absent(
            reads::publication_count(tx, session).await?,
        )),
    }
}

#[cfg(test)]
mod cancellation_tests;

/// Original immutable bounded audit; no authority is returned.
pub(super) async fn source_audit(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &SessionOwnerOperationId,
) -> Result<Option<Audit>, Error> {
    reads::audit(tx, session, id).await
}

/// Legacy native claim entrypoints cannot pass an independently reserved preparation.
pub(super) async fn guard_native_claim(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
) -> Result<(), Error> {
    reads::scope(tx, session).await?;
    if reads::has_preparation(tx, session).await? {
        return Err(Error::Busy);
    }
    Ok(())
}

mod protocol;

pub(in crate::sqlite) mod scope_origin;

pub(super) use protocol::selected as authenticate_source_audit;

#[cfg(test)]
pub(super) use drivers::classified as classify_driver_join;

pub(super) async fn source_audit_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &SessionOwnerOperationId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<Audit>, Error> {
    reads::audit_with_proof(tx, session, id, proof).await
}

pub(in crate::sqlite) use preparation::{VerifiedTool, verify_tool};
