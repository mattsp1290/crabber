//! Source-bound original registration and current dependency-only access.
mod context;
mod metadata;
use super::{
    Audit, Error, MutateSessionOwner, Record, SessionOwnerCommand, SessionOwnerOperation,
    SqliteStore, before, fault, finish, publish_revision, reads, unknown,
};
use crate::session_owner::service::{
    RegisterSessionServiceDependency, SessionServiceDependencyAccess,
    SessionServiceDependencyOutcome,
};
impl SqliteStore {
    pub(in crate::sqlite) async fn register_service(
        &self,
        request: RegisterSessionServiceDependency,
    ) -> Result<SessionServiceDependencyOutcome, Error> {
        let source = request.source()?;
        let session = &source.root.session_id;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = Box::pin(async {
            super::super::deletion::ensure_live(&mut tx, session)
                .await
                .map_err(crate::deletion::owner_error)?;
            if let Some(receipt) = metadata::receipt(&mut tx, &request).await? {
                return Ok(SessionServiceDependencyOutcome::Replayed(Box::new(receipt)));
            }
            if reads::audit(&mut tx, session, &source.identities()?.begin)
                .await?
                .is_some()
            {
                return Err(Error::SemanticConflict);
            }
            reads::scope(&mut tx, session).await?;
            Box::pin(reads::ensure_generation_slot(
                &mut tx,
                &request.grant,
                &source,
            ))
            .await?;
            Box::pin(self.verify_service_preparation(&mut tx, &request.grant, None))
                .await?
                .require_resource_activation()?;
            let roster = Box::pin(reads::services(&mut tx, session, &source.root.root.id)).await?;
            metadata::ensure_available(&roster, &source.generation.scope)?;
            let reservation = source.reservation(&request.grant)?;
            if super::services::metadata::exists(&mut tx, session, &reservation.binding.id).await? {
                return Err(Error::SemanticConflict);
            }
            reads::capacity(&mut tx, session).await?;
            let now = self.clock.now();
            let mut record = Record::reserve(&reservation, None, now)?;
            record.service = Some(Box::new(source.clone()));
            let authority = record.grant()?;
            let reserved = record.receipt(
                reservation.id.clone(),
                source.fingerprint(&reservation)?,
                SessionOwnerOperation::Reserve,
            );
            let publication = publish_revision(&mut tx, session, true).await?;
            reads::publish(
                &mut tx,
                &Audit {
                    version: 2,
                    receipt: reserved,
                    record: record.clone(),
                    publication: Some(publication),
                },
                true,
            )
            .await?;
            let command = MutateSessionOwner {
                id: source.identities()?.begin,
                session_id: session.clone(),
                target: authority.target().clone(),
                command: SessionOwnerCommand::Begin,
            };
            record.mutate(&command, Some(&authority), now)?;
            let began = record.receipt(
                command.id.clone(),
                command.fingerprint()?,
                SessionOwnerOperation::Begin,
            );
            let publication = publish_revision(&mut tx, session, true).await?;
            reads::publish(
                &mut tx,
                &Audit {
                    version: 2,
                    receipt: began,
                    record,
                    publication: Some(publication),
                },
                false,
            )
            .await?;
            let receipt = metadata::receipt(&mut tx, &request)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            before(self)?;
            Ok(SessionServiceDependencyOutcome::Applied {
                receipt: Box::new(receipt),
                authority,
            })
        })
        .await;
        let outcome = finish(tx, result).await?;
        if matches!(outcome, SessionServiceDependencyOutcome::Applied { .. }) && fault(self) == 2 {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
    pub(in crate::sqlite) async fn check_service_access(
        &self,
        request: SessionServiceDependencyAccess,
    ) -> Result<(), Error> {
        let binding = &request.grant.original_reservation().owner.binding;
        request.source.generation.validate_scope(
            &binding.session_id,
            &binding.workspace_id,
            &binding.directory,
        )?;
        if request.source.root.session_id != binding.session_id {
            return Err(Error::IdentityMismatch);
        }
        let session = &binding.session_id;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = Box::pin(async {
            super::super::deletion::ensure_live(&mut tx, session)
                .await
                .map_err(crate::deletion::owner_error)?;
            reads::scope(&mut tx, session).await?;
            Box::pin(self.verify_service_preparation(&mut tx, &request.grant, None))
                .await?
                .require_resource_activation()?;
            let record =
                reads::current(&mut tx, session, &request.source.identities()?.owner).await?;
            if record.service.as_deref() != Some(&request.source) {
                return Err(Error::SemanticConflict);
            }
            if !request
                .authority
                .authenticates(session, &record.snapshot.target, &record.token)
            {
                return Err(Error::StaleOwner);
            }
            if record.snapshot.state != crate::session_owner::SessionOwnerState::Active
                || record.lease_until <= self.clock.now()
                || record.owned_scope.is_none()
            {
                return Err(Error::InvalidTransition);
            }
            if record.cancellation.is_some() {
                return Err(Error::CancellationPending);
            }
            Ok(())
        })
        .await;
        finish(tx, result).await
    }
}

#[cfg(test)]
mod tests;
