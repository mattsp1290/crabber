//! Source-bound original registration and current dependency-only access.
mod context;
mod metadata;
mod write;
use super::{Error, SqliteStore, before, fault, finish, reads, unknown};
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
            let mut proof = crate::public_session::v2::proof::Proof::bounded(32 * 1024 * 1024);
            if let Some(receipt) =
                metadata::receipt_with_proof(&mut tx, &request, &mut proof).await?
            {
                return Ok(SessionServiceDependencyOutcome::Replayed(Box::new(receipt)));
            }
            if reads::audit_with_proof(&mut tx, session, &source.identities()?.begin, &mut proof)
                .await?
                .is_some()
            {
                return Err(Error::SemanticConflict);
            }
            reads::scope(&mut tx, session).await?;
            Box::pin(super::scope_origin::require(
                &mut tx,
                session,
                &request
                    .grant
                    .original_reservation()
                    .owner
                    .binding
                    .workspace_id,
                &request.grant.original_reservation().owner.binding.directory,
                &mut proof,
            ))
            .await?;
            Box::pin(reads::ensure_generation_slot(
                &mut tx,
                &request.grant,
                &source,
                &mut proof,
            ))
            .await?;
            Box::pin(self.verify_service_preparation(
                &mut tx,
                &request.grant,
                None,
                true,
                &mut proof,
            ))
            .await?
            .require_resource_activation()?;
            let roster = Box::pin(reads::services_with_proof(
                &mut tx,
                session,
                &source.root.root.id,
                &mut proof,
            ))
            .await?;
            metadata::ensure_available(&roster, &source.generation.scope)?;
            let (receipt, authority) = Box::pin(write::register(
                &mut tx,
                &request,
                &source,
                self.clock.now(),
                &mut proof,
            ))
            .await?;
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
            let mut proof = crate::public_session::v2::proof::Proof::bounded(32 * 1024 * 1024);
            Box::pin(super::scope_origin::require(
                &mut tx,
                session,
                &binding.workspace_id,
                &binding.directory,
                &mut proof,
            ))
            .await?;
            Box::pin(self.verify_service_preparation(
                &mut tx,
                &request.grant,
                None,
                true,
                &mut proof,
            ))
            .await?
            .require_resource_activation()?;
            let id = request.source.identities()?.owner;
            super::public::preflight(&mut tx, session, &id.0, &mut proof)
                .await
                .map_err(|error| match error {
                    crate::public_session::v2::proof::Failure::Limited(_) => Error::Capacity,
                    crate::public_session::v2::proof::Failure::Error(_) => {
                        Error::UnknownStoreOutcome
                    }
                })?;
            super::protocol::require(&mut tx, session, &id, &mut proof)
                .await
                .map_err(|error| error.into_domain_or(Error::Capacity))?;
            let record = reads::current(&mut tx, session, &id).await?;
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
