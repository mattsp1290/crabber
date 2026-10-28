//! Fresh factory dispatch reauthenticates its live root and native source.
use super::super::{Error, Record, ReserveSessionOwner, parent};
use crate::session_owner::owned_scope::OwnedScopeParentAuthority;
impl crate::postgres::PostgresStore {
    pub(super) async fn owned_scope_context(
        &self,
        tx: &mut crate::postgres::transaction::Transaction<'_, sqlx::Postgres>,
        record: &Record,
        authority: Option<&OwnedScopeParentAuthority>,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), Error> {
        if let Some(source) = &record.service {
            let Some(OwnedScopeParentAuthority::ServicePreparation(grant)) = authority else {
                return Err(Error::InvalidRequest);
            };
            if source.root
                != crate::session_owner::preparation::PreparationResourceLink::for_grant(grant)?
            {
                return Err(Error::InvalidRequest);
            }
            if record.cancellation.is_some() {
                return Err(Error::CancellationPending);
            }
            let binding = &grant.original_reservation().owner.binding;
            super::super::scope_origin::require(
                tx,
                &binding.session_id,
                &binding.workspace_id,
                &binding.directory,
                proof,
            )
            .await?;
            return Box::pin(self.verify_service_preparation(
                tx,
                grant,
                Some(&record.snapshot.target),
                true,
                proof,
            ))
            .await
            .map_err(crate::public_session::v2::proof::ReadError::owner)?
            .require_resource_activation();
        }
        if record.resource.is_some() {
            let Some(OwnedScopeParentAuthority::Preparation(grant)) = authority else {
                return Err(Error::InvalidRequest);
            };
            let original = grant.original_reservation();
            let session = &original.owner.binding.session_id;
            if grant
                .retained_resources_authority()
                .map(crate::session_owner::SessionOwnerAuthority::target)
                != Some(&record.snapshot.target)
                || *session != record.snapshot.binding.session_id
            {
                return Err(Error::InvalidRequest);
            }
            Box::pin(self.verify_service_preparation(tx, grant, None, false, proof))
                .await
                .map_err(crate::public_session::v2::proof::ReadError::owner)?;
            return Ok(());
        }
        if record.preparation.is_some() {
            return Err(Error::InvalidRequest);
        }
        let native = match authority {
            Some(OwnedScopeParentAuthority::Native(value)) => Some(value),
            None => None,
            _ => return Err(Error::InvalidRequest),
        };
        let request = ReserveSessionOwner {
            id: record.origin.clone(),
            binding: record.snapshot.binding.clone(),
            owner: record.owner.clone(),
            lease_ms: 1,
        };
        if parent::proof(tx, &request, native, self.clock.now(), proof).await? != record.parent {
            return Err(Error::StaleOwner);
        }
        Ok(())
    }
}

pub(super) async fn preflight(
    tx: &mut crate::postgres::transaction::Transaction<'_, sqlx::Postgres>,
    request: &crate::session_owner::owned_scope::OwnedScopeActivation,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    super::super::public::preflight(tx, request.session_id(), &request.target().id.0, proof)
        .await
        .map_err(|error| match error {
            crate::public_session::v2::proof::Failure::Limited(_) => Error::Capacity,
            crate::public_session::v2::proof::Failure::Error(_) => Error::UnknownStoreOutcome,
        })
}
