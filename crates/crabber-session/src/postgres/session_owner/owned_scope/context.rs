//! Fresh factory dispatch reauthenticates its live root and native source.
use super::super::{Error, Record, ReserveSessionOwner, parent};
use crate::session_owner::owned_scope::OwnedScopeParentAuthority;
impl crate::postgres::PostgresStore {
    pub(super) async fn owned_scope_context(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        record: &Record,
        authority: Option<&OwnedScopeParentAuthority>,
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
            return Box::pin(self.verify_service_preparation(
                tx,
                grant,
                Some(&record.snapshot.target),
            ))
            .await?
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
            Box::pin(self.verify_service_preparation(tx, grant, None)).await?;
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
        if parent::proof(tx, &request, native, self.clock.now()).await? != record.parent {
            return Err(Error::StaleOwner);
        }
        Ok(())
    }
}
