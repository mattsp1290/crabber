//! Fresh factory dispatch reauthenticates its live root and native source.
use super::super::{Error, Record, ReserveSessionOwner, parent, reads, revision};
use crate::session_owner::owned_scope::OwnedScopeParentAuthority;
impl crate::postgres::PostgresStore {
    pub(super) async fn owned_scope_context(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        record: &Record,
        authority: Option<&OwnedScopeParentAuthority>,
    ) -> Result<(), Error> {
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
            let root = reads::current(tx, session, &original.owner.binding.id).await?;
            self.preparation_source_context(tx, original).await?;
            reads::preparation_order(tx, grant).await?;
            let audits = reads::preparation_audits(
                tx,
                session,
                &root.snapshot.target.id,
                Some(&record.snapshot.target.id),
            )
            .await?;
            let revision = revision(tx, session).await?;
            crate::session_owner::preparation::proof::verify_with_resources(
                grant,
                &root,
                Some(record),
                &audits,
                revision,
                self.clock.now(),
            )?;
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
