//! Current root and bounded independently linked service publication proof.
use super::super::{Error, reads, revision};
use crate::session_owner::{
    SessionOwnerTarget,
    preparation::{
        PreparationGrant,
        proof::{ServiceRequirement, VerifiedPreparation},
    },
};
impl crate::sqlite::SqliteStore {
    pub(in crate::sqlite::session_owner) async fn verify_service_preparation<'a>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        grant: &'a PreparationGrant,
        activating: Option<&SessionOwnerTarget>,
    ) -> Result<VerifiedPreparation<'a>, Error> {
        let original = grant.original_reservation();
        let session = &original.owner.binding.session_id;
        let root = reads::current(tx, session, &original.owner.binding.id).await?;
        self.preparation_source_context(tx, original).await?;
        reads::preparation_order(tx, grant).await?;
        let child = if let Some(authority) = grant.retained_resources_authority() {
            Some(reads::current(tx, session, &authority.target().id).await?)
        } else {
            None
        };
        let records = reads::services(tx, session, &root.snapshot.target.id).await?;
        let audits = reads::preparation_audits(
            tx,
            session,
            &root.snapshot.target.id,
            child.as_ref().map(|record| &record.snapshot.target.id),
        )
        .await?;
        let current = revision(tx, session).await?;
        let requirement = match activating {
            Some(target) => ServiceRequirement::Activate {
                records: &records,
                target,
            },
            None => ServiceRequirement::Ready(&records),
        };
        if records.is_empty() && activating.is_none() {
            return crate::session_owner::preparation::proof::verify_with_resources(
                grant,
                &root,
                child.as_ref(),
                &audits,
                current,
                self.clock.now(),
            );
        }
        crate::session_owner::preparation::proof::verify_with_services(
            grant,
            &root,
            child.as_ref(),
            &audits,
            current,
            self.clock.now(),
            requirement,
        )
    }
}
