//! Current root and bounded independently linked service publication proof.
use super::super::{Error, reads, revision};
use crate::public_session::v2::proof::Proof;
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
        strict: bool,
        proof: &mut Proof,
    ) -> Result<VerifiedPreparation<'a>, Error> {
        let original = grant.original_reservation();
        let session = &original.owner.binding.session_id;
        verify_owner(tx, session, &original.owner.binding.id, strict, proof).await?;
        let root = reads::current(tx, session, &original.owner.binding.id).await?;
        self.preparation_source_context(tx, original, proof).await?;
        reads::preparation_order_with_proof(tx, grant, proof).await?;
        let child = if let Some(authority) = grant.retained_resources_authority() {
            verify_owner(tx, session, &authority.target().id, strict, proof).await?;
            Some(reads::current(tx, session, &authority.target().id).await?)
        } else {
            None
        };
        let records =
            reads::services_with_proof(tx, session, &root.snapshot.target.id, proof).await?;
        let audits = reads::preparation_audits(
            tx,
            session,
            &root.snapshot.target.id,
            child.as_ref().map(|record| &record.snapshot.target.id),
            proof,
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

async fn verify_owner(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    session: &crabber_core::SessionId,
    id: &crabber_core::SessionOwnerId,
    strict: bool,
    proof: &mut Proof,
) -> Result<(), Error> {
    super::super::public::preflight(tx, session, &id.0, proof)
        .await
        .map_err(|error| match error {
            crate::public_session::v2::proof::Failure::Limited(_) => Error::Capacity,
            crate::public_session::v2::proof::Failure::Error(_) => Error::UnknownStoreOutcome,
        })?;
    if strict {
        super::super::protocol::require(tx, session, id, proof)
            .await
            .map_err(|error| error.into_domain_or(Error::Capacity))?;
    }
    Ok(())
}
