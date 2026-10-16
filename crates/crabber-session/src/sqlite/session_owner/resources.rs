//! Independently owned resources are granted before factory dispatch.
use super::{
    Audit, Error, MutateSessionOwner, Record, SessionOwnerCommand, SessionOwnerOperation,
    SqliteStore, before, fault, finish, publish_revision, reads, revision, unknown,
};
use crate::session_owner::preparation::{
    PreparationGrant, PreparationResourceLink, PreparationResourceOutcome,
    PreparationResourceReceipt,
};
use sqlx::{Sqlite, Transaction};
impl SqliteStore {
    pub(in crate::sqlite) async fn retain_preparation_resources(
        &self,
        grant: PreparationGrant,
    ) -> Result<PreparationResourceOutcome, Error> {
        let link = PreparationResourceLink::for_grant(&grant)?;
        let request = PreparationResourceLink::reservation(&grant)?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = Box::pin(async {
            if let Some(receipt) = metadata(&mut tx, &grant).await? {
                return Ok(PreparationResourceOutcome::Replayed(Box::new(receipt)));
            }
            reads::scope(&mut tx, &link.session_id).await?;
            let root = reads::current(&mut tx, &link.session_id, &link.root.id).await?;
            reads::preparation_order(&mut tx, &grant).await?;
            let audits = reads::preparation_audits(&mut tx, &link.session_id, &link.root.id, None).await?;
            self.preparation_source_context(&mut tx, grant.original_reservation()).await?;
            let current = revision(&mut tx, &link.session_id).await?;
            let now = self.clock.now();
            crate::session_owner::preparation::proof::verify_root_for_link(&grant, &root, &audits, current, now)?;
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_current WHERE session_id=$1 AND owner_id=$2)")
                .bind(&link.session_id.0).bind(&request.binding.id.0).fetch_one(&mut *tx).await.map_err(unknown)?;
            if exists { return Err(Error::SemanticConflict); }
            reads::capacity(&mut tx, &link.session_id).await?;
            let mut record = Record::reserve(&request, None, now)?;
            record.resource = Some(link.clone());
            let authority = record.grant()?;
            let reserved = record.receipt(request.id.clone(), link.fingerprint(&request)?, SessionOwnerOperation::Reserve);
            let publication = publish_revision(&mut tx, &link.session_id, true).await?;
            reads::publish(&mut tx, &Audit {version: 2, receipt: reserved.clone(), record: record.clone(), publication: Some(publication)}, true).await?;
            let command = MutateSessionOwner {
                id: link.identities()?.begin, session_id: link.session_id.clone(),
                target: authority.target().clone(), command: SessionOwnerCommand::Begin,
            };
            record.mutate(&command, Some(&authority), now)?;
            let began = record.receipt(command.id.clone(), command.fingerprint()?, SessionOwnerOperation::Begin);
            let publication = publish_revision(&mut tx, &link.session_id, true).await?;
            reads::publish(&mut tx, &Audit {version: 2, receipt: began.clone(), record, publication: Some(publication)}, false).await?;
            #[cfg(test)]
            crate::session_owner_contract::process::pause("resources-before");
            before(self)?;
            Ok(PreparationResourceOutcome::Applied {
                receipt: Box::new(PreparationResourceReceipt {link: link.clone(), reserved, began}),
                grant: Box::new(grant.with_retained_resources(authority)?),
            })
        }).await;
        let outcome = finish(tx, result).await?;
        if matches!(outcome, PreparationResourceOutcome::Applied { .. }) {
            #[cfg(test)]
            crate::session_owner_contract::process::pause("resources-after");
            if fault(self) == 2 {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        Ok(outcome)
    }
}
async fn metadata(
    tx: &mut Transaction<'_, Sqlite>,
    grant: &PreparationGrant,
) -> Result<Option<PreparationResourceReceipt>, Error> {
    let link = PreparationResourceLink::for_grant(grant)?;
    let ids = link.identities()?;
    let Some(reserved) = reads::audit(tx, &link.session_id, &ids.reserve).await? else {
        return Ok(None);
    };
    if reserved.receipt.semantic_digest
        != link.fingerprint(&PreparationResourceLink::reservation(grant)?)?
        || reserved.record.resource.as_ref() != Some(&link)
    {
        return Err(Error::SemanticConflict);
    }
    let began = reads::audit(tx, &link.session_id, &ids.begin)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let root_reserve = reads::audit(tx, &link.session_id, &link.root_reserve)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let root_begin = reads::audit(tx, &link.session_id, &link.root_begin)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    Ok(Some(
        crate::session_owner::preparation::resource_source::receipt(
            grant,
            &reserved,
            &began,
            &root_reserve,
            &root_begin,
        )?,
    ))
}
