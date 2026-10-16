//! A child is linked only while holding the original root capability.
use super::{
    Audit, Error, Ledger, MutateSessionOwner, OffsetDateTime, Publication, RevisionState,
    SessionId, SessionOwnerCommand, SessionOwnerId, SessionOwnerOperation, SessionOwnerOutcome,
};
use crate::session_owner::preparation::{
    PreparationGrant, PreparationResourceLink, PreparationResourceOutcome,
    PreparationResourceReceipt,
};
impl Ledger {
    pub(super) fn preparation_audits(
        &self,
        session: &SessionId,
        root: &SessionOwnerId,
        child: Option<&SessionOwnerId>,
    ) -> Result<Vec<Audit>, Error> {
        let selected: Vec<_> = self
            .audits
            .iter()
            .filter(|((scope, _), audit)| {
                scope == session
                    && (audit.receipt.owner.target.id == *root
                        || child == Some(&audit.receipt.owner.target.id))
                    && audit.receipt.operation != SessionOwnerOperation::Renew
            })
            .map(|(_, audit)| audit)
            .take(super::super::preparation::proof::MAX_PUBLICATIONS + 1)
            .collect();
        if selected.len() > super::super::preparation::proof::MAX_PUBLICATIONS {
            return Err(Error::Capacity);
        }
        for audit in &selected {
            audit.validate()?;
        }
        let mut audits: Vec<_> = selected.into_iter().cloned().collect();
        audits.sort_by_key(|audit| audit.record.sequence);
        Ok(audits)
    }
    pub fn retain_preparation_resources(
        &mut self,
        grant: &PreparationGrant,
        revision: RevisionState,
        now: OffsetDateTime,
    ) -> Result<PreparationResourceOutcome, Error> {
        let link = PreparationResourceLink::for_grant(grant)?;
        let request = PreparationResourceLink::reservation(grant)?;
        let ids = link.identities()?;
        if let Some(receipt) = self.preparation_resource_receipt(grant)? {
            return Ok(PreparationResourceOutcome::Replayed(Box::new(receipt)));
        }
        let root = self.checked(&link.session_id, &link.root.id)?;
        self.check_preparation_order(grant)?;
        let audits = self.preparation_audits(&link.session_id, &link.root.id, None)?;
        super::super::preparation::proof::verify_root_for_link(
            grant, root, &audits, revision, now,
        )?;
        let SessionOwnerOutcome::Applied {
            receipt: reserved,
            authority: Some(authority),
        } = self.reserve_with_source(&request, None, now, revision, None, Some(link.clone()))?
        else {
            return Err(Error::UnknownStoreOutcome);
        };
        let command = MutateSessionOwner {
            id: ids.begin,
            session_id: link.session_id.clone(),
            target: authority.target().clone(),
            command: SessionOwnerCommand::Begin,
        };
        let after = Publication::new(revision, true)?.after;
        let (SessionOwnerOutcome::Applied { receipt: began, .. }, true) =
            self.mutate_at_revision(&command, Some(&authority), now, after)?
        else {
            return Err(Error::UnknownStoreOutcome);
        };
        Ok(PreparationResourceOutcome::Applied {
            receipt: Box::new(PreparationResourceReceipt {
                link,
                reserved,
                began,
            }),
            grant: Box::new(grant.clone().with_retained_resources(authority)?),
        })
    }
    pub fn mutate_preparation_join(
        &mut self,
        proof: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        now: OffsetDateTime,
        revision: RevisionState,
    ) -> Result<(SessionOwnerOutcome, bool), Error> {
        let request = super::super::preparation::proof::join_request(proof.grant)?;
        self.mutate_with_handoff(
            &request,
            Some(proof.grant.owner_authority()),
            now,
            revision,
            Some(proof),
        )
    }
    pub fn preparation_resource_receipt(
        &self,
        grant: &PreparationGrant,
    ) -> Result<Option<PreparationResourceReceipt>, Error> {
        let link = PreparationResourceLink::for_grant(grant)?;
        let ids = link.identities()?;
        let Some(reserved) = self.audits.get(&(link.session_id.clone(), ids.reserve)) else {
            return Ok(None);
        };
        if reserved.receipt.semantic_digest
            != link.fingerprint(&PreparationResourceLink::reservation(grant)?)?
            || reserved.record.resource.as_ref() != Some(&link)
        {
            return Err(Error::SemanticConflict);
        }
        let get = |id| {
            self.audits
                .get(&(link.session_id.clone(), id))
                .ok_or(Error::UnknownStoreOutcome)
        };
        Ok(Some(super::super::preparation::resource_source::receipt(
            grant,
            reserved,
            get(ids.begin)?,
            get(link.root_reserve.clone())?,
            get(link.root_begin.clone())?,
        )?))
    }
}

#[cfg(test)]
impl Ledger {
    pub(crate) fn replace_handoff_for_test(
        &mut self,
        metadata: &crate::session_owner::preparation::PreparationReconciliation,
        target: Option<crate::session_owner::SessionOwnerTarget>,
    ) {
        self.records
            .get_mut(&(metadata.session_id.clone(), metadata.owner.id.clone()))
            .unwrap()
            .retained_resources
            .clone_from(&target);
        self.audits
            .get_mut(&(metadata.session_id.clone(), metadata.join_id.clone()))
            .unwrap()
            .record
            .retained_resources = target;
    }
}
