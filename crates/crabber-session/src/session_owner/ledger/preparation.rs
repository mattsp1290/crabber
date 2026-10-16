//! Bounded original preparation lineage and independent publication order.
use super::{BTreeMap, Error, Ledger, OffsetDateTime, RevisionState, SessionId, SessionOwnerState};
impl Ledger {
    pub fn verify_preparation_join(
        &self,
        grant: &crate::session_owner::preparation::PreparationGrant,
    ) -> Result<(), Error> {
        let join = crate::session_owner::preparation::proof::join_request(grant)?;
        let source = self.audits.get(&(join.session_id, join.id));
        crate::session_owner::preparation::proof::verify_join_audit(grant, source)
    }
    pub fn preparation_metadata(
        &self,
        session: &SessionId,
        key: &crate::session_owner::preparation::PreparationKey,
    ) -> Result<Option<crate::session_owner::preparation::PreparationReconciliation>, Error> {
        let ids = crate::session_owner::preparation::identity::ids(key, session)?;
        self.audits
            .get(&(session.clone(), ids.reserve))
            .map(|audit| crate::session_owner::preparation::identity::metadata(audit, session, key))
            .transpose()
    }
    /// Immutable audit routing is the independent clock before a Session exists.
    /// No historical private record body is decoded or cloned.
    pub fn publication_count(&self, session: &SessionId) -> u64 {
        self.publications
            .keys()
            .filter(|(scope, _)| scope == session)
            .fold(0u64, |count, _| count.saturating_add(1))
    }
    #[cfg(test)]
    pub(crate) fn erase_historical_audit_for_test(
        &mut self,
        session: &SessionId,
        id: &crabber_core::SessionOwnerOperationId,
    ) {
        assert!(self.audits.remove(&(session.clone(), id.clone())).is_some());
    }
    pub(super) fn check_preparation_order(
        &self,
        grant: &crate::session_owner::preparation::PreparationGrant,
    ) -> Result<(), Error> {
        let request = grant.original_reservation();
        let session = &request.owner.binding.session_id;
        let (origin, owner) = self
            .publications
            .get(&(session.clone(), request.owner.id.clone()))
            .ok_or(Error::UnknownStoreOutcome)?;
        if owner != &request.owner.binding.id {
            return Err(Error::UnknownStoreOutcome);
        }
        if self
            .publications
            .iter()
            .any(|((scope, _), (ordinal, owner))| {
                scope == session
                    && ordinal > origin
                    && owner != &request.owner.binding.id
                    && grant
                        .retained_resources_authority()
                        .is_none_or(|child| owner != &child.target().id)
            })
        {
            return Err(Error::RevisionConflict);
        }
        Ok(())
    }
    pub fn has_preparation(&self, session: &SessionId) -> bool {
        let mut latest = BTreeMap::new();
        for ((scope, _), audit) in &self.audits {
            if scope == session {
                let entry = latest
                    .entry(&audit.receipt.owner.target.id)
                    .or_insert(audit);
                if audit.record.sequence > entry.record.sequence {
                    *entry = audit;
                }
            }
        }
        latest.values().any(|audit| {
            audit.record.preparation.is_some()
                && audit.receipt.owner.state != SessionOwnerState::Joined
        })
    }
    pub fn verify_preparation<'a>(
        &self,
        grant: &'a crate::session_owner::preparation::PreparationGrant,
        revision: RevisionState,
        now: OffsetDateTime,
    ) -> Result<crate::session_owner::preparation::proof::VerifiedPreparation<'a>, Error> {
        let session = &grant.original_reservation().owner.binding.session_id;
        let id = &grant.original_reservation().owner.binding.id;
        let record = self.checked(session, id)?;
        self.check_preparation_order(grant)?;
        let child = grant
            .retained_resources_authority()
            .map(|authority| self.checked(session, &authority.target().id))
            .transpose()?;
        let audits =
            self.preparation_audits(session, id, child.map(|value| &value.snapshot.target.id))?;
        crate::session_owner::preparation::proof::verify_with_resources(
            grant, record, child, &audits, revision, now,
        )
    }
}
