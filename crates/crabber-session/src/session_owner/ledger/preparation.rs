//! Bounded original preparation lineage and independent publication order.
use super::{
    BTreeMap, Error, Ledger, OffsetDateTime, RevisionState, SessionId, SessionOwnerOperation,
    SessionOwnerState,
};
impl Ledger {
    /// Borrowed owner carriers used by the existing preparation validators,
    /// before their bounded audit/service Vec copies or the inherited State clone.
    pub(crate) fn preflight_preparation(
        &self,
        grant: &crate::session_owner::preparation::PreparationGrant,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), Error> {
        let session = &grant.original_reservation().owner.binding.session_id;
        proof
            .measure(&self.contexts.get(session))
            .map_err(|_| Error::Capacity)?;
        for (key, value) in self.records.iter().filter(|((s, _), _)| s == session) {
            proof.measure(&(key, value)).map_err(|_| Error::Capacity)?;
        }
        for (key, value) in self.audits.iter().filter(|((s, _), _)| s == session) {
            proof.measure(&(key, value)).map_err(|_| Error::Capacity)?;
        }
        for (key, value) in self.publications.iter().filter(|((s, _), _)| s == session) {
            proof.measure(&(key, value)).map_err(|_| Error::Capacity)?;
        }
        proof
            .charge(
                (crate::session_owner::preparation::proof::MAX_PUBLICATIONS + 1)
                    * std::mem::size_of::<&super::Audit>(),
            )
            .map_err(|_| Error::Capacity)?;
        Ok(())
    }
    pub(crate) fn verify_preparation_with_proof<'a>(
        &self,
        grant: &'a crate::session_owner::preparation::PreparationGrant,
        revision: RevisionState,
        now: OffsetDateTime,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::session_owner::preparation::proof::VerifiedPreparation<'a>, Error> {
        self.preflight_preparation(grant, proof)?;
        self.verify_preparation(grant, revision, now)
    }
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
    #[cfg(test)]
    pub(crate) fn erase_latest_preparation_for_test(
        &mut self,
        session: &SessionId,
        begin: &crabber_core::SessionOwnerOperationId,
    ) {
        let audit = self
            .audits
            .get_mut(&(session.clone(), begin.clone()))
            .unwrap();
        assert!(audit.record.preparation.take().is_some());
    }
    pub(super) fn check_preparation_order(
        &self,
        grant: &crate::session_owner::preparation::PreparationGrant,
    ) -> Result<(), Error> {
        let request = grant.original_reservation();
        let session = &request.owner.binding.session_id;
        let services = self.preparation_services(grant)?;
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
                    && !services
                        .iter()
                        .any(|service| owner == &service.snapshot.target.id)
            })
        {
            return Err(Error::RevisionConflict);
        }
        Ok(())
    }
    pub fn has_preparation(&self, session: &SessionId) -> bool {
        if self.validate_scope(session).is_err() {
            return true;
        }
        let mut latest = BTreeMap::new();
        let mut preparation_owners = std::collections::BTreeSet::new();
        for ((scope, _), audit) in &self.audits {
            if scope == session {
                if audit.receipt.operation == SessionOwnerOperation::Reserve
                    && (audit.record.preparation.is_some()
                        || audit.receipt.owner.binding.kind
                            == super::super::SessionOwnerKind::Preparation)
                {
                    preparation_owners.insert(&audit.receipt.owner.target.id);
                }
                let entry = latest
                    .entry(&audit.receipt.owner.target.id)
                    .or_insert(audit);
                if audit.record.sequence > entry.record.sequence {
                    *entry = audit;
                }
            }
        }
        latest.values().any(|audit| {
            (audit.record.preparation.is_some()
                || preparation_owners.contains(&audit.receipt.owner.target.id))
                && (audit.receipt.owner.state != audit.record.snapshot.state
                    || audit.record.snapshot.state != SessionOwnerState::Joined
                    || !matches!(
                        audit.receipt.operation,
                        SessionOwnerOperation::Join | SessionOwnerOperation::ResolveJoined
                    ))
        })
    }
    pub fn verify_preparation<'a>(
        &self,
        grant: &'a crate::session_owner::preparation::PreparationGrant,
        revision: RevisionState,
        now: OffsetDateTime,
    ) -> Result<crate::session_owner::preparation::proof::VerifiedPreparation<'a>, Error> {
        self.verify_preparation_requirement(grant, revision, now, None)
    }
    pub fn verify_service_activation<'a>(
        &self,
        grant: &'a crate::session_owner::preparation::PreparationGrant,
        revision: RevisionState,
        now: OffsetDateTime,
        target: &crate::session_owner::SessionOwnerTarget,
    ) -> Result<crate::session_owner::preparation::proof::VerifiedPreparation<'a>, Error> {
        self.verify_preparation_requirement(grant, revision, now, Some(target))
    }
    fn verify_preparation_requirement<'a>(
        &self,
        grant: &'a crate::session_owner::preparation::PreparationGrant,
        revision: RevisionState,
        now: OffsetDateTime,
        activating: Option<&crate::session_owner::SessionOwnerTarget>,
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
        let services = self.preparation_services(grant)?;
        let services = match activating {
            Some(target) => {
                crate::session_owner::preparation::proof::ServiceRequirement::Activate {
                    records: &services,
                    target,
                }
            }
            None => crate::session_owner::preparation::proof::ServiceRequirement::Ready(&services),
        };
        crate::session_owner::preparation::proof::verify_with_services(
            grant, record, child, &audits, revision, now, services,
        )
    }
}
