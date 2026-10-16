//! Original sources select activation; mutable scope labels confer no authority.
use super::{Error, Ledger, Publication, RevisionState};
use crate::session_owner::owned_scope::{OwnedScopeActivation, source};
use crate::session_owner::{
    SessionOwnerAuthority, SessionOwnerOperation, SessionOwnerReceipt, record::Record,
};
impl Ledger {
    pub(crate) fn scope_activation_receipt(
        &self,
        request: &OwnedScopeActivation,
    ) -> Result<Option<SessionOwnerReceipt>, Error> {
        let digest = request.fingerprint()?;
        self.receipt(request.session_id(), request.operation_id())?
            .map(|receipt| {
                if receipt.semantic_digest != digest
                    || receipt.operation != SessionOwnerOperation::ActivateScope
                {
                    return Err(Error::SemanticConflict);
                }
                Ok(receipt)
            })
            .transpose()
    }
    pub(crate) fn scope_activation_record(
        &self,
        request: &OwnedScopeActivation,
    ) -> Result<Record, Error> {
        let record = self.checked(request.session_id(), &request.target().id)?;
        let reserve = self
            .audits
            .get(&(request.session_id().clone(), request.source.reserve.clone()))
            .ok_or(Error::UnknownStoreOutcome)?;
        let begin = self
            .audits
            .get(&(request.session_id().clone(), request.source.begin.clone()))
            .ok_or(Error::UnknownStoreOutcome)?;
        source::verify(&request.source, record, reserve, begin)?;
        Ok(record.clone())
    }
    pub(crate) fn activate_scope(
        &mut self,
        mut record: Record,
        request: &OwnedScopeActivation,
        authority: &SessionOwnerAuthority,
        revision: RevisionState,
        now: time::OffsetDateTime,
    ) -> Result<SessionOwnerReceipt, Error> {
        source::activate(&mut record, request, authority, now)?;
        let receipt = record.receipt(
            request.id.clone(),
            request.fingerprint()?,
            SessionOwnerOperation::ActivateScope,
        );
        self.publish(record, receipt.clone(), Publication::new(revision, true)?)?;
        Ok(receipt)
    }
    pub(crate) fn joined_scope_receipt(
        &self,
        proof: &super::super::owned_scope::JoinedOwnedScope,
    ) -> Result<Option<SessionOwnerReceipt>, Error> {
        let request = proof.request()?;
        self.receipt(&request.session_id, &request.id)?
            .map(|receipt| {
                if receipt.operation != SessionOwnerOperation::Join
                    || receipt.semantic_digest != proof.fingerprint()?
                {
                    return Err(Error::SemanticConflict);
                }
                Ok(receipt)
            })
            .transpose()
    }
    pub(crate) fn join_scope(
        &mut self,
        proof: &super::super::owned_scope::JoinedOwnedScope,
        authority: &SessionOwnerAuthority,
        revision: RevisionState,
        now: time::OffsetDateTime,
    ) -> Result<SessionOwnerReceipt, Error> {
        let request = proof.request()?;
        let mut record = self
            .checked(&request.session_id, &request.target.id)?
            .clone();
        let activation = proof.activation();
        let read = |id: &crabber_core::SessionOwnerOperationId| {
            self.audits.get(&(request.session_id.clone(), id.clone()))
        };
        source::join_candidate(
            &record,
            proof,
            read(&activation.source.reserve).ok_or(Error::UnknownStoreOutcome)?,
            read(&activation.source.begin).ok_or(Error::UnknownStoreOutcome)?,
            read(&activation.id),
        )?;
        record.mutate(&request, Some(authority), now)?;
        record.owned_join = Some(proof.inspection().clone());
        let receipt = record.receipt(
            request.id,
            proof.fingerprint()?,
            SessionOwnerOperation::Join,
        );
        self.publish(record, receipt.clone(), Publication::new(revision, true)?)?;
        Ok(receipt)
    }
    pub(super) fn measure_owned_sources(
        &self,
        record: &Record,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), crate::public_session::v2::proof::Failure> {
        use crate::public_session::v2::proof::unknown;
        let mut ids = Vec::with_capacity(5);
        if let Some(link) = &record.resource {
            ids.extend([&link.root_reserve, &link.root_begin]);
        }
        if let Some(scope) = source::context(record) {
            ids.extend([&scope.source.reserve, &scope.source.begin]);
            if record.owned_scope.is_some() {
                ids.push(&scope.id);
            }
        }
        for id in ids {
            let audit = self
                .audits
                .get(&(record.snapshot.binding.session_id.clone(), id.clone()))
                .ok_or_else(|| unknown("owner owned source absent"))?;
            proof.measure(audit)?;
        }
        Ok(())
    }
    pub(super) fn verify_scope(&self, record: &Record) -> Result<(), Error> {
        let Some(scope) = source::context(record) else {
            return Ok(());
        };
        let session = &record.snapshot.binding.session_id;
        let read = |id: &crabber_core::SessionOwnerOperationId| {
            self.audits
                .get(&(session.clone(), id.clone()))
                .ok_or(Error::UnknownStoreOutcome)
        };
        source::current(
            record,
            read(&scope.source.reserve)?,
            read(&scope.source.begin)?,
            self.audits.get(&(session.clone(), scope.id.clone())),
        )
    }
    #[cfg(test)]
    pub(crate) fn replace_scope_join_for_test(
        &mut self,
        authority: &SessionOwnerAuthority,
        source: Option<crate::session_owner::owned_scope::JoinSource>,
    ) {
        let key = (
            authority.session_id().clone(),
            authority.target().id.clone(),
        );
        self.records
            .get_mut(&key)
            .unwrap()
            .owned_join
            .clone_from(&source);
        let id = self
            .audits
            .iter()
            .filter(|((session, _), audit)| {
                session == authority.session_id()
                    && audit.record.snapshot.target == *authority.target()
            })
            .max_by_key(|(_, audit)| audit.record.sequence)
            .unwrap()
            .0
            .clone();
        self.audits.get_mut(&id).unwrap().record.owned_join = source;
    }
}
