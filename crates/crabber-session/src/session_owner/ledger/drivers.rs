//! Original first-driver retries never reissue authority.
use super::{Error, Ledger, RevisionState, SessionOwnerOutcome, verify_current};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::driver::{
    DriverLink, ExecutionDriverGrant, ExecutionDriverOutcome, ExecutionDriverReceipt,
    RegisterExecutionDriver,
};

impl Ledger {
    /// Authenticate independently retained publications before any fresh
    /// registration/activation can infer that cancellation or an owner is absent.
    pub(crate) fn validate_driver_membership(
        &self,
        session: &crabber_core::SessionId,
        proof: &mut Proof,
    ) -> Result<(), Error> {
        // Membership scans inspect borrowed identity projections only. Charge
        // their actual carriers too, including orphan current record keys.
        for (scope, id) in self.records.keys() {
            if scope == session {
                proof
                    .measure(&(scope, id))
                    .map_err(|_| Error::UnknownStoreOutcome)?;
            }
        }
        self.validate_publications(session, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        self.validate_owner_membership(session)
    }

    /// Original metadata lookup only. Fresh eligibility and the independently
    /// retained membership roster are checked separately before mutation.
    pub(crate) fn driver_activation_receipt_bounded(
        &self,
        request: &crate::session_owner::owned_scope::OwnedScopeActivation,
        proof: &mut Proof,
    ) -> Result<Option<crate::session_owner::SessionOwnerReceipt>, Error> {
        request.fingerprint()?;
        if let Some(audit) = self.source_audit(request.session_id(), request.operation_id()) {
            bounded(audit, proof)?;
        }
        self.scope_activation_receipt(request)
    }

    /// Borrow the coherent actual current record after charging every selected
    /// current/original/latest/context/cancel/owned-scope proof carrier. The
    /// caller retains the same allowance through fresh native eligibility.
    pub(crate) fn driver_activation_record_bounded(
        &self,
        request: &crate::session_owner::owned_scope::OwnedScopeActivation,
        proof: &mut Proof,
    ) -> Result<&crate::session_owner::record::Record, Error> {
        request.fingerprint()?;
        let session = request.session_id();
        let id = &request.target().id;
        let current = self
            .records
            .get(&(session.clone(), id.clone()))
            .ok_or(Error::UnknownStoreOutcome)?;
        let record = self
            .checked_for_read(session, id, current, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        // A supplied activation may name other original operations. Charge
        // those exact leaves before source::verify validates them too.
        let reserve = self
            .source_audit(session, &request.source.reserve)
            .ok_or(Error::UnknownStoreOutcome)?;
        let begin = self
            .source_audit(session, &request.source.begin)
            .ok_or(Error::UnknownStoreOutcome)?;
        bounded(reserve, proof)?;
        bounded(begin, proof)?;
        crate::session_owner::owned_scope::source::verify(&request.source, record, reserve, begin)?;
        Ok(record)
    }
    pub(crate) fn driver_preparation(
        &self,
        original: &crate::session_owner::audit::Audit,
        proof: &mut Proof,
    ) -> Result<(), Error> {
        bounded(original, proof)?;
        let session = &original.receipt.session_id;
        let id = &original.receipt.owner.target.id;
        let current = self
            .records
            .get(&(session.clone(), id.clone()))
            .ok_or(Error::UnknownStoreOutcome)?;
        let record = self
            .checked_for_read(session, id, current, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if record.cancellation.is_some() {
            return Err(Error::CancellationPending);
        }
        if record.snapshot.target != original.receipt.owner.target
            || record.snapshot.state != crate::session_owner::SessionOwnerState::Active
            || record.resource != original.record.resource
            || record.resource.is_none()
            || record.owned_scope.is_none()
        {
            return Err(Error::StaleOwner);
        }
        Ok(())
    }
    pub(crate) fn driver_receipt(
        &self,
        request: &RegisterExecutionDriver,
        proof: &mut Proof,
    ) -> Result<Option<ExecutionDriverReceipt>, Error> {
        let ids = request.identities()?;
        let session = request.context.session_id();
        let Some(reserve) = self.source_audit(session, &ids.reserve) else {
            return Ok(None);
        };
        bounded(reserve, proof)?;
        let source = reserve
            .record
            .driver
            .as_deref()
            .ok_or(Error::SemanticConflict)?;
        source.matches_request(request)?;
        let begin = self
            .source_audit(session, &ids.begin)
            .ok_or(Error::UnknownStoreOutcome)?;
        bounded(begin, proof)?;
        verify_current(&begin.record, reserve, begin)?;
        let receipt = ExecutionDriverReceipt {
            session_id: session.clone(),
            run_id: request.fence.run_id.clone(),
            request_digest: request.fingerprint()?,
            reserve: reserve.receipt.clone(),
            begin: begin.receipt.clone(),
        };
        crate::session_owner::owned_scope::OwnedScopeSource::from_receipts(
            &receipt.reserve,
            &receipt.begin,
        )?;
        Ok(Some(receipt))
    }
    pub(crate) fn register_driver(
        &mut self,
        request: &RegisterExecutionDriver,
        source: DriverLink,
        revision: RevisionState,
        now: time::OffsetDateTime,
        proof: &mut Proof,
    ) -> Result<ExecutionDriverOutcome, Error> {
        if let Some(receipt) = self.driver_receipt(request, proof)? {
            return Ok(ExecutionDriverOutcome::Replayed(receipt));
        }
        self.validate_driver_membership(request.context.session_id(), proof)?;
        let reservation = source.reservation(request)?;
        let source_identity = source.identity()?;
        let SessionOwnerOutcome::Applied {
            receipt: reserve,
            authority: Some(authority),
        } = self.reserve_with_source(
            &reservation,
            None,
            now,
            revision,
            super::ReservationSource::Driver(Box::new(source)),
        )?
        else {
            return Err(Error::UnknownStoreOutcome);
        };
        let begin = crate::session_owner::MutateSessionOwner {
            id: request.identities()?.begin,
            session_id: request.context.session_id().clone(),
            target: authority.target().clone(),
            command: crate::session_owner::SessionOwnerCommand::Begin,
        };
        let (SessionOwnerOutcome::Applied { receipt: begin, .. }, true) = self.mutate_at_revision(
            &begin,
            Some(&authority),
            now,
            super::Publication::new(revision, true)?.after,
        )?
        else {
            return Err(Error::UnknownStoreOutcome);
        };
        let receipt = ExecutionDriverReceipt {
            session_id: request.context.session_id().clone(),
            run_id: request.fence.run_id.clone(),
            request_digest: request.fingerprint()?,
            reserve,
            begin,
        };
        let grant = ExecutionDriverGrant::from_fresh_begin(
            receipt.clone(),
            authority,
            request.clone(),
            source_identity,
        )?;
        Ok(ExecutionDriverOutcome::Applied {
            receipt,
            grant: Box::new(grant),
        })
    }
}
fn bounded(value: &impl serde::Serialize, proof: &mut Proof) -> Result<(), Error> {
    proof
        .measure(value)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    crate::snapshot::encoded_bytes(value, 32768)
        .map(|_| ())
        .ok_or(Error::UnknownStoreOutcome)
}

#[cfg(test)]
impl Ledger {
    pub(crate) fn rollback_driver_child_cancel_for_test(
        &mut self,
        session: &crabber_core::SessionId,
        operation: &crabber_core::SessionOwnerOperationId,
    ) {
        let removed = self
            .audits
            .remove(&(session.clone(), operation.clone()))
            .unwrap();
        assert_eq!(
            removed.receipt.operation,
            crate::session_owner::SessionOwnerOperation::RequestCancel
        );
        let owner = &removed.receipt.owner.target.id;
        let previous = self
            .audits
            .values()
            .filter(|audit| {
                audit.receipt.session_id == *session
                    && audit.receipt.owner.target.id == *owner
                    && audit.record.sequence < removed.record.sequence
            })
            .max_by_key(|audit| audit.record.sequence)
            .unwrap()
            .record
            .clone();
        assert!(previous.cancellation.is_none());
        self.records
            .insert((session.clone(), owner.clone()), previous);
        assert!(
            self.publications
                .contains_key(&(session.clone(), operation.clone()))
        );
    }

    pub(crate) fn oversize_driver_child_for_test(
        &mut self,
        session: &crabber_core::SessionId,
        owner: &crabber_core::SessionOwnerId,
    ) {
        self.records
            .get_mut(&(session.clone(), owner.clone()))
            .unwrap()
            .owner = "x".repeat(crate::deletion::coverage::MAX_PROOF_BYTES + 1);
    }

    pub(crate) fn erase_driver_original_declaration_for_test(
        &mut self,
        session: &crabber_core::SessionId,
        reserve: &crabber_core::SessionOwnerOperationId,
    ) {
        let original = self
            .audits
            .get_mut(&(session.clone(), reserve.clone()))
            .unwrap();
        assert!(
            original
                .record
                .preparation
                .as_mut()
                .unwrap()
                .resource_contract
                .take()
                .is_some()
        );
    }
}
