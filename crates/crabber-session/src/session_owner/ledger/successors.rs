//! Successor metadata is original-only; fresh Reserve/Begin share one allowance.
use super::{Audit, Error, Ledger, Publication, Record, RevisionState, SessionOwnerOperation};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::driver::{
    ExecutionDriverReceipt, RegisterSuccessorExecutionDriver, SuccessorDriverLink,
    SuccessorExecutionDriverGrant, SuccessorExecutionDriverOutcome,
    SuccessorExecutionDriverReceipt,
};

impl Ledger {
    pub(crate) fn successor_preparation(
        &self,
        root: &Audit,
        child: &Audit,
        now: time::OffsetDateTime,
        proof: &mut Proof,
    ) -> Result<(), Error> {
        for original in [root, child] {
            bounded(original, proof)?;
        }
        let session = &root.receipt.session_id;
        let load = |original: &Audit, proof: &mut Proof| {
            let id = &original.record.snapshot.target.id;
            let current = self
                .records
                .get(&(session.clone(), id.clone()))
                .ok_or(Error::UnknownStoreOutcome)?;
            // checked_for_read also corroborates an actual resource link. Meter
            // the current link's selected roots before that validator runs,
            // including a corrupt link pointing away from the expected root.
            if let Some(link) = crate::session_owner::preparation::resource_source::link(current) {
                for id in [&link.root_reserve, &link.root_begin] {
                    bounded(
                        self.source_audit(session, id)
                            .ok_or(Error::UnknownStoreOutcome)?,
                        proof,
                    )?;
                }
            }
            self.checked_for_read(session, id, current, proof)
                .map_err(|_| Error::UnknownStoreOutcome)
        };
        if load(root, proof)? != &root.record {
            return Err(Error::StaleOwner);
        }
        let current = load(child, proof)?;
        if current.cancellation.is_some() {
            return Err(Error::CancellationPending);
        }
        if current.snapshot.target != child.record.snapshot.target
            || current.snapshot.state != crate::session_owner::SessionOwnerState::Active
            || current.resource != child.record.resource
            || current.resource.is_none()
            || current.owned_scope.is_none()
            || current.lease_until <= now
        {
            return Err(Error::StaleOwner);
        }
        Ok(())
    }
    pub(crate) fn successor_receipt(
        &self,
        request: &RegisterSuccessorExecutionDriver,
        proof: &mut Proof,
    ) -> Result<Option<SuccessorExecutionDriverReceipt>, Error> {
        let ids = request.identities()?;
        let session = request.context.session_id();
        let Some(reserve) = self.source_audit(session, &ids.reserve) else {
            return Ok(None);
        };
        bounded(reserve, proof)?;
        let link = reserve
            .record
            .successor_driver
            .as_deref()
            .ok_or(Error::SemanticConflict)?;
        link.matches_request(request)?;
        let begin = self
            .source_audit(session, &ids.begin)
            .ok_or(Error::UnknownStoreOutcome)?;
        bounded(begin, proof)?;
        super::verify_current(&begin.record, reserve, begin)?;
        let worker = ExecutionDriverReceipt {
            session_id: session.clone(),
            run_id: request.fence.run_id.clone(),
            request_digest: request.fingerprint()?,
            reserve: reserve.receipt.clone(),
            begin: begin.receipt.clone(),
        };
        crate::session_owner::owned_scope::OwnedScopeSource::from_receipts(
            &worker.reserve,
            &worker.begin,
        )?;
        Ok(Some(SuccessorExecutionDriverReceipt {
            claim: request.claim.clone(),
            worker,
        }))
    }
    fn successor_capacity(&self, session: &crabber_core::SessionId) -> Result<(), Error> {
        let mut latest = std::collections::BTreeMap::new();
        for ((scope, _), audit) in &self.audits {
            if scope == session {
                let selected = latest
                    .entry(&audit.receipt.owner.target.id)
                    .or_insert(audit);
                if selected.record.sequence < audit.record.sequence {
                    *selected = audit;
                }
            }
        }
        if latest
            .values()
            .filter(|audit| {
                audit.receipt.owner.state != crate::session_owner::SessionOwnerState::Joined
            })
            .count()
            >= 1024
        {
            return Err(Error::Capacity);
        }
        Ok(())
    }
    pub(crate) fn register_successor(
        &mut self,
        request: &RegisterSuccessorExecutionDriver,
        link: &SuccessorDriverLink,
        revision: RevisionState,
        now: time::OffsetDateTime,
        proof: &mut Proof,
    ) -> Result<SuccessorExecutionDriverOutcome, Error> {
        if let Some(receipt) = self.successor_receipt(request, proof)? {
            return Ok(SuccessorExecutionDriverOutcome::Replayed(receipt));
        }
        self.validate_driver_membership(request.context.session_id(), proof)?;
        let reservation = link.reservation(request)?;
        let session = request.context.session_id();
        self.check_fresh_operation(session, &reservation.id)?;
        self.context(
            session,
            request.context.workspace_id(),
            request.context.directory(),
        )?;
        if self
            .records
            .contains_key(&(session.clone(), reservation.binding.id.clone()))
        {
            return Err(Error::SemanticConflict);
        }
        self.successor_capacity(session)?;
        let mut record = Record::reserve(&reservation, None, now)?;
        record.successor_driver = Some(Box::new(link.clone()));
        let authority = record.grant()?;
        let reserve = record.receipt(
            reservation.id.clone(),
            link.fingerprint(&reservation)?,
            SessionOwnerOperation::Reserve,
        );
        let publication = Publication::new(revision, true)?;
        bounded(
            &Audit {
                version: 2,
                receipt: reserve.clone(),
                record: record.clone(),
                publication: Some(publication),
            },
            proof,
        )?;
        self.publish(record.clone(), reserve.clone(), publication)?;
        let command = crate::session_owner::MutateSessionOwner {
            id: request.identities()?.begin,
            session_id: session.clone(),
            target: authority.target().clone(),
            command: crate::session_owner::SessionOwnerCommand::Begin,
        };
        record.mutate(&command, Some(&authority), now)?;
        let begin = record.receipt(
            command.id.clone(),
            command.fingerprint()?,
            SessionOwnerOperation::Begin,
        );
        let publication = Publication::new(publication.after, true)?;
        bounded(
            &Audit {
                version: 2,
                receipt: begin.clone(),
                record: record.clone(),
                publication: Some(publication),
            },
            proof,
        )?;
        self.publish(record, begin.clone(), publication)?;
        let receipt = SuccessorExecutionDriverReceipt {
            claim: request.claim.clone(),
            worker: ExecutionDriverReceipt {
                session_id: session.clone(),
                run_id: request.fence.run_id.clone(),
                request_digest: request.fingerprint()?,
                reserve,
                begin,
            },
        };
        let grant = SuccessorExecutionDriverGrant::from_fresh_begin(
            receipt.clone(),
            authority,
            request.clone(),
            link.identity()?,
        )?;
        Ok(SuccessorExecutionDriverOutcome::Applied {
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
