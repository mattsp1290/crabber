//! Same-ledger cleanup publishers; public Raw Join behavior remains separate.
use super::{
    Audit, Error, Ledger, Publication, Record, RevisionState, SessionId, SessionOwnerId,
    SessionOwnerOperation, SessionOwnerReceipt,
};
use crate::native_enrollment::initial_consumer::VerifiedUnactivatedRetirement;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerAuthority, owned_scope::JoinedOwnedScope,
};
use time::OffsetDateTime;
impl Ledger {
    pub(crate) fn initial_retirement_current(
        &self,
        session: &SessionId,
        id: &SessionOwnerId,
        proof: &mut Proof,
    ) -> Result<Option<&Record>, Error> {
        self.validate_driver_membership(session, proof)?;
        self.records
            .get(&(session.clone(), id.clone()))
            .map(|record| {
                self.checked_for_read(session, id, record, proof)
                    .map_err(|_| Error::UnknownStoreOutcome)
            })
            .transpose()
    }
    /// Complete original Driver membership is authenticated before a borrowed
    /// run roster can be used to prove that this epoch has no registered owner.
    pub(crate) fn initial_retirement_drivers<'a>(
        &'a self,
        session: &SessionId,
        proof: &mut Proof,
    ) -> Result<Vec<&'a crate::session_owner::driver::DriverLink>, Error> {
        self.validate_driver_membership(session, proof)?;
        let mut owners = std::collections::BTreeSet::new();
        let mut drivers = Vec::new();
        for ((scope, _), audit) in &self.audits {
            if scope == session && audit.record.driver.is_some() {
                proof.measure(audit).map_err(|_| Error::Capacity)?;
                let id = &audit.record.snapshot.target.id;
                if !owners.contains(id) {
                    let current = self
                        .initial_retirement_current(session, id, proof)?
                        .ok_or(Error::UnknownStoreOutcome)?;
                    let driver = current
                        .driver
                        .as_deref()
                        .ok_or(Error::UnknownStoreOutcome)?;
                    proof.measure(&(id, driver)).map_err(|_| Error::Capacity)?;
                    owners.insert(id);
                    drivers.push(driver);
                }
            }
        }
        Ok(drivers)
    }
    pub(crate) fn retire_unactivated_initial(
        &mut self,
        witness: VerifiedUnactivatedRetirement<'_>,
        revision: RevisionState,
        now: OffsetDateTime,
        proof: &mut Proof,
    ) -> Result<Audit, Error> {
        let request = witness.request();
        let current = self
            .initial_retirement_current(&request.session_id, &request.target.id, proof)?
            .ok_or(Error::UnknownStoreOutcome)?;
        proof.measure(current).map_err(|_| Error::Capacity)?;
        let mut record = current.clone();
        let request = record.retire_unactivated_initial(witness, now)?;
        let digest = request.fingerprint()?;
        meter_receipt(&record, &request, &digest, proof)?;
        let receipt = record.receipt(request.id, digest, SessionOwnerOperation::Join);
        self.publish_retirement(record, receipt, revision, proof)
    }
    pub(crate) fn join_initial_scope(
        &mut self,
        joined: &JoinedOwnedScope,
        authority: &SessionOwnerAuthority,
        revision: RevisionState,
        now: OffsetDateTime,
        proof: &mut Proof,
    ) -> Result<Audit, Error> {
        let request = joined.request()?;
        let current = self
            .initial_retirement_current(&request.session_id, &request.target.id, proof)?
            .ok_or(Error::UnknownStoreOutcome)?;
        let activation = joined.activation();
        let read = |id| {
            self.source_audit(&request.session_id, id)
                .ok_or(Error::UnknownStoreOutcome)
        };
        for id in [
            &activation.source.reserve,
            &activation.source.begin,
            &activation.id,
        ] {
            if let Some(audit) = self.source_audit(&request.session_id, id) {
                proof.measure(audit).map_err(|_| Error::Capacity)?;
            }
        }
        crate::session_owner::owned_scope::source::join_candidate(
            current,
            joined,
            read(&activation.source.reserve)?,
            read(&activation.source.begin)?,
            self.source_audit(&request.session_id, &activation.id),
        )?;
        proof.measure(current).map_err(|_| Error::Capacity)?;
        let mut record = current.clone();
        record.mutate(&request, Some(authority), now)?;
        proof
            .measure(joined.inspection())
            .map_err(|_| Error::Capacity)?;
        record.owned_join = Some(joined.inspection().clone());
        let digest = joined.fingerprint()?;
        meter_receipt(&record, &request, &digest, proof)?;
        let receipt = record.receipt(request.id, digest, SessionOwnerOperation::Join);
        self.publish_retirement(record, receipt, revision, proof)
    }
    fn publish_retirement(
        &mut self,
        record: Record,
        receipt: SessionOwnerReceipt,
        revision: RevisionState,
        proof: &mut Proof,
    ) -> Result<Audit, Error> {
        // publish stores one additional independent current Record clone.
        proof.measure(&record).map_err(|_| Error::Capacity)?;
        proof
            .measure(&(
                &receipt.session_id,
                &receipt.operation_id,
                &record.snapshot.target.id,
            ))
            .map_err(|_| Error::Capacity)?;
        let session = receipt.session_id.clone();
        let id = receipt.operation_id.clone();
        self.publish(record, receipt, Publication::new(revision, true)?)?;
        let audit = self
            .source_audit(&session, &id)
            .ok_or(Error::UnknownStoreOutcome)?;
        proof.measure(audit).map_err(|_| Error::Capacity)?;
        Ok(audit.clone())
    }
}
fn meter_receipt(
    record: &Record,
    request: &MutateSessionOwner,
    digest: &crabber_core::InputFingerprint,
    proof: &mut Proof,
) -> Result<(), Error> {
    #[derive(serde::Serialize)]
    struct View<'a> {
        session_id: &'a SessionId,
        operation_id: &'a crabber_core::SessionOwnerOperationId,
        semantic_digest: &'a crabber_core::InputFingerprint,
        operation: SessionOwnerOperation,
        owner: &'a crate::session_owner::SessionOwnerSnapshot,
    }
    proof
        .measure(&View {
            session_id: &record.snapshot.binding.session_id,
            operation_id: &request.id,
            semantic_digest: digest,
            operation: SessionOwnerOperation::Join,
            owner: &record.snapshot,
        })
        .map_err(|_| Error::Capacity)
}
