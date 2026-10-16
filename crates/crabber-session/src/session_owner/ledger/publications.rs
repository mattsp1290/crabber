//! Publication routing survives removal of current rows and private histories.
//! Read only the exact identity projection before validating current bodies.
use super::{Ledger, SessionId, SessionOwnerOperation};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
impl Ledger {
    /// A retained publication reserves its operation key even if its audit was
    /// lost. This semantic conflict precedes unrelated scope-integrity errors.
    pub(super) fn check_fresh_operation(
        &self,
        session: &SessionId,
        operation: &crabber_core::SessionOwnerOperationId,
    ) -> Result<(), super::Error> {
        if self
            .publications
            .contains_key(&(session.clone(), operation.clone()))
        {
            return Err(super::Error::SemanticConflict);
        }
        Ok(())
    }

    pub(super) fn validate_publications(
        &self,
        session: &SessionId,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        for ((scope, operation), (ordinal, owner)) in &self.publications {
            if scope != session {
                continue;
            }
            proof.measure(&(scope, operation, ordinal, owner))?;
            crate::session_owner::identifier(&scope.0, 512, false).map_err(unknown)?;
            crate::session_owner::identifier(&operation.0, 128, false).map_err(unknown)?;
            crate::session_owner::identifier(&owner.0, 128, false).map_err(unknown)?;
            let audit = self
                .audits
                .get(&(scope.clone(), operation.clone()))
                .ok_or_else(|| unknown("owner publication source missing"))?;
            proof.measure(&(
                &audit.receipt.session_id,
                &audit.receipt.operation_id,
                &audit.receipt.owner.target.id,
                &audit.record.snapshot.binding.session_id,
                &audit.record.snapshot.target.id,
                audit.receipt.operation,
                &audit.publication,
            ))?;
            if *ordinal == 0
                || *ordinal > self.next_publication
                || audit.receipt.session_id != *scope
                || audit.receipt.operation_id != *operation
                || audit.receipt.owner.target.id != *owner
                || audit.record.snapshot.binding.session_id != *scope
                || audit.record.snapshot.target.id != *owner
                || audit.receipt.operation == SessionOwnerOperation::Renew
                || audit
                    .publication
                    .is_none_or(|publication| !publication.public_change)
                || !self.records.contains_key(&(scope.clone(), owner.clone()))
            {
                return Err(unknown("owner publication routing"));
            }
        }
        // A removed publication cannot silently change the independent clock.
        for ((scope, operation), audit) in &self.audits {
            if scope != session
                || !audit
                    .publication
                    .is_some_and(|publication| publication.public_change)
            {
                continue;
            }
            proof.measure(&(
                scope,
                operation,
                &audit.receipt.owner.target.id,
                &audit.publication,
            ))?;
            crate::session_owner::identifier(&operation.0, 128, false).map_err(unknown)?;
            if !self
                .publications
                .contains_key(&(scope.clone(), operation.clone()))
            {
                return Err(unknown("owner publication missing"));
            }
        }
        Ok(())
    }
}
