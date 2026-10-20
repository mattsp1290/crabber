//! A new generation cannot conceal an unresolved dependency from another root.
use super::super::{Error, Ledger, SessionOwnerOperation};
use crate::public_session::v2::proof::{Failure, Proof};
use crate::session_owner::{
    SessionOwnerKind, SessionOwnerState, preparation::PreparationGrant, service::SessionServiceLink,
};
fn failure(error: &Failure) -> Error {
    match error {
        Failure::Limited(_) => Error::Capacity,
        Failure::Error(_) => Error::UnknownStoreOutcome,
    }
}
impl Ledger {
    pub(crate) fn ensure_service_generation_slot(
        &self,
        grant: &PreparationGrant,
        source: &SessionServiceLink,
    ) -> Result<(), Error> {
        let binding = &grant.original_reservation().owner.binding;
        let session = &binding.session_id;
        let mut proof = Proof::bounded(32 * 1024 * 1024);
        self.validate_owner_membership(session)?;
        // The reverse join catches a forged extra current source; original
        // Reserve membership below also catches an erased mutable source.
        for ((scope, _), record) in &self.records {
            if scope != session && record.snapshot.binding.session_id != *session {
                continue;
            }
            if record.service.is_none()
                && record.snapshot.binding.kind != SessionOwnerKind::SessionService
            {
                continue;
            }
            proof.measure(record).map_err(|error| failure(&error))?;
            if scope != session || record.snapshot.binding.session_id != *session {
                return Err(Error::UnknownStoreOutcome);
            }
            let origin = self
                .audits
                .get(&(session.clone(), record.origin.clone()))
                .ok_or(Error::UnknownStoreOutcome)?;
            proof.measure(origin).map_err(|error| failure(&error))?;
            if origin.record.service.is_none()
                && origin.record.snapshot.binding.kind != SessionOwnerKind::SessionService
            {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        let mut live = 0;
        for ((scope, _), origin) in &self.audits {
            if origin.receipt.operation != SessionOwnerOperation::Reserve {
                continue;
            }
            if scope != session && origin.record.snapshot.binding.session_id != *session {
                continue;
            }
            if origin.record.service.is_none()
                && origin.record.snapshot.binding.kind != SessionOwnerKind::SessionService
            {
                continue;
            }
            proof.measure(origin).map_err(|error| failure(&error))?;
            if scope != session || origin.receipt.session_id != *session {
                return Err(Error::UnknownStoreOutcome);
            }
            origin.validate()?;
            let id = &origin.record.snapshot.target.id;
            let record = self
                .records
                .get(&(session.clone(), id.clone()))
                .ok_or(Error::UnknownStoreOutcome)?;
            let current = self
                .checked_for_read(session, id, record, &mut proof)
                .map_err(|error| failure(&error))?;
            let Some(link) = current.service.as_ref() else {
                return Err(Error::Busy);
            };
            if current.snapshot.binding.workspace_id != binding.workspace_id
                || current.snapshot.binding.directory != binding.directory
            {
                return Err(Error::IdentityMismatch);
            }
            // Raw Join/ResolveJoined labels are not observed task/resource joins.
            if current.snapshot.state == SessionOwnerState::Joined && current.owned_join.is_some() {
                continue;
            }
            live += 1;
            if live > crate::session_owner::service::MAX_SESSION_SERVICES_PER_PREPARATION {
                return Err(Error::Capacity);
            }
            if link.generation.scope == source.generation.scope {
                return Err(Error::Busy);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
impl Ledger {
    pub(crate) fn alter_service_for_test(
        &mut self,
        session: &crabber_core::SessionId,
        owner: &crabber_core::SessionOwnerId,
        huge: bool,
    ) {
        let record = self
            .records
            .get_mut(&(session.clone(), owner.clone()))
            .unwrap();
        if huge {
            record.owner = "x".repeat(33 * 1024 * 1024);
        } else {
            record.service = None;
        }
    }
}
