//! Bounded independent membership and original sources for preparation services.
use super::{Error, Ledger, Record, SessionOwnerOperation};
use crate::session_owner::preparation::{PreparationGrant, PreparationResourceLink};
use std::collections::BTreeSet;

const MAX_SERVICES: usize = crate::session_owner::service::MAX_SESSION_SERVICES_PER_PREPARATION;

impl Ledger {
    pub(super) fn preparation_services(
        &self,
        grant: &PreparationGrant,
    ) -> Result<Vec<Record>, Error> {
        let root = PreparationResourceLink::for_grant(grant)?;
        let mut ids = BTreeSet::new();
        // Original Reserve audits establish membership even when a mutable
        // current source is removed or reattached to another root.
        for ((scope, _), audit) in &self.audits {
            if scope == &root.session_id
                && audit.receipt.operation == SessionOwnerOperation::Reserve
                && audit
                    .record
                    .service
                    .as_ref()
                    .is_some_and(|link| link.root.root.id == root.root.id)
            {
                audit.validate()?;
                if audit.receipt.session_id != *scope {
                    return Err(Error::UnknownStoreOutcome);
                }
                ids.insert(audit.receipt.owner.target.id.clone());
                if ids.len() > MAX_SERVICES {
                    return Err(Error::Capacity);
                }
            }
        }
        // The reverse membership check also refuses a forged extra current row
        // which has no corresponding original immutable service reservation.
        for ((scope, id), record) in &self.records {
            if scope == &root.session_id
                && record
                    .service
                    .as_ref()
                    .is_some_and(|link| link.root.root.id == root.root.id)
            {
                crate::session_owner::identifier(&id.0, 128, false)?;
                if !ids.contains(id) {
                    return Err(Error::UnknownStoreOutcome);
                }
            }
        }
        ids.into_iter()
            .map(|id| {
                let record = self.checked(&root.session_id, &id)?;
                if record.service.as_ref().is_none_or(|link| link.root != root) {
                    return Err(Error::UnknownStoreOutcome);
                }
                // checked validates bounded current/original/latest/parent carriers
                // before this clone; unrelated owner bodies are never materialized.
                Ok(record.clone())
            })
            .collect()
    }
}

impl Ledger {
    pub fn service_receipt(
        &self,
        request: &crate::session_owner::service::RegisterSessionServiceDependency,
    ) -> Result<Option<crate::session_owner::service::SessionServiceDependencyReceipt>, Error> {
        use crate::session_owner::service::SessionServiceDependencyReceipt;
        let source = request.source()?;
        let ids = source.identities()?;
        let Some(reserved) = self
            .audits
            .get(&(source.root.session_id.clone(), ids.reserve))
        else {
            return Ok(None);
        };
        if reserved.receipt.semantic_digest
            != source.fingerprint(&source.reservation(&request.grant)?)?
            || reserved.record.service.as_deref() != Some(&source)
        {
            return Err(Error::SemanticConflict);
        }
        let get = |id| {
            self.audits
                .get(&(source.root.session_id.clone(), id))
                .ok_or(Error::UnknownStoreOutcome)
        };
        let began = get(ids.begin)?;
        super::verify_current(&began.record, reserved, began)?;
        crate::session_owner::preparation::resource_source::verify(
            &reserved.record,
            get(source.root.root_reserve.clone())?,
            get(source.root.root_begin.clone())?,
        )?;
        let receipt = SessionServiceDependencyReceipt {
            source,
            reserved: reserved.receipt.clone(),
            began: began.receipt.clone(),
        };
        receipt.verify()?;
        Ok(Some(receipt))
    }

    pub fn register_service(
        &mut self,
        request: &crate::session_owner::service::RegisterSessionServiceDependency,
        revision: super::RevisionState,
        now: time::OffsetDateTime,
    ) -> Result<crate::session_owner::service::SessionServiceDependencyOutcome, Error> {
        use crate::session_owner::service::SessionServiceDependencyOutcome;
        if let Some(receipt) = self.service_receipt(request)? {
            return Ok(SessionServiceDependencyOutcome::Replayed(Box::new(receipt)));
        }
        self.verify_preparation(&request.grant, revision, now)?
            .require_resource_activation()?;
        let source = request.source()?;
        let roster = self.preparation_services(&request.grant)?;
        if roster.len() == MAX_SERVICES {
            return Err(Error::Capacity);
        }
        if roster.iter().any(|entry| {
            entry.service.as_ref().is_some_and(|link| {
                std::mem::discriminant(&link.generation.scope)
                    == std::mem::discriminant(&source.generation.scope)
            })
        }) {
            return Err(Error::InvalidRequest);
        }
        let reservation = source.reservation(&request.grant)?;
        let super::SessionOwnerOutcome::Applied {
            authority: Some(authority),
            ..
        } = self.reserve_with_source(
            &reservation,
            None,
            now,
            revision,
            super::ReservationSource::Service(Box::new(source.clone())),
        )?
        else {
            return Err(Error::UnknownStoreOutcome);
        };
        let command = crate::session_owner::MutateSessionOwner {
            id: source.identities()?.begin,
            session_id: source.root.session_id,
            target: authority.target().clone(),
            command: crate::session_owner::SessionOwnerCommand::Begin,
        };
        self.mutate_at_revision(
            &command,
            Some(&authority),
            now,
            super::Publication::new(revision, true)?.after,
        )?;
        let receipt = self
            .service_receipt(request)?
            .ok_or(Error::UnknownStoreOutcome)?;
        Ok(SessionServiceDependencyOutcome::Applied {
            receipt: Box::new(receipt),
            authority,
        })
    }

    pub fn check_service_access(
        &self,
        request: &crate::session_owner::service::SessionServiceDependencyAccess,
        revision: super::RevisionState,
        now: time::OffsetDateTime,
    ) -> Result<(), Error> {
        self.verify_preparation(&request.grant, revision, now)?
            .require_resource_activation()?;
        let binding = &request.grant.original_reservation().owner.binding;
        request.source.generation.validate_scope(
            &binding.session_id,
            &binding.workspace_id,
            &binding.directory,
        )?;
        if request.source.root.session_id != binding.session_id {
            return Err(Error::IdentityMismatch);
        }
        let record = self.checked(&binding.session_id, &request.source.identities()?.owner)?;
        if record.service.as_deref() != Some(&request.source) {
            return Err(Error::SemanticConflict);
        }
        if !request.authority.authenticates(
            &binding.session_id,
            &record.snapshot.target,
            &record.token,
        ) {
            return Err(Error::StaleOwner);
        }
        if record.snapshot.state != crate::session_owner::SessionOwnerState::Active
            || record.lease_until <= now
            || record.owned_scope.is_none()
        {
            return Err(Error::InvalidTransition);
        }
        if record.cancellation.is_some() {
            return Err(Error::CancellationPending);
        }
        Ok(())
    }
}
