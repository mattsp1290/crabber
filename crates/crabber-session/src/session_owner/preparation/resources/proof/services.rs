//! Only exact independently linked service publications enter preparation CAS.
use super::{Audit, Error, Operation, PreparationGrant, Record, SessionOwnerState};
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerTarget,
    owned_scope::OwnedScopeSource,
    service::{MAX_SESSION_SERVICES_PER_PREPARATION, SessionServiceGenerationScope},
};
use std::collections::BTreeSet;
use time::OffsetDateTime;

struct Entry<'a> {
    record: &'a Record,
    sequence: u64,
    source: OwnedScopeSource,
}
pub(super) struct Services<'a>(Vec<Entry<'a>>);
impl<'a> Services<'a> {
    pub fn new(
        grant: &PreparationGrant,
        records: &'a [Record],
        allow_unactivated: Option<&SessionOwnerTarget>,
        now: OffsetDateTime,
    ) -> Result<Self, Error> {
        if records.len() > MAX_SESSION_SERVICES_PER_PREPARATION {
            return Err(Error::Capacity);
        }
        let root = crate::session_owner::preparation::PreparationResourceLink::for_grant(grant)?;
        let binding = &grant.original_reservation().owner.binding;
        let mut scopes = BTreeSet::new();
        let mut entries = Vec::with_capacity(records.len());
        for record in records {
            record.validate()?;
            let link = record.service.as_ref().ok_or(Error::UnknownStoreOutcome)?;
            let scope = match link.generation.scope {
                SessionServiceGenerationScope::Shared => 0,
                SessionServiceGenerationScope::Workspace { .. } => 1,
                SessionServiceGenerationScope::Session { .. } => 2,
            };
            if link.root != root
                || !scopes.insert(scope)
                || record.snapshot.binding.workspace_id != binding.workspace_id
                || record.snapshot.binding.directory != binding.directory
            {
                return Err(Error::UnknownStoreOutcome);
            }
            let may_activate = allow_unactivated == Some(&record.snapshot.target);
            if record.snapshot.state != SessionOwnerState::Active
                || record.lease_until <= now
                || record.cancellation.is_some()
                || (record.owned_scope.is_none() && !may_activate)
                || (record.owned_scope.is_some() && may_activate)
            {
                return Err(Error::StaleOwner);
            }
            let ids = link.identities()?;
            let request = link.reservation(grant)?;
            if request.binding != record.snapshot.binding {
                return Err(Error::UnknownStoreOutcome);
            }
            let command = MutateSessionOwner {
                id: ids.begin.clone(),
                session_id: root.session_id.clone(),
                target: record.snapshot.target.clone(),
                command: SessionOwnerCommand::Begin,
            };
            entries.push(Entry {
                record,
                sequence: 0,
                source: OwnedScopeSource {
                    session: root.session_id.clone(),
                    target: record.snapshot.target.clone(),
                    reserve: ids.reserve,
                    reserve_digest: link.fingerprint(&request)?,
                    begin: ids.begin,
                    begin_digest: command.fingerprint()?,
                    binding: crate::session_owner::digest(&record.snapshot.binding)?,
                },
            });
        }
        if allow_unactivated.is_some_and(|target| {
            !entries
                .iter()
                .any(|entry| entry.record.snapshot.target == *target)
        }) {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(Self(entries))
    }

    pub fn advance(&mut self, audit: &Audit) -> Result<(), Error> {
        let entry = self
            .0
            .iter_mut()
            .find(|entry| entry.record.snapshot.target == audit.record.snapshot.target)
            .ok_or(Error::InvalidTransition)?;
        if audit.record.service != entry.record.service
            || audit.record.snapshot.binding != entry.record.snapshot.binding
            || audit.record.snapshot.created_at != entry.record.snapshot.created_at
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let source = &entry.source;
        match (audit.receipt.operation, entry.sequence) {
            (Operation::Reserve, 0)
                if audit.record.sequence == 1
                    && audit.receipt.operation_id == source.reserve
                    && audit.receipt.semantic_digest == source.reserve_digest => {}
            (Operation::Begin, 1)
                if audit.record.sequence == 2
                    && audit.receipt.operation_id == source.begin
                    && audit.receipt.semantic_digest == source.begin_digest => {}
            (Operation::ActivateScope, 2)
                if audit.record.sequence == 3
                    && audit.record.owned_scope == entry.record.owned_scope
                    && audit
                        .record
                        .owned_scope
                        .as_ref()
                        .is_some_and(|scope| scope.source == *source) => {}
            // Cancellation, observation, terminalization and arbitrary owner
            // operations remain outside the preparation's permitted lineage.
            _ => return Err(Error::InvalidTransition),
        }
        entry.sequence += 1;
        Ok(())
    }

    pub fn finish(&self) -> Result<(), Error> {
        if self.0.iter().any(|entry| {
            entry.sequence
                != if entry.record.owned_scope.is_some() {
                    3
                } else {
                    2
                }
        }) {
            return Err(Error::RevisionConflict);
        }
        Ok(())
    }
}
