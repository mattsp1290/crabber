//! In-memory ledger used by the native Memory adapter. Audit entries are the
//! immutable source; current rows never select their own alleged latest proof.
use crate::session_owner::audit::{Audit, verify_current};
use crate::session_owner::record::Record;
use crate::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerAuthority, SessionOwnerCommand,
    SessionOwnerError as Error, SessionOwnerOperation, SessionOwnerOutcome, SessionOwnerReceipt,
    SessionOwnerSnapshot, SessionOwnerState,
};
use crabber_core::{SessionId, SessionOwnerId, SessionOwnerOperationId};
use std::collections::BTreeMap;
use time::OffsetDateTime;
#[derive(Clone)]
struct Context {
    workspace: String,
    directory: String,
    origin: SessionOwnerOperationId,
}
#[derive(Clone, Default)]
pub(crate) struct Ledger {
    records: BTreeMap<(SessionId, SessionOwnerId), Record>,
    audits: BTreeMap<(SessionId, SessionOwnerOperationId), Audit>,
    contexts: BTreeMap<SessionId, Context>,
}
impl Ledger {
    pub fn context(
        &self,
        session: &SessionId,
        workspace: &str,
        directory: &str,
    ) -> Result<(), Error> {
        let Some(context) = self.contexts.get(session) else {
            if self.records.keys().any(|(scope, _)| scope == session)
                || self.audits.keys().any(|(scope, _)| scope == session)
            {
                return Err(Error::UnknownStoreOutcome);
            }
            return Ok(());
        };
        crate::session_owner::validation::context_text(&context.workspace, 512)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        crate::session_owner::validation::context_text(&context.directory, 4096)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        crate::session_owner::identifier(&context.origin.0, 128, false)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let origin = self
            .audits
            .get(&(session.clone(), context.origin.clone()))
            .ok_or(Error::UnknownStoreOutcome)?;
        origin.validate()?;
        if origin.receipt.operation != SessionOwnerOperation::Reserve
            || origin.receipt.session_id != *session
            || origin.receipt.operation_id != context.origin
            || origin.record.snapshot.binding.workspace_id != context.workspace
            || origin.record.snapshot.binding.directory != context.directory
        {
            return Err(Error::UnknownStoreOutcome);
        }
        if workspace != context.workspace || directory != context.directory {
            return Err(Error::IdentityMismatch);
        }
        Ok(())
    }
    pub fn receipt(
        &self,
        session: &SessionId,
        id: &SessionOwnerOperationId,
    ) -> Result<Option<SessionOwnerReceipt>, Error> {
        crate::session_owner::identifier(&session.0, 512, false)?;
        crate::session_owner::identifier(&id.0, 128, false)?;
        let Some(audit) = self.audits.get(&(session.clone(), id.clone())) else {
            return Ok(None);
        };
        audit.validate()?;
        if audit.receipt.session_id != *session || audit.receipt.operation_id != *id {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(Some(audit.receipt.clone()))
    }
    fn checked(&self, session: &SessionId, id: &SessionOwnerId) -> Result<&Record, Error> {
        crate::session_owner::identifier(&session.0, 512, false)?;
        crate::session_owner::identifier(&id.0, 128, false)?;
        let record = self
            .records
            .get(&(session.clone(), id.clone()))
            .ok_or_else(|| {
                if self.audits.values().any(|audit| {
                    audit.receipt.session_id == *session && audit.receipt.owner.target.id == *id
                }) {
                    Error::UnknownStoreOutcome
                } else {
                    Error::NotFound
                }
            })?;
        record.validate()?;
        if record.snapshot.binding.session_id != *session || record.snapshot.target.id != *id {
            return Err(Error::UnknownStoreOutcome);
        }
        // Borrow-only scan in the test store; SQL selects indexed maximum in the
        // same transaction. No current pointer or label can hide a later audit.
        let latest = self
            .audits
            .values()
            .filter(|audit| {
                audit.receipt.session_id == *session && audit.receipt.owner.target.id == *id
            })
            .max_by_key(|audit| audit.record.sequence)
            .ok_or(Error::UnknownStoreOutcome)?;
        let origin = self
            .audits
            .get(&(session.clone(), record.origin.clone()))
            .ok_or(Error::UnknownStoreOutcome)?;
        verify_current(record, origin, latest)?;
        self.context(
            session,
            &record.snapshot.binding.workspace_id,
            &record.snapshot.binding.directory,
        )?;
        Ok(record)
    }
    pub fn snapshot(
        &self,
        session: &SessionId,
        id: &SessionOwnerId,
    ) -> Result<SessionOwnerSnapshot, Error> {
        Ok(self.checked(session, id)?.snapshot.clone())
    }
    fn validate_scope(&self, session: &SessionId) -> Result<(), Error> {
        // Identity anti-joins detect a removed owner even if another forged row
        // preserves the row count. Historical private bodies are not decoded.
        for ((scope, _), audit) in &self.audits {
            if scope == session
                && audit.receipt.operation == SessionOwnerOperation::Reserve
                && !self.records.keys().any(|(record_scope, id)| {
                    record_scope == session && *id == audit.receipt.owner.target.id
                })
            {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        for (scope, id) in self.records.keys().filter(|(scope, _)| scope == session) {
            if !self.audits.iter().any(|((audit_scope, _), audit)| {
                audit_scope == scope
                    && audit.receipt.operation == SessionOwnerOperation::Reserve
                    && audit.receipt.owner.target.id == *id
            }) {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        Ok(())
    }
    #[cfg(test)]
    pub fn reserve(
        &mut self,
        request: &ReserveSessionOwner,
        now: OffsetDateTime,
    ) -> Result<SessionOwnerOutcome, Error> {
        self.reserve_authenticated(request, None, now)
    }
    pub fn reserve_authenticated(
        &mut self,
        request: &ReserveSessionOwner,
        parent: Option<crate::native_parent::Witness>,
        now: OffsetDateTime,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let session = &request.binding.session_id;
        if let Some(saved) = self.receipt(session, &request.id)? {
            if saved.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            return Ok(SessionOwnerOutcome::Replayed(saved));
        }
        self.validate_scope(session)?;
        self.context(
            session,
            &request.binding.workspace_id,
            &request.binding.directory,
        )?;
        if self
            .records
            .contains_key(&(session.clone(), request.binding.id.clone()))
        {
            return Err(Error::SemanticConflict);
        }
        // Capacity comes from each owner's independently latest immutable state,
        // never a mutable Joined label or a full scan of historical private bodies.
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
        if latest
            .values()
            .filter(|audit| audit.receipt.owner.state != SessionOwnerState::Joined)
            .count()
            >= 1024
        {
            return Err(Error::Capacity);
        }
        let record = Record::reserve(request, parent, now)?;
        let authority = record.grant()?;
        let receipt = record.receipt(request.id.clone(), digest, SessionOwnerOperation::Reserve);
        self.contexts
            .entry(session.clone())
            .or_insert_with(|| Context {
                workspace: request.binding.workspace_id.clone(),
                directory: request.binding.directory.clone(),
                origin: request.id.clone(),
            });
        self.publish(record, receipt.clone())?;
        Ok(SessionOwnerOutcome::Applied {
            receipt,
            authority: Some(authority),
        })
    }
    pub fn mutate(
        &mut self,
        request: &MutateSessionOwner,
        authority: Option<&SessionOwnerAuthority>,
        now: OffsetDateTime,
    ) -> Result<(SessionOwnerOutcome, bool), Error> {
        let digest = request.fingerprint()?;
        if let Some(saved) = self.receipt(&request.session_id, &request.id)? {
            if saved.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            return Ok((SessionOwnerOutcome::Replayed(saved), false));
        }
        self.validate_scope(&request.session_id)?;
        let mut record = self
            .checked(&request.session_id, &request.target.id)?
            .clone();
        let public_change = record.mutate(request, authority, now)?;
        let operation = match request.command {
            SessionOwnerCommand::Begin => SessionOwnerOperation::Begin,
            SessionOwnerCommand::Renew { .. } => SessionOwnerOperation::Renew,
            SessionOwnerCommand::Join { .. } => SessionOwnerOperation::Join,
            SessionOwnerCommand::MarkUncertain { .. } => SessionOwnerOperation::MarkUncertain,
            SessionOwnerCommand::ReclaimUnstarted { .. } => SessionOwnerOperation::ReclaimUnstarted,
            SessionOwnerCommand::ResolveJoined { .. } => SessionOwnerOperation::ResolveJoined,
        };
        let grant = if operation == SessionOwnerOperation::ReclaimUnstarted {
            Some(record.grant()?)
        } else {
            None
        };
        let receipt = record.receipt(request.id.clone(), digest, operation);
        self.publish(record, receipt.clone())?;
        Ok((
            SessionOwnerOutcome::Applied {
                receipt,
                authority: grant,
            },
            public_change,
        ))
    }
    fn publish(&mut self, record: Record, receipt: SessionOwnerReceipt) -> Result<(), Error> {
        let audit = Audit {
            version: 1,
            receipt,
            record,
        };
        audit.validate()?;
        let operation = (
            audit.receipt.session_id.clone(),
            audit.receipt.operation_id.clone(),
        );
        if self.audits.contains_key(&operation) {
            return Err(Error::SemanticConflict);
        }
        self.records.insert(
            (
                audit.receipt.session_id.clone(),
                audit.record.snapshot.target.id.clone(),
            ),
            audit.record.clone(),
        );
        self.audits.insert(operation, audit);
        Ok(())
    }
}

#[cfg(test)]
#[path = "ledger_tests.rs"]
mod tests;
