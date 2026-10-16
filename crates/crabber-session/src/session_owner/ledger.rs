//! In-memory ledger used by the native Memory adapter. Audit entries are the
//! immutable source; current rows never select their own alleged latest proof.
#[path = "ledger/cancellation.rs"]
mod cancellation;
#[path = "ledger/owned_scope.rs"]
mod owned_scope;
#[path = "ledger/public.rs"]
mod public;
#[path = "ledger/publications.rs"]
mod publications;
#[cfg(test)]
#[path = "ledger/purge_tests.rs"]
mod purge_tests;
use crate::session_owner::audit::{Audit, Publication, RevisionState, verify_current};
use crate::session_owner::record::Record;
use crate::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerAuthority, SessionOwnerCommand,
    SessionOwnerError as Error, SessionOwnerOperation, SessionOwnerOutcome, SessionOwnerReceipt,
    SessionOwnerSnapshot, SessionOwnerState,
};
use crabber_core::{SessionId, SessionOwnerId, SessionOwnerOperationId};
use std::collections::BTreeMap;
use time::OffsetDateTime;
#[derive(Clone, serde::Serialize)]
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
    // Independent append-only public publication routing; never a current-row pointer.
    publications: BTreeMap<(SessionId, SessionOwnerOperationId), (u64, SessionOwnerId)>,
    next_publication: u64,
}
impl Ledger {
    pub(crate) fn source_audit(
        &self,
        session: &SessionId,
        id: &SessionOwnerOperationId,
    ) -> Option<&Audit> {
        self.audits.get(&(session.clone(), id.clone()))
    }

    /// Physical erasure only; the caller separately establishes deletion readiness.
    /// Publication allocation remains monotonic across erased scopes.
    pub(crate) fn purge_session(
        &mut self,
        session: &SessionId,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), crate::public_session::v2::proof::Failure> {
        use crate::public_session::v2::proof::unknown;
        for ((scope, _), record) in &self.records {
            if scope == session || &record.snapshot.binding.session_id == session {
                proof.measure(record)?;
                if scope != session || record.snapshot.binding.session_id != *session {
                    return Err(unknown("owner scope"));
                }
            }
        }
        for ((scope, _), audit) in &self.audits {
            if scope == session || &audit.receipt.session_id == session {
                proof.measure(audit)?;
                if scope != session || audit.receipt.session_id != *session {
                    return Err(unknown("owner audit scope"));
                }
            }
        }
        if let Some(context) = self.contexts.get(session) {
            proof.measure(context)?;
        }
        for ((scope, operation), publication) in &self.publications {
            if scope == session {
                proof.measure(&(scope, operation, publication))?;
            }
        }
        self.validate_scope(session).map_err(unknown)?;
        self.records.retain(|(scope, _), _| scope != session);
        self.audits.retain(|(scope, _), _| scope != session);
        self.contexts.remove(session);
        self.publications.retain(|(scope, _), _| scope != session);
        Ok(())
    }
    /// A fork target must be a fresh identity, including pre-session ownership.
    /// This is a collision check, never a quiescence or deletion-readiness proof.
    pub(crate) fn contains_scope(&self, session: &SessionId) -> bool {
        self.contexts.contains_key(session)
            || self.records.keys().any(|(scope, _)| scope == session)
            || self.audits.keys().any(|(scope, _)| scope == session)
            || self.publications.keys().any(|(scope, _)| scope == session)
    }
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
        if let Some(link) = crate::session_owner::preparation::resource_source::link(record) {
            let reserve = self
                .audits
                .get(&(session.clone(), link.root_reserve.clone()))
                .ok_or(Error::UnknownStoreOutcome)?;
            let begin = self
                .audits
                .get(&(session.clone(), link.root_begin.clone()))
                .ok_or(Error::UnknownStoreOutcome)?;
            super::preparation::resource_source::verify(record, reserve, begin)?;
        }
        self.verify_scope(record)?;
        let request = record.cancellation.as_ref().map(|value| &value.request.id);
        let observation = record
            .cancellation
            .as_ref()
            .and_then(|value| value.observed.as_ref().map(|observed| &observed.request.id));
        crate::session_owner::cancellation::proof::sources(
            record,
            request.and_then(|id| self.audits.get(&(session.clone(), id.clone()))),
            observation.and_then(|id| self.audits.get(&(session.clone(), id.clone()))),
        )?;

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
        self.validate_publications(
            session,
            &mut crate::public_session::v2::proof::Proof::legacy(),
        )
        .map_err(|_| Error::UnknownStoreOutcome)?;
        self.validate_owner_membership(session)
    }
    fn validate_owner_membership(&self, session: &SessionId) -> Result<(), Error> {
        // Identity anti-joins detect a removed owner even if another forged row
        // preserves the row count. Historical private bodies are not decoded.
        for ((scope, _), audit) in &self.audits {
            if scope == session
                && !self.records.keys().any(|(record_scope, id)| {
                    record_scope == session && *id == audit.receipt.owner.target.id
                })
            {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        if self.contexts.contains_key(session)
            && !self.records.keys().any(|(scope, _)| scope == session)
        {
            return Err(Error::UnknownStoreOutcome);
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
        self.reserve_authenticated(
            request,
            None,
            now,
            RevisionState::Absent(self.publication_count(&request.binding.session_id)),
        )
    }
    #[cfg(test)]
    pub fn reserve_authenticated(
        &mut self,
        request: &ReserveSessionOwner,
        parent: Option<crate::native_parent::Witness>,
        now: OffsetDateTime,
        revision: RevisionState,
    ) -> Result<SessionOwnerOutcome, Error> {
        self.reserve_with_origin(request, parent, now, revision, None)
    }
    pub fn reserve_with_origin(
        &mut self,
        request: &ReserveSessionOwner,
        parent: Option<crate::native_parent::Witness>,
        now: OffsetDateTime,
        revision: RevisionState,
        preparation: Option<super::preparation::Origin>,
    ) -> Result<SessionOwnerOutcome, Error> {
        self.reserve_with_source(request, parent, now, revision, preparation, None)
    }
    fn reserve_with_source(
        &mut self,
        request: &ReserveSessionOwner,
        parent: Option<crate::native_parent::Witness>,
        now: OffsetDateTime,
        revision: RevisionState,
        preparation: Option<super::preparation::Origin>,
        resource: Option<super::preparation::PreparationResourceLink>,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = if let Some(resource) = &resource {
            resource.fingerprint(request)?
        } else {
            preparation.as_ref().map_or_else(
                || request.fingerprint(),
                |origin| origin.fingerprint(request),
            )?
        };
        let session = &request.binding.session_id;
        if let Some(saved) = self.receipt(session, &request.id)? {
            if saved.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            return Ok(SessionOwnerOutcome::Replayed(saved));
        }
        self.check_fresh_operation(session, &request.id)?;
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
        let mut record = Record::reserve(request, parent, now)?;
        record.preparation = preparation;
        record.resource = resource;
        let authority = record.grant()?;
        let receipt = record.receipt(request.id.clone(), digest, SessionOwnerOperation::Reserve);
        self.contexts
            .entry(session.clone())
            .or_insert_with(|| Context {
                workspace: request.binding.workspace_id.clone(),
                directory: request.binding.directory.clone(),
                origin: request.id.clone(),
            });
        self.publish(record, receipt.clone(), Publication::new(revision, true)?)?;
        Ok(SessionOwnerOutcome::Applied {
            receipt,
            authority: Some(authority),
        })
    }
    #[cfg(test)]
    pub fn mutate(
        &mut self,
        request: &MutateSessionOwner,
        authority: Option<&SessionOwnerAuthority>,
        now: OffsetDateTime,
    ) -> Result<(SessionOwnerOutcome, bool), Error> {
        self.mutate_at_revision(
            request,
            authority,
            now,
            RevisionState::Absent(self.publication_count(&request.session_id)),
        )
    }
    pub fn mutate_at_revision(
        &mut self,
        request: &MutateSessionOwner,
        authority: Option<&SessionOwnerAuthority>,
        now: OffsetDateTime,
        revision: RevisionState,
    ) -> Result<(SessionOwnerOutcome, bool), Error> {
        self.mutate_with_handoff(request, authority, now, revision, None)
    }
    fn mutate_with_handoff(
        &mut self,
        request: &MutateSessionOwner,
        authority: Option<&SessionOwnerAuthority>,
        now: OffsetDateTime,
        revision: RevisionState,
        handoff: Option<&super::preparation::proof::VerifiedPreparation<'_>>,
    ) -> Result<(SessionOwnerOutcome, bool), Error> {
        let digest = match handoff {
            Some(proof) => super::preparation::proof::join_fingerprint(proof, request)?,
            None => request.fingerprint()?,
        };
        if let Some(saved) = self.receipt(&request.session_id, &request.id)? {
            if saved.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            return Ok((SessionOwnerOutcome::Replayed(saved), false));
        }
        self.check_fresh_operation(&request.session_id, &request.id)?;
        self.validate_scope(&request.session_id)?;
        let mut record = self
            .checked(&request.session_id, &request.target.id)?
            .clone();
        let public_change = record.mutate(request, authority, now)?;
        if let Some(grant) = handoff {
            super::preparation::proof::record_handoff(&mut record, grant)?;
        }
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
        self.publish(
            record,
            receipt.clone(),
            Publication::new(revision, public_change)?,
        )?;
        Ok((
            SessionOwnerOutcome::Applied {
                receipt,
                authority: grant,
            },
            public_change,
        ))
    }
    #[cfg(test)]
    pub(crate) fn relabel_renew_for_test(
        &mut self,
        session: &SessionId,
        id: &SessionOwnerOperationId,
    ) {
        self.audits
            .get_mut(&(session.clone(), id.clone()))
            .unwrap()
            .receipt
            .operation = SessionOwnerOperation::Renew;
    }
    fn publish(
        &mut self,
        record: Record,
        receipt: SessionOwnerReceipt,
        publication: Publication,
    ) -> Result<(), Error> {
        let audit = Audit {
            version: 2,
            receipt,
            record,
            publication: Some(publication),
        };
        audit.validate()?;
        let operation = (
            audit.receipt.session_id.clone(),
            audit.receipt.operation_id.clone(),
        );
        if self.audits.contains_key(&operation) || self.publications.contains_key(&operation) {
            return Err(Error::SemanticConflict);
        }
        if publication.public_change {
            let ordinal = self
                .next_publication
                .checked_add(1)
                .ok_or(Error::UnknownStoreOutcome)?;
            self.publications.insert(
                operation.clone(),
                (ordinal, audit.record.snapshot.target.id.clone()),
            );
            self.next_publication = ordinal;
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

#[path = "ledger/resources.rs"]
mod resources;

#[path = "ledger/preparation.rs"]
mod preparation;
