//! Atomic imported history creation. No Run, phase, event or native call is copied.
use super::{MemoryStore, OperationReceipt, State};
use crate::fork::{
    Audit,
    history::{Decision, ImportedHistory},
};
use crate::{ForkBoundary, ForkError as Error, ForkOutcome, ForkReceipt, ForkSession};
use crabber_core::{ControlId, Session, SessionId, SessionRevision};

#[path = "fork/source.rs"]
mod source;

fn receipt(
    state: &State,
    target: &SessionId,
    id: &ControlId,
) -> Result<Option<ForkReceipt>, Error> {
    super::deletion::ensure_live(state, target).map_err(crate::deletion::fork_error)?;
    match state.operation_receipts.get(&(target.clone(), id.clone())) {
        None => Ok(None),
        Some(OperationReceipt::Fork(saved)) => {
            let audit = state
                .fork_audits
                .get(&(target.clone(), id.clone()))
                .ok_or(Error::UnknownStoreOutcome)?;
            audit.validate()?;
            if audit.receipt != *saved
                || saved.target.session_id != *target
                || saved.operation_id != *id
            {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(Some(saved.clone()))
        }
        Some(_) => Err(Error::SemanticConflict),
    }
}
impl MemoryStore {
    pub(super) fn fork_receipt(
        &self,
        target: &SessionId,
        id: &ControlId,
    ) -> Result<Option<ForkReceipt>, Error> {
        crate::management::validate_scope(target, id).map_err(|_| Error::InvalidRequest)?;
        receipt(&self.state.lock().unwrap(), target, id)
    }
    pub(super) fn fork(&self, request: ForkSession) -> Result<ForkOutcome, Error> {
        let digest = request.fingerprint()?;
        // Preparation borrows the existing store. A small allowance must not
        // clone unrelated sessions or private Run state before validation.
        let mut guard = self.state.lock().expect("memory store poisoned");
        let state = &mut *guard;
        {
            if let Some(saved) = receipt(state, &request.target_session_id, &request.id)? {
                if saved.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(ForkOutcome::Replayed(saved));
            }
            if !state.sessions.contains_key(&request.target_session_id)
                && (state
                    .ownership_origins
                    .contains_key(&request.target_session_id)
                    || state
                        .ownership_protocol_sessions
                        .contains(&request.target_session_id))
            {
                return Err(Error::UnknownStoreOutcome);
            }
            let source = state
                .sessions
                .get(&request.source_session_id)
                .ok_or(Error::NotFound)?;
            let revision = state
                .public_revisions
                .get(&request.source_session_id)
                .copied()
                .unwrap_or(SessionRevision::INITIAL);
            if revision != request.expected_source_revision {
                return Err(Error::RevisionConflict);
            }
            if source.id != request.source_session_id {
                return Err(Error::UnknownStoreOutcome);
            }
            if source.workspace_id != request.workspace_id || source.directory != request.directory
            {
                return Err(Error::SemanticConflict);
            }
            if state.sessions.contains_key(&request.target_session_id)
                || state
                    .session_owners
                    .contains_scope(&request.target_session_id)
            {
                return Err(Error::TargetExists);
            }
            let all = state.messages.for_session(&request.source_session_id);
            let count = match &request.boundary {
                ForkBoundary::Empty => 0,
                ForkBoundary::AfterMessage { message_id } => {
                    all.iter()
                        .position(|m| m.id == *message_id)
                        .ok_or(Error::NotFound)?
                        + 1
                }
            };
            if count > request.limits.messages {
                return Ok(ForkOutcome::Limited(crate::SnapshotLimit::Messages));
            }
            let messages = &all[..count];
            let prepared = match source::prepare(state, &request, messages) {
                Ok(prepared) => prepared,
                Err(source::Failure::Limited) => return Ok(ForkOutcome::ProofLimited),
                Err(source::Failure::Error(error)) => return Err(error),
            };
            match prepared {
                Decision::Limited(limit) => Ok(ForkOutcome::Limited(limit)),
                Decision::Unsafe(nearest_earlier) => {
                    Ok(ForkOutcome::UnsafeBoundary { nearest_earlier })
                }
                Decision::Ready(history) => {
                    publish(state, request, digest, history, self.clock.now())
                }
            }
        }
    }
}

fn publish(
    state: &mut State,
    request: ForkSession,
    digest: crabber_core::InputFingerprint,
    history: ImportedHistory,
    now: time::OffsetDateTime,
) -> Result<ForkOutcome, Error> {
    if crate::snapshot::encoded_bytes(&history, 32 * 1024 * 1024).is_none() {
        return Ok(ForkOutcome::Limited(crate::SnapshotLimit::EncodedBytes));
    }
    let session = Session {
        id: request.target_session_id.clone(),
        workspace_id: request.workspace_id.clone(),
        directory: request.directory.clone(),
        title: request.title.clone(),
        created_at: now,
        updated_at: now,
    };
    let metadata = crate::management::Metadata {
        archived: false,
        defaults: request.defaults.clone(),
    };
    let revision = SessionRevision::INITIAL
        .next()
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let saved = ForkReceipt {
        operation_id: request.id.clone(),
        semantic_digest: digest,
        source_session_id: request.source_session_id.clone(),
        source_revision: request.expected_source_revision,
        boundary: request.boundary.clone(),
        target: crate::management::summary(&session, metadata.clone(), revision),
        messages: history.messages.len(),
        imported_calls: history.calls.len(),
        history_digest: history.digest()?,
    };
    let audit = Audit {
        request,
        receipt: saved.clone(),
    };
    audit.validate()?;
    let origin = crate::deletion::origin::OwnershipOrigin::fork(&saved);
    // Every fallible check precedes the first mutation under the same lock.
    if history.calls.iter().any(|call| {
        state.calls.contains_key(&call.id) || state.imported_call_ids.contains(&call.id)
    }) {
        return Err(Error::UnknownStoreOutcome);
    }
    let target = &session.id;
    super::native_parent::source_protocol::session_birth(state, target)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    state.ownership_protocol_sessions.insert(target.clone());
    state.ownership_origins.insert(target.clone(), origin);
    for message in history.messages {
        state.messages.push(target.clone(), message);
    }
    for call in history.calls {
        state.imported_call_ids.insert(call.id.clone());
        state.imported_calls.push(target.clone(), call);
    }
    for provenance in history.provenance {
        state.imported_provenance.push(target.clone(), provenance);
    }
    state.public_revisions.insert(target.clone(), revision);
    state.snapshot_revisions.insert(target.clone(), 1);
    state.management.insert(target.clone(), metadata);
    state
        .fork_audits
        .insert((target.clone(), saved.operation_id.clone()), audit);
    state.operation_receipts.insert(
        (target.clone(), saved.operation_id.clone()),
        OperationReceipt::Fork(saved.clone()),
    );
    state.sessions.insert(target.clone(), session);
    Ok(ForkOutcome::Applied(saved))
}

#[cfg(test)]
#[path = "fork/test_support.rs"]
mod test_support;
