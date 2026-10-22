#[path = "queue/admission.rs"]
mod admission;
#[path = "queue/inspection.rs"]
mod inspection;
#[path = "queue/protocol.rs"]
pub(in crate::memory) mod protocol;
#[path = "queue/selection.rs"]
pub(in crate::memory) mod selection;
use super::{MemoryStore, OperationReceipt, State};
use crate::{
    StoreError,
    queue::{
        AdmissionGuard, Audit, Change, MAX_PENDING, MAX_PENDING_BYTES, QueueAction, QueueAuthority,
        QueueMutation, QueueOutcome, QueuePage, QueuePageRequest, QueueReceipt, QueueState,
        QueueSummary, Record, RetainedQueue, complete, corrupt, encode, identifier, invalid, page,
        scope, target, validate_summary, verify_current, verify_sources,
    },
};
use crabber_core::{ControlId, QueueId, SessionId, SessionRevision};
fn revision(state: &State, session: &SessionId) -> Result<SessionRevision, StoreError> {
    super::deletion::ensure_live(state, session)?;
    if !state.sessions.contains_key(session) {
        return Err(StoreError::NotFound);
    }
    Ok(state
        .public_revisions
        .get(session)
        .copied()
        .unwrap_or(SessionRevision::INITIAL))
}
fn receipt(
    state: &State,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<QueueReceipt>, StoreError> {
    receipt_with_proof(
        state,
        session,
        id,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
}
fn receipt_with_proof(
    state: &State,
    session: &SessionId,
    id: &ControlId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<QueueReceipt>, StoreError> {
    super::deletion::ensure_live(state, session)?;
    match state.operation_receipts.get(&(session.clone(), id.clone())) {
        None => Ok(None),
        Some(OperationReceipt::Queue(saved)) => {
            protocol::selected(state, session, id, proof).map_err(protocol::domain)?;
            Ok(Some(saved.clone()))
        }
        Some(_) => Err(StoreError::AdmissionConflict),
    }
}
fn record(state: &State, session: &SessionId, id: &QueueId) -> Result<RetainedQueue, StoreError> {
    let scope = crate::queue::source::Scope {
        session: session.clone(),
        queue: id.clone(),
    };
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
    protocol::record(state, &scope, &mut proof)
        .and_then(|loaded| loaded.retained(&mut proof))
        .map_err(protocol::domain)
}

fn current(
    state: &State,
    summary: &QueueSummary,
    admission_key: &crabber_core::AdmissionKey,
) -> Result<(), StoreError> {
    let latest = state
        .operation_receipts
        .values()
        .filter_map(|receipt| match receipt {
            OperationReceipt::Queue(receipt)
                if receipt.session_id == summary.session_id
                    && receipt.summary.queue_id == summary.queue_id =>
            {
                Some(receipt)
            }
            _ => None,
        })
        .max_by_key(|receipt| receipt.revision)
        .ok_or_else(corrupt)?;
    let actual = state
        .receipts
        .get(&(summary.session_id.clone(), admission_key.clone()));
    verify_current(summary, latest, actual)
}
fn head(state: &State, session: &SessionId) -> Result<Option<QueueSummary>, StoreError> {
    let mut head: Option<&QueueSummary> = None;
    for ((routing, id), record) in &state.queue {
        if routing != session {
            continue;
        }
        validate_summary(&record.summary, session, id, record.summary.sequence)?;
        current(state, &record.summary, &record.input.admission.key)?;
        if matches!(
            record.summary.state,
            QueueState::Queued | QueueState::Claimed
        ) && head.is_none_or(|head| record.summary.sequence < head.sequence)
        {
            head = Some(&record.summary);
        }
    }
    Ok(head.cloned())
}
impl MemoryStore {
    pub(super) fn queue_heads(
        &self,
        request: &crate::QueueHeadsRequest,
    ) -> Result<crate::QueueHeadsPage, StoreError> {
        crate::queue::validate_heads(request)?;
        let state = self.state.lock().unwrap();
        let mut heads = Vec::new();
        let mut bytes = 0usize;
        for (id, session) in &state.sessions {
            if session.workspace_id != request.workspace_id
                || request.cursor.as_ref().is_some_and(|cursor| id <= cursor)
            {
                continue;
            }
            if let Some(summary) = head(&state, id)? {
                let size = encode(&summary)?.len();
                bytes = bytes.checked_add(size).ok_or_else(corrupt)?;
                if size > 16384 || bytes > 256 << 10 {
                    return Err(corrupt());
                }
                heads.push(summary);
                if heads.len() > usize::from(request.limit) {
                    break;
                }
            }
        }
        Ok(crate::queue::heads_page(request, heads))
    }

    pub(super) fn queue_session_snapshot(
        &self,
        id: &SessionId,
    ) -> Result<crate::SessionSummary, StoreError> {
        scope(id, &ControlId("session".into()))?;
        let state = self.state.lock().unwrap();
        super::deletion::ensure_live(&state, id)?;
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        let (session, metadata) =
            protocol::context(&state, id, &mut proof).map_err(protocol::domain)?;
        let metadata = metadata.cloned().unwrap_or_default();
        Ok(crate::management::summary(
            session,
            metadata,
            revision(&state, id)?,
        ))
    }

    pub(super) fn queue_lookup(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<QueueReceipt>, StoreError> {
        scope(session, id)?;
        receipt(&self.state.lock().unwrap(), session, id)
    }
    pub(super) fn queue_private(
        &self,
        session: &SessionId,
        id: &QueueId,
    ) -> Result<RetainedQueue, StoreError> {
        scope(session, &ControlId("read".into()))?;
        identifier(&id.0)?;
        record(&self.state.lock().unwrap(), session, id)
    }
    pub(super) fn queue_get(
        &self,
        session: &SessionId,
        id: &QueueId,
    ) -> Result<QueueSummary, StoreError> {
        scope(session, &ControlId("read".into()))?;
        identifier(&id.0)?;
        let state = self.state.lock().unwrap();
        super::deletion::ensure_live(&state, session)?;
        let record = state
            .queue
            .get(&(session.clone(), id.clone()))
            .ok_or(StoreError::NotFound)?;
        let summary = &record.summary;
        current(&state, summary, &record.input.admission.key)?;
        validate_summary(summary, session, id, summary.sequence)?;
        Ok(summary.clone())
    }
    pub(super) fn queue_list(&self, request: &QueuePageRequest) -> Result<QueuePage, StoreError> {
        page(
            request,
            request
                .expected_revision
                .unwrap_or(SessionRevision::INITIAL),
            vec![],
        )?;
        let state = self.state.lock().unwrap();
        let revision = revision(&state, &request.session_id)?;
        protocol::consistent(
            &state,
            &request.session_id,
            &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
        )
        .map_err(protocol::domain)?;
        let mut selected = std::collections::BTreeMap::new();
        for ((session, id), record) in &state.queue {
            if *session != request.session_id {
                continue;
            }
            let entry = &record.summary;
            validate_summary(entry, session, id, entry.sequence)?;
            current(&state, entry, &record.input.admission.key)?;
            if entry.sequence <= request.after_sequence.unwrap_or(0) {
                continue;
            }
            selected.insert(entry.sequence, entry);
            if selected.len() > usize::from(request.limit) + 1 {
                selected.pop_last();
            }
        }
        let bytes = selected.values().try_fold(0usize, |sum, entry| {
            Ok::<_, StoreError>(sum + encode(entry)?.len())
        })?;
        if bytes > 256 << 10 {
            return Err(corrupt());
        }
        let entries = selected.into_values().cloned().collect();
        page(request, revision, entries)
    }
    pub(super) fn queue_mutate(&self, request: &QueueMutation) -> Result<QueueOutcome, StoreError> {
        self.queue_mutate_with_proof(
            request,
            &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
        )
    }
    fn queue_mutate_with_proof(
        &self,
        request: &QueueMutation,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<QueueOutcome, StoreError> {
        let digest = request.fingerprint()?;
        let mut guard = self.state.lock().expect("memory store poisoned");
        if let Some(saved) = receipt_with_proof(&guard, &request.session_id, &request.id, proof)? {
            if saved.semantic_digest != digest {
                return Err(StoreError::AdmissionConflict);
            }
            return Ok(QueueOutcome::Replayed(saved));
        }
        // The actual protected originals remain borrowed from this same locked
        // State while the inherited transaction clone is mutated separately.
        let selected = selection::select(&guard, request, proof)?;
        let mut next = guard.clone();
        let result =
            self.queue_apply_guarded(&mut next, request.clone(), digest, None, proof, selected)?;
        *guard = next;
        drop(guard);
        #[cfg(test)]
        if matches!(result, QueueOutcome::Applied { .. }) {
            crate::binding_fault::trip(&self.queue_fault, 2)?;
        }
        Ok(result)
    }
    pub(super) fn queue_apply_guarded(
        &self,
        state: &mut State,
        request: QueueMutation,
        digest: crabber_core::InputFingerprint,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
        selected: selection::Selected<'_>,
    ) -> Result<QueueOutcome, StoreError> {
        let now = self.clock.now();
        super::deletion::ensure_live(state, &request.session_id)?;
        let session = state
            .sessions
            .get(&request.session_id)
            .ok_or(StoreError::NotFound)?;
        let revision = revision(state, &request.session_id)?;
        let metadata = state
            .management
            .get(&request.session_id)
            .cloned()
            .unwrap_or_default();
        crate::queue::validate_session(session, &metadata)?;
        let summary = crate::management::summary(session, metadata, revision);
        let selection::Parts {
            head,
            original,
            previous,
            prepared,
            quota,
        } = selected.into_parts(&request)?;
        guards(state, &request, quota)?;
        let sequence = if matches!(request.action, QueueAction::Enqueue { .. }) {
            state
                .queue
                .values()
                .filter(|r| r.summary.session_id == request.session_id)
                .map(|r| r.summary.sequence)
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(invalid)?
        } else {
            0
        };
        let change = crate::queue::apply_guarded(
            &request,
            &summary,
            head.as_ref(),
            original.map(|record| (record, prepared)),
            sequence,
            now,
            preparation,
        )?;
        let outcome = self.queue_admit_and_publish(
            state,
            (request, digest, previous, now),
            change,
            preparation,
            read_proof,
        )?;
        #[cfg(test)]
        crate::binding_fault::trip(&self.queue_fault, 1)?;
        Ok(outcome)
    }
}
fn publish_queue_change(
    state: &mut State,
    operation: (
        QueueMutation,
        crabber_core::InputFingerprint,
        Option<crate::queue::source::Source>,
        time::OffsetDateTime,
    ),
    mut change: Change<'_>,
    read_proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(QueueReceipt, Option<QueueAuthority>), StoreError> {
    let (request, digest, previous, now) = operation;
    let saved = publish_queue_revision(state, &request, digest, change.record.summary.clone())?;
    let key = (request.session_id.clone(), request.id.clone());
    let audit = Audit {
        request,
        receipt: saved.clone(),
        authority: change.authority.clone(),
        created_at: now,
    };
    read_proof.measure(&audit).map_err(|failure| {
        protocol::domain(crate::public_session::v2::proof::ReadError::Budget(failure))
    })?;
    let source = if let Some(previous) = previous {
        Some(crate::queue::source::Source::new(
            &audit,
            previous.sequence.checked_add(1).ok_or_else(corrupt)?,
            Some(previous.digest),
        )?)
    } else if matches!(audit.request.action, QueueAction::Enqueue { .. }) {
        Some(crate::queue::source::Source::new(&audit, 1, None)?)
    } else {
        None
    };
    if let Some(source) = &source {
        // Both the append and its actual retained return are independent small
        // Source carriers; charge before the one publication copy.
        read_proof.measure(&(source, source)).map_err(|failure| {
            protocol::domain(crate::public_session::v2::proof::ReadError::Budget(failure))
        })?;
        state.queue_publications.publish(&audit, source.clone())?;
    }
    if matches!(audit.request.action, QueueAction::Prepare { .. }) {
        change.record.prepared = Some(crate::queue::PreparedReference::published(
            &audit,
            source.as_ref(),
            read_proof,
        )?);
    }
    change.record.validate(
        &audit.request.session_id,
        &saved.summary.queue_id,
        saved.summary.sequence,
    )?;
    let full_prepared = match &audit.request.action {
        QueueAction::Prepare { prepared, .. } => Some(prepared.as_ref()),
        _ => change
            .record
            .prepare_operation
            .as_ref()
            .map(|id| {
                let original = state
                    .queue_audits
                    .get(&(audit.request.session_id.clone(), id.clone()))
                    .ok_or_else(corrupt)?;
                let QueueAction::Prepare { prepared, .. } = &original.request.action else {
                    return Err(corrupt());
                };
                Ok(prepared.as_ref())
            })
            .transpose()?,
    };
    change.record.former_encoded_len(full_prepared)?;
    encode(&change.record)?;
    state.queue.insert(
        (
            audit.request.session_id.clone(),
            saved.summary.queue_id.clone(),
        ),
        change.record,
    );
    state.queue_audits.insert(key.clone(), audit);
    state
        .operation_receipts
        .insert(key, OperationReceipt::Queue(saved.clone()));
    Ok((saved, change.authority))
}

fn guards(
    state: &State,
    request: &QueueMutation,
    quota: Option<(usize, usize)>,
) -> Result<(), StoreError> {
    if let QueueAction::Enqueue { input, .. } = &request.action
        && (state.queue.values().any(|r| {
            r.summary.session_id == request.session_id
                && r.input.admission.key == input.admission.key
        }) || state
            .receipts
            .contains_key(&(request.session_id.clone(), input.admission.key.clone())))
    {
        return Err(StoreError::AdmissionConflict);
    }
    if matches!(
        request.action,
        QueueAction::Enqueue { .. } | QueueAction::Prepare { .. }
    ) {
        let (pending, bytes) = quota.ok_or_else(corrupt)?;
        let budget = if matches!(request.action, QueueAction::Prepare { .. }) {
            MAX_PENDING_BYTES + crate::queue::MAX_RECORD
        } else {
            MAX_PENDING_BYTES
        };
        if (matches!(request.action, QueueAction::Enqueue { .. }) && pending >= MAX_PENDING)
            || bytes > budget
        {
            return Err(StoreError::Busy);
        }
    }
    if matches!(
        request.action,
        QueueAction::Claim { .. } | QueueAction::Prepare { .. }
    ) && (super::has_pending_continuation(state, &request.session_id)?
        || state
            .runs
            .values()
            .any(|run| run.session_id == request.session_id && !run.status.is_terminal()))
    {
        return Err(StoreError::Busy);
    }
    Ok(())
}
fn revision_of(state: &State, session: &SessionId) -> SessionRevision {
    state.public_revisions[session]
}

#[cfg(test)]
#[path = "queue/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "queue/corruption_tests.rs"]
mod corruption_tests;

pub(super) fn replay(
    state: &State,
    request: &QueueMutation,
) -> Result<Option<QueueOutcome>, StoreError> {
    replay_with_proof(
        state,
        request,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
}
pub(in crate::memory) fn replay_with_proof(
    state: &State,
    request: &QueueMutation,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<QueueOutcome>, StoreError> {
    if let Some(saved) = receipt_with_proof(state, &request.session_id, &request.id, proof)? {
        if saved.semantic_digest != request.fingerprint()? {
            return Err(StoreError::AdmissionConflict);
        }
        return Ok(Some(QueueOutcome::Replayed(saved)));
    }
    Ok(None)
}
pub(super) fn preparation_context(
    state: &State,
    authority: &crate::QueueAuthority,
    intent: &crabber_core::InputFingerprint,
    now: time::OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), StoreError> {
    preflight(state, &authority.session_id, proof)?;
    let scope = crate::queue::source::Scope {
        session: authority.session_id.clone(),
        queue: authority.queue_id.clone(),
    };
    let saved = protocol::record(state, &scope, proof).map_err(protocol::domain)?;
    if !state
        .queue_publications
        .protocol(&scope, proof)
        .map_err(protocol::domain)?
    {
        return Err(StoreError::HostUnsupported);
    }
    preparation_context_selected(state, &saved, saved.prepared, authority, intent, now)
}
pub(in crate::memory) fn preparation_context_selected(
    state: &State,
    saved: &Record,
    prepared: Option<&crate::BoundKeyedAdmitRequest>,
    authority: &crate::QueueAuthority,
    intent: &crabber_core::InputFingerprint,
    now: time::OffsetDateTime,
) -> Result<(), StoreError> {
    if let Some(prepared) = prepared
        && crate::session_owner::preparation::queue_admission_intent(prepared)
            .map_err(|_| StoreError::AdmissionConflict)?
            != *intent
    {
        return Err(StoreError::AdmissionConflict);
    }
    let claim = saved.claim.as_ref().ok_or(StoreError::Conflict)?;
    if saved.summary.state != QueueState::Claimed
        || claim.authority != *authority
        || claim.expires <= now
        || head(state, &authority.session_id)?
            .is_none_or(|head| head.queue_id != authority.queue_id)
    {
        return Err(StoreError::Conflict);
    }
    if super::has_pending_continuation(state, &authority.session_id)?
        || state
            .runs
            .values()
            .any(|run| run.session_id == authority.session_id && !run.status.is_terminal())
    {
        return Err(StoreError::Busy);
    }
    Ok(())
}

/// Aggregate queue membership comes from immutable audits before current labels.
pub(super) fn public_pending(
    state: &State,
    session: &SessionId,
    limit: u16,
    safe: &mut crate::public_session::budget::Budget,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Vec<QueueSummary>, crate::public_session::v2::proof::Failure> {
    use crate::public_session::v2::proof::{Failure, unknown};
    for ((scope, _), audit) in &state.queue_audits {
        if scope == session
            && !state
                .queue
                .contains_key(&(session.clone(), audit.receipt.summary.queue_id.clone()))
        {
            return Err(unknown("queue source without current"));
        }
    }
    protocol::consistent(state, session, proof)
        .map_err(crate::public_session::v2::proof::ReadError::public)?;
    let mut result = Vec::new();
    for ((scope, id), record) in &state.queue {
        if scope != session {
            continue;
        }
        proof.measure(&record.summary)?;
        validate_summary(&record.summary, session, id, record.summary.sequence).map_err(unknown)?;
        let selected = crate::queue::source::Scope {
            session: session.clone(),
            queue: id.clone(),
        };
        let head = state
            .queue_publications
            .projected(&selected, proof, |id, proof| {
                let key = (session.clone(), id.clone());
                let audit = state.queue_audits.get(&key).ok_or_else(corrupt)?;
                let Some(OperationReceipt::Queue(saved)) = state.operation_receipts.get(&key)
                else {
                    return Err(corrupt().into());
                };
                proof
                    .measure(&(&key, &audit.receipt, saved))
                    .map_err(crate::public_session::v2::proof::ReadError::Budget)?;
                if audit.receipt != *saved
                    || saved.session_id != *session
                    || saved.operation_id != *id
                {
                    return Err(corrupt().into());
                }
                Ok(saved)
            })
            .map_err(crate::public_session::v2::proof::ReadError::public)?;

        let (key, latest) = state
            .queue_audits
            .iter()
            .filter(|((scope, _), audit)| scope == session && audit.receipt.summary.queue_id == *id)
            .max_by_key(|(_, audit)| audit.receipt.revision)
            .ok_or_else(|| unknown("queue latest"))?;
        proof.measure(&latest.receipt)?;
        let Some(OperationReceipt::Queue(saved)) = state.operation_receipts.get(key) else {
            return Err(unknown("queue source receipt"));
        };
        proof.measure(saved)?;
        if *saved != latest.receipt
            || key.1 != saved.operation_id
            || latest.request.id != saved.operation_id
            || latest.request.session_id != *session
            || saved.summary != record.summary
        {
            return Err(unknown("queue source routing"));
        }
        let actual = state
            .receipts
            .get(&(session.clone(), record.input.admission.key.clone()));
        if let Some(actual) = actual {
            proof.measure(actual)?;
        }
        if let Some(head) = head {
            head.verify_receipt(saved).map_err(unknown)?;
        }
        verify_current(&record.summary, saved, actual).map_err(unknown)?;
        if matches!(
            record.summary.state,
            QueueState::Queued | QueueState::Claimed
        ) {
            if result.len() >= usize::from(limit) {
                return Err(Failure::Limited(crate::PublicSessionLimitV2::QueueEntries));
            }
            safe.measure(&record.summary)?;
            result.push(record.summary.clone());
        }
    }
    result.sort_by_key(|summary| summary.sequence);
    if result
        .windows(2)
        .any(|rows| rows[0].sequence >= rows[1].sequence)
    {
        return Err(unknown("queue FIFO"));
    }
    Ok(result)
}

/// Selected queue evidence is precharged before the inherited whole-State
/// transaction clone. This does not bound that unrelated whole-State clone.
pub(super) fn preflight(
    state: &State,
    session: &SessionId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), StoreError> {
    protocol::context(state, session, proof).map_err(protocol::domain)?;
    let mut safe = crate::public_session::budget::Budget::new(
        32 << 20,
        crate::PublicSessionLimit::HeaderBytes,
    );
    public_pending(state, session, u16::MAX, &mut safe, proof).map_err(|_| corrupt())?;
    Ok(())
}

#[cfg(test)]
#[path = "queue/publication_tests.rs"]
mod publication_tests;

fn publish_queue_revision(
    state: &mut State,
    request: &QueueMutation,
    digest: crabber_core::InputFingerprint,
    summary: QueueSummary,
) -> Result<QueueReceipt, StoreError> {
    super::bump_public_revision(state, &request.session_id)?;
    Ok(QueueReceipt {
        session_id: request.session_id.clone(),
        operation_id: request.id.clone(),
        semantic_digest: digest,
        revision: revision_of(state, &request.session_id),
        summary,
    })
}

#[cfg(test)]
#[path = "queue/initial_selected_tests.rs"]
mod initial_selected_tests;
