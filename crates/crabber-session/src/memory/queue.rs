use super::{MemoryStore, OperationReceipt, State};
use crate::{
    StoreError,
    queue::{
        AdmissionGuard, Audit, Change, MAX_PENDING, MAX_PENDING_BYTES, QueueAction, QueueMutation,
        QueueOutcome, QueuePage, QueuePageRequest, QueueReceipt, QueueState, QueueSummary, Record,
        RetainedQueue, apply, complete, corrupt, encode, identifier, invalid, page, scope, target,
        validate_summary, verify_audit, verify_current, verify_sources,
    },
};
use crabber_core::{ControlId, QueueId, SessionId, SessionRevision};
fn revision(state: &State, session: &SessionId) -> Result<SessionRevision, StoreError> {
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
    match state.operation_receipts.get(&(session.clone(), id.clone())) {
        None => Ok(None),
        Some(OperationReceipt::Queue(saved)) => {
            let audit = state
                .queue_audits
                .get(&(session.clone(), id.clone()))
                .ok_or_else(corrupt)?;
            verify_audit(audit, saved)?;
            Ok(Some(saved.clone()))
        }
        Some(_) => Err(StoreError::AdmissionConflict),
    }
}
fn record(state: &State, session: &SessionId, id: &QueueId) -> Result<Record, StoreError> {
    let record = state
        .queue
        .get(&(session.clone(), id.clone()))
        .ok_or(StoreError::NotFound)?
        .clone();
    record.validate(session, id, record.summary.sequence)?;
    current(state, &record.summary, &record.input.admission.key)?;
    let enqueue = source(state, session, &record.enqueue_operation)?;
    let prepare = record
        .prepare_operation
        .as_ref()
        .map(|id| source(state, session, id))
        .transpose()?;
    let claim = record
        .claim
        .as_ref()
        .map(|claim| source(state, session, &claim.operation))
        .transpose()?;
    verify_sources(&record, enqueue, prepare, claim)?;
    Ok(record)
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
fn source<'a>(
    state: &'a State,
    session: &SessionId,
    id: &ControlId,
) -> Result<&'a Audit, StoreError> {
    receipt(state, session, id)?.ok_or_else(corrupt)?;
    state
        .queue_audits
        .get(&(session.clone(), id.clone()))
        .ok_or_else(corrupt)
}
impl MemoryStore {
    pub(super) fn queue_session_snapshot(
        &self,
        id: &SessionId,
    ) -> Result<crate::SessionSummary, StoreError> {
        scope(id, &ControlId("session".into()))?;
        let state = self.state.lock().unwrap();
        let session = state.sessions.get(id).ok_or(StoreError::NotFound)?;
        let metadata = state.management.get(id).cloned().unwrap_or_default();
        crate::queue::validate_session(session, &metadata)?;
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
        Ok(record(&self.state.lock().unwrap(), session, id)?.retained())
    }
    pub(super) fn queue_get(
        &self,
        session: &SessionId,
        id: &QueueId,
    ) -> Result<QueueSummary, StoreError> {
        scope(session, &ControlId("read".into()))?;
        identifier(&id.0)?;
        let state = self.state.lock().unwrap();
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
    pub(super) fn queue_mutate(&self, request: QueueMutation) -> Result<QueueOutcome, StoreError> {
        let digest = request.fingerprint()?;
        let result = self.transact(|state| {
            if let Some(saved) = receipt(state, &request.session_id, &request.id)? {
                if saved.semantic_digest != digest {
                    return Err(StoreError::AdmissionConflict);
                }
                return Ok(QueueOutcome::Replayed(saved));
            }
            self.queue_apply(state, request, digest)
        })?;
        #[cfg(test)]
        if matches!(result, QueueOutcome::Applied { .. }) {
            crate::binding_fault::trip(&self.queue_fault, 2)?;
        }
        Ok(result)
    }
    fn queue_apply(
        &self,
        state: &mut State,
        request: QueueMutation,
        digest: crabber_core::InputFingerprint,
    ) -> Result<QueueOutcome, StoreError> {
        let now = self.clock.now();
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
        let summary = crate::management::summary(
            session,
            state
                .management
                .get(&request.session_id)
                .cloned()
                .unwrap_or_default(),
            revision,
        );
        let head = head(state, &request.session_id)?;
        let id = target(&request.action).or_else(|| head.as_ref().map(|r| &r.queue_id));
        let original = id
            .map(|id| record(state, &request.session_id, id))
            .transpose()
            .or_else(|e| {
                if matches!(request.action, QueueAction::Enqueue { .. })
                    && matches!(e, StoreError::NotFound)
                {
                    Ok(None)
                } else {
                    Err(e)
                }
            })?;
        guards(state, &request)?;
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
        let mut change = apply(&request, &summary, head.as_ref(), original, sequence, now)?;
        let admitted = self.queue_admission(state, &mut change)?;
        super::bump_public_revision(state, &request.session_id)?;
        let saved = QueueReceipt {
            session_id: request.session_id.clone(),
            operation_id: request.id.clone(),
            semantic_digest: digest,
            revision: revision_of(state, &request.session_id),
            summary: change.record.summary.clone(),
        };
        change.record.validate(
            &request.session_id,
            &saved.summary.queue_id,
            saved.summary.sequence,
        )?;
        encode(&change.record)?;
        let key = (request.session_id.clone(), request.id.clone());
        state.queue.insert(
            (request.session_id.clone(), saved.summary.queue_id.clone()),
            change.record,
        );
        state.queue_audits.insert(
            key.clone(),
            Audit {
                request,
                receipt: saved.clone(),
                authority: change.authority.clone(),
                created_at: now,
            },
        );
        state
            .operation_receipts
            .insert(key, OperationReceipt::Queue(saved.clone()));
        #[cfg(test)]
        crate::binding_fault::trip(&self.queue_fault, 1)?;
        Ok(QueueOutcome::Applied {
            receipt: saved,
            authority: change.authority,
            admitted,
        })
    }
    fn queue_admission(
        &self,
        state: &mut State,
        change: &mut Change,
    ) -> Result<Option<Box<crate::AdmitOutcome>>, StoreError> {
        if let Some(proof) = &change.admission {
            let bound = proof.bound();
            let outcome = super::admission::admit_keyed_guarded(
                state,
                &bound.keyed,
                self.clock.now(),
                AdmissionGuard::Queue(proof),
            )?;
            let crate::KeyedAdmitOutcome::Started { receipt, admitted } = outcome else {
                return Err(StoreError::AdmissionConflict);
            };
            let binding = crate::RetainedExecutionBinding::new(
                &admitted.run,
                &bound.keyed.request.user_message,
                bound.binding,
            );
            state
                .execution_bindings
                .insert(admitted.run.id.clone(), binding.encode()?);
            complete(change, &admitted, receipt);
            Ok(Some(admitted))
        } else {
            Ok(None)
        }
    }
}
fn guards(state: &State, request: &QueueMutation) -> Result<(), StoreError> {
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
        let pending: Vec<_> = state
            .queue
            .values()
            .filter(|r| {
                r.summary.session_id == request.session_id
                    && matches!(r.summary.state, QueueState::Queued | QueueState::Claimed)
            })
            .collect();
        let bytes = pending.iter().try_fold(encode(request)?.len(), |sum, r| {
            Ok::<_, StoreError>(sum + encode(r)?.len())
        })?;
        let budget = if matches!(request.action, QueueAction::Prepare { .. }) {
            MAX_PENDING_BYTES + crate::queue::MAX_RECORD
        } else {
            MAX_PENDING_BYTES
        };
        if (matches!(request.action, QueueAction::Enqueue { .. }) && pending.len() >= MAX_PENDING)
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
