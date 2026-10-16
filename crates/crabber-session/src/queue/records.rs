use crate::queue::{
    BoundKeyedAdmitRequest, ControlId, Deserialize, QueueAction, QueueAuthority, QueueId,
    QueueInput, QueueMutation, QueuePage, QueuePageRequest, QueueReceipt, QueueState, QueueSummary,
    RetainedQueue, Serialize, SessionId, SessionRevision, StoreError, corrupt, invalid,
};
use time::OffsetDateTime;
pub(crate) const MAX_RECORD: usize = 32 << 20;
pub(crate) const MAX_PENDING: usize = 64;
pub(crate) const MAX_PENDING_BYTES: usize = 32 << 20;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    pub enqueue_operation: ControlId,
    pub prepare_operation: Option<ControlId>,
    pub summary: QueueSummary,
    pub input: QueueInput,
    pub claim: Option<Claim>,
    pub prepared: Option<BoundKeyedAdmitRequest>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Claim {
    pub operation: ControlId,
    pub authority: QueueAuthority,
    pub owner: String,
    pub expires: OffsetDateTime,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Audit {
    pub request: QueueMutation,
    pub receipt: QueueReceipt,
    pub authority: Option<QueueAuthority>,
    pub created_at: OffsetDateTime,
}
pub(crate) fn scope(session: &SessionId, id: &ControlId) -> Result<(), StoreError> {
    crate::management::validate_scope(session, id).map_err(|_| invalid())
}
pub(crate) fn identifier(value: &str) -> Result<(), StoreError> {
    crate::attempt::validate_identifier(value).map_err(|_| invalid())
}
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<String, StoreError> {
    let encoded = serde_json::to_string(value).map_err(|_| invalid())?;
    if encoded.len() > MAX_RECORD {
        return Err(invalid());
    }
    Ok(encoded)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, StoreError> {
    if value.len() > MAX_RECORD {
        return Err(corrupt());
    }
    serde_json::from_str(value).map_err(|_| corrupt())
}
impl Record {
    pub fn validate(&self, session: &SessionId, id: &QueueId, seq: u64) -> Result<(), StoreError> {
        if self.summary.session_id != *session
            || self.summary.queue_id != *id
            || self.summary.sequence != seq
            || seq == 0
            || self.input.request.session_id != *session
            || self.summary.prepared != self.prepared.is_some()
            || self.prepared.is_some() != self.prepare_operation.is_some()
            || (self.summary.state == QueueState::Admitted) != self.summary.admission.is_some()
            || (self.summary.state == QueueState::Claimed) != self.claim.is_some()
        {
            return Err(corrupt());
        }
        identifier(&id.0)?;
        if let Some(claim) = &self.claim {
            let a = &claim.authority;
            if a.session_id != *session
                || a.queue_id != *id
                || a.generation == 0
                || a.generation != self.summary.claim_generation
                || a.token.len() != 36
            {
                return Err(corrupt());
            }
        }
        if let Some(prepared) = &self.prepared {
            self.verify_prepared(prepared)?;
        }
        Ok(())
    }
    pub fn verify_prepared(&self, prepared: &BoundKeyedAdmitRequest) -> Result<(), StoreError> {
        prepared.binding.validate_keyed(&prepared.keyed)?;
        let original = &self.input.request;
        let actual = &prepared.binding.request;
        if prepared.keyed.options != self.input.admission
            || prepared.binding.input_envelope.as_ref() != Some(&self.input.input)
            || actual.session_id != original.session_id
            || actual.workspace_id != original.workspace_id
            || actual.directory != original.directory
            || actual.text != original.text
            || actual.selection_policy.is_explicit() != original.selection_policy.is_explicit()
            || (original.selection_policy.is_explicit()
                && (actual.provider_id != original.provider_id
                    || actual.model_id != original.model_id))
        {
            return Err(StoreError::AdmissionConflict);
        }
        Ok(())
    }
    pub fn retained(&self) -> RetainedQueue {
        RetainedQueue {
            summary: self.summary.clone(),
            input: self.input.clone(),
            prepared: self.prepared.clone(),
        }
    }
}
pub(crate) fn verify_audit(audit: &Audit, saved: &QueueReceipt) -> Result<(), StoreError> {
    if audit.receipt != *saved
        || audit.request.fingerprint()? != saved.semantic_digest
        || audit.request.session_id != saved.session_id
        || audit.request.id != saved.operation_id
        || saved.summary.session_id != saved.session_id
    {
        return Err(corrupt());
    }
    Ok(())
}
pub(crate) fn page(
    request: &QueuePageRequest,
    revision: SessionRevision,
    mut entries: Vec<QueueSummary>,
) -> Result<QueuePage, StoreError> {
    scope(&request.session_id, &ControlId("page".into()))?;
    if !(1..=64).contains(&request.limit)
        || (request.after_sequence.is_some() && request.expected_revision.is_none())
    {
        return Err(invalid());
    }
    if request
        .expected_revision
        .is_some_and(|expected| expected != revision)
    {
        return Err(StoreError::AdmissionRevisionConflict);
    }
    let more = entries.len() > usize::from(request.limit);
    entries.truncate(usize::from(request.limit));
    let next_after = if more {
        entries.last().map(|entry| entry.sequence)
    } else {
        None
    };
    let result = QueuePage {
        revision,
        entries,
        next_after,
    };
    if serde_json::to_vec(&result).map_err(|_| corrupt())?.len() > 256 << 10 {
        return Err(corrupt());
    }
    Ok(result)
}

pub(crate) fn validate_summary(
    summary: &QueueSummary,
    session: &SessionId,
    id: &QueueId,
    seq: u64,
) -> Result<(), StoreError> {
    if summary.session_id != *session
        || summary.queue_id != *id
        || summary.sequence != seq
        || seq == 0
        || (summary.state == QueueState::Admitted) != summary.admission.is_some()
        || summary
            .admission
            .as_ref()
            .is_some_and(|r| r.session_id != *session)
        || serde_json::to_vec(summary).map_err(|_| corrupt())?.len() > 16384
    {
        return Err(corrupt());
    }
    identifier(&id.0)
}

pub(crate) fn verify_sources(
    record: &Record,
    enqueue: &Audit,
    prepare: Option<&Audit>,
    claim: Option<&Audit>,
) -> Result<(), StoreError> {
    if enqueue.request.id != record.enqueue_operation
        || enqueue.request.session_id != record.summary.session_id
    {
        return Err(corrupt());
    }
    let QueueAction::Enqueue { queue_id, .. } = &enqueue.request.action else {
        return Err(corrupt());
    };
    if *queue_id != record.summary.queue_id
        || enqueue.receipt.summary.sequence != record.summary.sequence
    {
        return Err(corrupt());
    }
    let mut reconstructed = enqueue.request.clone();
    reconstructed.action = QueueAction::Enqueue {
        queue_id: record.summary.queue_id.clone(),
        input: Box::new(record.input.clone()),
    };
    if reconstructed.fingerprint()? != enqueue.receipt.semantic_digest {
        return Err(corrupt());
    }
    match (&record.claim, claim) {
        (None, None) => {}
        (Some(live), Some(audit)) => {
            let QueueAction::Claim { owner, lease } = &audit.request.action else {
                return Err(corrupt());
            };
            if audit.request.id != live.operation
                || audit.receipt.summary.queue_id != record.summary.queue_id
                || audit.receipt.summary.sequence != record.summary.sequence
                || audit.receipt.summary.claim_generation != live.authority.generation
                || audit.authority.as_ref() != Some(&live.authority)
                || live.owner != *owner
                || Some(live.expires)
                    != audit
                        .created_at
                        .checked_add(time::Duration::try_from(*lease).map_err(|_| corrupt())?)
            {
                return Err(corrupt());
            }
        }
        _ => return Err(corrupt()),
    }
    match (&record.prepare_operation, &record.prepared, prepare) {
        (None, None, None) => {}
        (Some(id), Some(prepared), Some(audit)) => {
            if audit.request.id != *id
                || audit.request.session_id != record.summary.session_id
                || audit.receipt.summary.queue_id != record.summary.queue_id
            {
                return Err(corrupt());
            }
            let QueueAction::Prepare { authority, .. } = &audit.request.action else {
                return Err(corrupt());
            };
            let mut reconstructed = audit.request.clone();
            reconstructed.action = QueueAction::Prepare {
                authority: authority.clone(),
                prepared: Box::new(prepared.clone()),
            };
            if reconstructed.fingerprint()? != audit.receipt.semantic_digest {
                return Err(corrupt());
            }
        }
        _ => return Err(corrupt()),
    }
    Ok(())
}

pub(crate) fn verify_current(
    summary: &QueueSummary,
    latest: &QueueReceipt,
    actual_admission: Option<&crabber_core::AdmissionReceipt>,
) -> Result<(), StoreError> {
    if latest.session_id != summary.session_id
        || latest.summary != *summary
        || summary.admission.as_ref() != actual_admission
    {
        return Err(corrupt());
    }
    Ok(())
}

pub(crate) fn validate_session(
    session: &crabber_core::Session,
    metadata: &crate::management::Metadata,
) -> Result<(), StoreError> {
    scope(&session.id, &ControlId("session".into()))?;
    if session.workspace_id.len() > 512
        || session.directory.len() > 16384
        || session.workspace_id.contains('\0')
        || session.directory.contains('\0')
        || serde_json::to_vec(session).map_err(|_| corrupt())?.len() > 32768
    {
        return Err(corrupt());
    }
    crate::management::validate_defaults(&metadata.defaults).map_err(|_| corrupt())
}
