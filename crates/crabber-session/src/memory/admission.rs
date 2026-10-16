//! Transaction-only admission helpers shared by bound and legacy entry points.
use super::{State, initial_epoch, insert_message, project_messages};
use crate::{AdmitOutcome, AdmitRequest, KeyedAdmitOutcome, KeyedAdmitRequest, StoreError};
use crabber_core::{
    AdmissionReceipt, EpochId, MessageId, Run, RunFence, RunId, RunStatus, Session, Usage,
};
use time::OffsetDateTime;

pub(super) fn admit_transaction(
    state: &mut State,
    request: &AdmitRequest,
    now: OffsetDateTime,
    allow_create: bool,
) -> Result<AdmitOutcome, StoreError> {
    admit_transaction_guarded(
        state,
        request,
        now,
        allow_create,
        crate::queue::AdmissionGuard::Ordinary,
    )
}
pub(super) fn admit_transaction_guarded(
    state: &mut State,
    request: &AdmitRequest,
    now: OffsetDateTime,
    allow_create: bool,
    guard: crate::queue::AdmissionGuard<'_>,
) -> Result<AdmitOutcome, StoreError> {
    guard.verify(request)?;
    if request.input_envelope.is_some() || !request.additional_messages.is_empty() {
        request.validate_messages()?;
    }
    let lease = time::Duration::try_from(request.lease)
        .map_err(|_| StoreError::Validation("lease is too large".into()))?;
    if lease <= time::Duration::ZERO {
        return Err(StoreError::Validation("lease must be positive".into()));
    }
    let session = admission_session(state, request, now, allow_create, guard)?;
    let session_id = session.id.clone();
    admission_guards(state, &session_id, guard)?;
    request.validate_messages()?;
    if request.user_message.session_id != session_id {
        return Err(StoreError::Validation(
            "user message has wrong session".into(),
        ));
    }
    let prior_history = if state.sessions.contains_key(&session_id) {
        project_messages(state, &session_id, None)?
    } else {
        Vec::new()
    };
    let previous_epoch = state
        .run_order
        .iter()
        .rev()
        .find_map(|id| {
            state
                .runs
                .get(id)
                .filter(|run| run.session_id == session_id)
        })
        .map(|run| state.epochs.get(&run.epoch_id).ok_or(StoreError::NotFound))
        .transpose()?
        .cloned();
    let run_id = RunId::new();
    let epoch_id = EpochId::new();
    let token = uuid::Uuid::new_v4().to_string();
    let run = Run {
        id: run_id.clone(),
        session_id: session_id.clone(),
        status: RunStatus::Running,
        owner: request.owner.clone(),
        claim_token: token.clone(),
        lease_until: now + lease,
        epoch_id: epoch_id.clone(),
        config_hash: request.config_hash.clone(),
        plan_fingerprint: request.plan_fingerprint.clone(),
        checkpoint: None,
        error: None,
        usage: Usage::default(),
        created_at: now,
        updated_at: now,
    };
    let mut user_message = request.user_message.clone();
    user_message.run_id = Some(run_id.clone());
    crate::ensure_storable(&session)?;
    crate::ensure_storable(&run)?;
    insert_message(state, &run, user_message)?;
    for additional in &request.additional_messages {
        let mut message = additional.clone();
        message.run_id = Some(run_id.clone());
        insert_message(state, &run, message)?;
    }
    state.sessions.insert(session_id.clone(), session.clone());
    super::native_parent::record_run(state, None, &run)
        .map_err(|_| StoreError::Validation("native run fence evidence unavailable".into()))?;
    state.runs.insert(run_id.clone(), run.clone());
    state.run_order.push(run_id.clone());
    let epoch = initial_epoch(epoch_id.clone(), session_id, run_id.clone(), previous_epoch);
    crate::ensure_storable(&epoch)?;
    state.epochs.insert(epoch_id.clone(), epoch);
    Ok(AdmitOutcome {
        session,
        run,
        fence: RunFence {
            run_id,
            claim_token: token,
        },
        assistant_placeholder: MessageId::new(),
        epoch: epoch_id,
        prior_history,
    })
}

fn admission_guards(
    state: &State,
    session_id: &crabber_core::SessionId,
    guard: crate::queue::AdmissionGuard<'_>,
) -> Result<(), StoreError> {
    if state
        .management
        .get(session_id)
        .is_some_and(|metadata| metadata.archived)
    {
        return Err(StoreError::Conflict);
    }
    if guard.is_ordinary()
        && state.queue.values().any(|record| {
            record.summary.session_id == *session_id
                && record.summary.state == crate::QueueState::Claimed
                && record.prepared.is_some()
        })
    {
        return Err(StoreError::Busy);
    }
    if super::has_pending_continuation(state, session_id)? {
        return Err(StoreError::Busy);
    }

    if state
        .runs
        .values()
        .any(|run| run.session_id == *session_id && !run.status.is_terminal())
    {
        return Err(StoreError::Busy);
    }
    Ok(())
}

fn admission_session(
    state: &State,
    request: &AdmitRequest,
    now: OffsetDateTime,
    allow_create: bool,
    guard: crate::queue::AdmissionGuard<'_>,
) -> Result<Session, StoreError> {
    let session_id = request
        .session_id
        .clone()
        .unwrap_or_else(|| request.user_message.session_id.clone());
    let session = if let Some(existing) = state.sessions.get(&session_id) {
        existing.ensure_identity(&request.workspace_id, &request.directory)?;
        existing.clone()
    } else if request.session_id.is_some() && !allow_create {
        return Err(StoreError::NotFound);
    } else {
        Session {
            id: session_id.clone(),
            workspace_id: request.workspace_id.clone(),
            directory: request.directory.clone(),
            title: request.title.clone(),
            created_at: now,
            updated_at: now,
        }
    };
    if guard.is_ordinary()
        && let Some(expected) = request.expected_revision
    {
        let current = state
            .public_revisions
            .get(&session_id)
            .copied()
            .unwrap_or(crabber_core::SessionRevision::INITIAL);
        if current != expected {
            return Err(StoreError::AdmissionRevisionConflict);
        }
    }
    Ok(session)
}

pub(super) fn admit_keyed_transaction(
    state: &mut State,
    keyed: &KeyedAdmitRequest,
    now: OffsetDateTime,
) -> Result<KeyedAdmitOutcome, StoreError> {
    admit_keyed_guarded(state, keyed, now, crate::queue::AdmissionGuard::Ordinary)
}
pub(super) fn admit_keyed_guarded(
    state: &mut State,
    keyed: &KeyedAdmitRequest,
    now: OffsetDateTime,
    guard: crate::queue::AdmissionGuard<'_>,
) -> Result<KeyedAdmitOutcome, StoreError> {
    let digest = keyed.validated_digest()?;
    let request = &keyed.request;
    let session_id = request
        .session_id
        .as_ref()
        .ok_or(StoreError::AdmissionConflict)?;
    if let Some(session) = state.sessions.get(session_id) {
        session.ensure_identity(&request.workspace_id, &request.directory)?;
    }
    let key = (session_id.clone(), keyed.options.key.clone());
    if let Some(receipt) = state.receipts.get(&key) {
        if receipt.fingerprint != keyed.options.fingerprint || receipt.semantic_digest != digest {
            return Err(StoreError::AdmissionConflict);
        }
        return Ok(KeyedAdmitOutcome::Replayed(receipt.clone()));
    }
    if guard.is_ordinary()
        && state.queue.values().any(|record| {
            record.summary.session_id == *session_id
                && record.input.admission.key == keyed.options.key
        })
    {
        return Err(StoreError::AdmissionConflict);
    }
    let admitted = admit_transaction_guarded(state, request, now, true, guard)?;
    let receipt = AdmissionReceipt {
        session_id: session_id.clone(),
        run_id: admitted.run.id.clone(),
        user_message_id: request.user_message.id.clone(),
        fingerprint: keyed.options.fingerprint.clone(),
        semantic_digest_version: if request.input_envelope.is_some() {
            2
        } else {
            1
        },
        semantic_digest: digest,
    };
    crate::ensure_storable(&receipt)?;
    if let Some(capsule) = keyed.execution.clone() {
        let execution = crate::AdmissionExecutionRecord {
            receipt: receipt.clone(),
            key: keyed.options.key.clone(),
            capsule,
            state: crate::AdmissionExecutionState::Unstarted,
        };
        if execution.capsule.version == 2 {
            execution.encode()?;
        } else {
            crate::ensure_storable(&execution)?;
        }
        state
            .admission_executions
            .insert(admitted.run.id.clone(), execution);
    }
    state.receipts.insert(key, receipt.clone());
    Ok(KeyedAdmitOutcome::Started {
        receipt,
        admitted: Box::new(admitted),
    })
}
