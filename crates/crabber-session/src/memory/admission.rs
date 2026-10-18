//! Transaction-only admission helpers shared by bound and legacy entry points.
use super::{State, initial_epoch, insert_message, project_messages};
use crate::native_enrollment::caller::{
    Caller, Fresh,
    initial::{CapturedAdmission, CapturedKeyedAdmission, Initial, Input},
};
use crate::{AdmitOutcome, AdmitRequest, KeyedAdmitOutcome, KeyedAdmitRequest, StoreError};
use crabber_core::{
    AdmissionReceipt, EpochId, MessageId, Run, RunFence, RunId, RunStatus, Session, Usage,
};
use time::OffsetDateTime;

pub(super) fn selected(
    state: &State,
    request: &AdmitRequest,
    keyed: Option<&KeyedAdmitRequest>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), StoreError> {
    let session = request
        .session_id
        .as_ref()
        .unwrap_or(&request.user_message.session_id);
    proof
        .measure(&state.sessions.get(session))
        .map_err(initial_invalid)?;
    if let Some(keyed) = keyed
        && let Some(receipt) = state
            .receipts
            .get(&(session.clone(), keyed.options.key.clone()))
    {
        proof.measure(receipt).map_err(initial_invalid)?;
        return Ok(());
    }
    state
        .session_owners
        .context_with_proof(session, &request.workspace_id, &request.directory, proof)
        .map_err(|_| StoreError::Conflict)?;
    proof
        .measure(&state.management.get(session))
        .map_err(initial_invalid)?;
    for record in state
        .queue
        .values()
        .filter(|r| r.summary.session_id == *session)
    {
        // This guard borrows only routing, state and preparation presence. Queue
        // selection separately charges the full selected head before cloning;
        // unrelated backlog inputs are not materialized by this admission read.
        proof
            .measure(&(
                &record.summary.session_id,
                record.summary.state,
                record.prepared.is_some(),
            ))
            .map_err(initial_invalid)?;
    }
    for run in state.runs.values().filter(|r| r.session_id == *session) {
        proof.measure(run).map_err(initial_invalid)?;
    }
    if let Some(run) = state
        .run_order
        .iter()
        .rev()
        .filter_map(|id| state.runs.get(id))
        .find(|r| r.session_id == *session)
    {
        proof
            .measure(&state.epochs.get(&run.epoch_id))
            .map_err(initial_invalid)?;
    }
    for message in state.messages.iter().filter(|m| m.session_id == *session) {
        proof.measure(message).map_err(initial_invalid)?;
    }
    pending_with_proof(state, session, proof)?;
    Ok(())
}
fn pending_with_proof(
    state: &State,
    session: &crabber_core::SessionId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<bool, StoreError> {
    super::approval::verify_session_with_proof(state, session, proof).map_err(|error| {
        error
            .map_domain(|_| StoreError::Validation("approval integrity failure".into()))
            .into_domain_or(initial_invalid("approval proof budget"))
    })?;
    let pending = super::tool_continuation::pending_in_session_with_proof(state, session, proof)
        .map_err(|_| StoreError::Validation("invalid tool continuation evidence".into()))?;
    proof
        .measure(&state.approval_barriers.get(session))
        .map_err(initial_invalid)?;
    Ok(state.approval_barriers.contains_key(session) || pending)
}
pub(super) fn admit_transaction_captured(
    state: &mut State,
    request: &AdmitRequest,
    now: OffsetDateTime,
    allow_create: bool,
    guard: crate::queue::AdmissionGuard<'_>,
    caller: crate::native_enrollment::caller::initial::Dispatch<'_>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<CapturedAdmission, StoreError> {
    caller
        .input_request(request, proof)
        .map_err(initial_invalid)?;
    selected(state, request, None, proof)?;
    guard.verify(request)?;
    if request.input_envelope.is_some() || !request.additional_messages.is_empty() {
        request.validate_messages()?;
    }
    let (owner, requested_lease) = guard.owner_and_lease(request);
    let lease = time::Duration::try_from(requested_lease)
        .map_err(|_| StoreError::Validation("lease is too large".into()))?;
    if lease <= time::Duration::ZERO {
        return Err(StoreError::Validation("lease must be positive".into()));
    }
    let session = admission_session(state, request, now, allow_create, guard)?;
    let session_id = session.id.clone();
    admission_guards(state, &session_id, guard, proof)?;
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
        owner: owner.to_owned(),
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
    let new_session = !state.sessions.contains_key(&session_id);
    let first = record_ownership(state, &session, &run, new_session, guard, caller, proof)?;
    if new_session {
        super::native_parent::source_protocol::session_birth(state, &session_id)
            .map_err(|_| StoreError::Conflict)?;
    }
    state.sessions.insert(session_id.clone(), session.clone());
    state.runs.insert(run_id.clone(), run.clone());
    state.run_order.push(run_id.clone());
    let epoch = initial_epoch(epoch_id.clone(), session_id, run_id.clone(), previous_epoch);
    crate::ensure_storable(&epoch)?;
    state.epochs.insert(epoch_id.clone(), epoch);
    Ok(CapturedAdmission {
        first,
        admitted: AdmitOutcome {
            session,
            run,
            fence: RunFence {
                run_id,
                claim_token: token,
            },
            assistant_placeholder: MessageId::new(),
            epoch: epoch_id,
            prior_history,
        },
    })
}

fn admission_guards(
    state: &State,
    session_id: &crabber_core::SessionId,
    guard: crate::queue::AdmissionGuard<'_>,
    proof: &mut crate::public_session::v2::proof::Proof,
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
    if pending_with_proof(state, session_id, proof)? {
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
    super::deletion::ensure_live(state, &session_id)?;
    state
        .session_owners
        .context(&session_id, &request.workspace_id, &request.directory)
        .map_err(|error| {
            if error == crate::session_owner::SessionOwnerError::IdentityMismatch {
                StoreError::SessionIdentityMismatch
            } else {
                StoreError::Validation("session ownership proof unavailable".into())
            }
        })?;
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
        && let Some(expected) = guard.expected_revision(request.expected_revision)
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

#[cfg(test)]
pub(super) fn admit_keyed_with_proof(
    state: &mut State,
    keyed: &KeyedAdmitRequest,
    now: OffsetDateTime,
    guard: crate::queue::AdmissionGuard<'_>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<KeyedAdmitOutcome, StoreError> {
    let captured = admit_keyed_captured(
        state,
        keyed,
        now,
        guard,
        crate::native_enrollment::caller::initial::Dispatch::raw(Caller::Raw),
        proof,
    )?;
    match captured {
        CapturedKeyedAdmission::Replayed(receipt) => Ok(KeyedAdmitOutcome::Replayed(receipt)),
        CapturedKeyedAdmission::Started { receipt, admitted } => {
            let admitted = finish_initial(
                state,
                Input::Keyed {
                    request: keyed,
                    receipt: &receipt,
                },
                *admitted,
                proof,
                #[cfg(test)]
                &crate::native_enrollment::caller::Probe::default(),
            )?;
            Ok(KeyedAdmitOutcome::Started {
                receipt,
                admitted: Box::new(admitted),
            })
        }
    }
}
pub(super) fn admit_keyed_captured(
    state: &mut State,
    keyed: &KeyedAdmitRequest,
    now: OffsetDateTime,
    guard: crate::queue::AdmissionGuard<'_>,
    caller: crate::native_enrollment::caller::initial::Dispatch<'_>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<CapturedKeyedAdmission, StoreError> {
    let request = &keyed.request;
    let session_id = request
        .session_id
        .as_ref()
        .ok_or(StoreError::AdmissionConflict)?;
    super::deletion::ensure_live(state, session_id)?;
    if let Some(session) = state.sessions.get(session_id) {
        session.ensure_identity(&request.workspace_id, &request.directory)?;
    }
    let key = (session_id.clone(), keyed.options.key.clone());
    if let Some(receipt) = state.receipts.get(&key) {
        // Historical exact replay keeps its existing bounded-domain validation;
        // this fresh-only witness does not claim to bound replay materialization.
        let digest = keyed.validated_digest()?;
        if receipt.fingerprint != keyed.options.fingerprint || receipt.semantic_digest != digest {
            return Err(StoreError::AdmissionConflict);
        }
        return Ok(CapturedKeyedAdmission::Replayed(receipt.clone()));
    }
    caller.input_keyed(keyed, proof).map_err(initial_invalid)?;
    let digest = keyed.validated_digest()?;
    if guard.is_ordinary()
        && state.queue.values().any(|record| {
            record.summary.session_id == *session_id
                && record.input.admission.key == keyed.options.key
        })
    {
        return Err(StoreError::AdmissionConflict);
    }
    let captured = admit_transaction_captured(state, request, now, true, guard, caller, proof)?;
    let admitted = &captured.admitted;
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
    if let Some(capsule) = &keyed.execution {
        crate::admission_journal::measure_record(
            &receipt,
            &keyed.options.key,
            capsule,
            crate::AdmissionExecutionState::Unstarted,
            proof,
        )
        .map_err(|error| error.into_domain_or(initial_invalid("execution record budget")))?;
        #[cfg(test)]
        state
            .pending_initial_carrier_observations
            .lock()
            .unwrap()
            .push((receipt.session_id.clone(), proof.used()));
        let execution = crate::AdmissionExecutionRecord {
            receipt: receipt.clone(),
            key: keyed.options.key.clone(),
            capsule: capsule.clone(),
            state: crate::AdmissionExecutionState::Unstarted,
        };
        if execution.capsule.version == 2 {
            execution.encode()?;
        } else {
            crate::ensure_storable(&execution)?;
        }
        super::admission_journal::init_with_proof(state, &execution, &admitted.run, proof)?;
        state
            .admission_executions
            .insert(admitted.run.id.clone(), execution);
    }
    state.receipts.insert(key, receipt.clone());
    proof
        .charge(std::mem::size_of::<
            crate::native_enrollment::caller::initial::CapturedAdmission,
        >())
        .map_err(initial_invalid)?;
    Ok(CapturedKeyedAdmission::Started {
        receipt,
        admitted: Box::new(captured),
    })
}

fn record_ownership(
    state: &mut State,
    session: &Session,
    run: &Run,
    new_session: bool,
    guard: crate::queue::AdmissionGuard<'_>,
    caller: crate::native_enrollment::caller::initial::Dispatch<'_>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<crate::native_enrollment::caller::initial::FirstPublication, StoreError> {
    proof.measure(&(session, run)).map_err(initial_invalid)?;
    let invalid = |_| StoreError::Validation("preparation admission evidence unavailable".into());
    let witness = guard
        .preparation()
        .map(|preparation| {
            let revision = state
                .public_revisions
                .get(&session.id)
                .copied()
                .unwrap_or(crabber_core::SessionRevision::INITIAL);
            crate::session_owner::preparation::admission::Witness::from_preparation(
                preparation,
                session,
                run,
                revision,
            )
        })
        .transpose()
        .map_err(invalid)?
        .flatten();
    if let Some(witness) = witness {
        proof.measure(&witness).map_err(initial_invalid)?;
        let origin = if new_session {
            Some(crate::deletion::origin::OwnershipOrigin::prepared(&witness).map_err(invalid)?)
        } else {
            None
        };
        let first = super::native_parent::record_owned_admission_with_birth(
            state,
            run,
            witness,
            caller.birth(),
            proof,
        )
        .map_err(|_| StoreError::Conflict)?;
        if let Some(origin) = origin {
            state.ownership_protocol_sessions.insert(session.id.clone());
            state.ownership_origins.insert(session.id.clone(), origin);
        }
        Ok(first)
    } else {
        super::native_parent::record_initial_run_with_birth(state, run, caller.birth(), proof)
            .map_err(|_| StoreError::Conflict)
    }
}
pub(super) fn finish_initial(
    state: &State,
    input: Input<'_>,
    captured: CapturedAdmission,
    proof: &mut crate::public_session::v2::proof::Proof,
    #[cfg(test)] probe: &crate::native_enrollment::caller::Probe,
) -> Result<AdmitOutcome, StoreError> {
    super::native_enrollment::record_raw_native(
        state,
        Caller::Raw,
        &Fresh::Initial(Initial {
            input,
            result: &captured.admitted,
            first: &captured.first,
        }),
        proof,
        #[cfg(test)]
        probe,
    )
    .map_err(initial_invalid)?;
    Ok(captured.admitted)
}
fn initial_invalid(_: impl std::fmt::Debug) -> StoreError {
    StoreError::Validation("native initial caller evidence unavailable".into())
}

impl super::MemoryStore {
    pub(super) fn admit_raw(&self, request: &AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        let result = self.transact_selected(
            &mut proof,
            |state, proof| {
                selected(state, request, None, proof)?;
                Ok(None)
            },
            |state, proof| {
                let captured = admit_transaction_captured(
                    state,
                    request,
                    self.clock.now(),
                    false,
                    crate::queue::AdmissionGuard::Ordinary,
                    crate::native_enrollment::caller::initial::Dispatch::raw(Caller::Raw),
                    proof,
                )?;
                finish_initial(
                    state,
                    Input::Ordinary(request),
                    captured,
                    proof,
                    #[cfg(test)]
                    &self.native_raw_probe,
                )
            },
        )?;
        #[cfg(test)]
        crate::binding_fault::trip(&self.binding_fault, 2)?;
        Ok(result)
    }
    pub(super) fn admit_keyed_raw(
        &self,
        request: &KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        let result = self.transact_selected(
            &mut proof,
            |state, proof| {
                selected(state, &request.request, Some(request), proof)?;
                Ok(None)
            },
            |state, proof| match admit_keyed_captured(
                state,
                request,
                self.clock.now(),
                crate::queue::AdmissionGuard::Ordinary,
                crate::native_enrollment::caller::initial::Dispatch::raw(Caller::Raw),
                proof,
            )? {
                CapturedKeyedAdmission::Replayed(receipt) => {
                    Ok(KeyedAdmitOutcome::Replayed(receipt))
                }
                CapturedKeyedAdmission::Started { receipt, admitted } => {
                    let admitted = finish_initial(
                        state,
                        Input::Keyed {
                            request,
                            receipt: &receipt,
                        },
                        *admitted,
                        proof,
                        #[cfg(test)]
                        &self.native_raw_probe,
                    )?;
                    Ok(KeyedAdmitOutcome::Started {
                        receipt,
                        admitted: Box::new(admitted),
                    })
                }
            },
        )?;
        #[cfg(test)]
        if matches!(&result, KeyedAdmitOutcome::Started { .. }) {
            crate::binding_fault::trip(&self.binding_fault, 2)?;
        }
        Ok(result)
    }
}
